/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class WindowingInvokable<IN> extends StreamInvokable<IN, Tuple2<IN, String[]>> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -8038984294071650730L;

	private static final Logger LOG = LoggerFactory.getLogger(WindowingInvokable.class);

	private LinkedList<TriggerPolicy<IN>> triggerPolicies;
	private LinkedList<EvictionPolicy<IN>> evictionPolicies;
	private LinkedList<ActiveTriggerPolicy<IN>> activeTriggerPolicies;
	private LinkedList<ActiveEvictionPolicy<IN>> activeEvictionPolicies;
	private LinkedList<Thread> activePolicyTreads = new LinkedList<Thread>();
	private LinkedList<IN> buffer = new LinkedList<IN>();
	private LinkedList<TriggerPolicy<IN>> currentTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
	private ReduceFunction<IN> reducer;

	public WindowingInvokable(ReduceFunction<IN> userFunction,
			LinkedList<TriggerPolicy<IN>> triggerPolicies,
			LinkedList<EvictionPolicy<IN>> evictionPolicies) {
		super(userFunction);

		this.reducer = userFunction;
		this.triggerPolicies = triggerPolicies;
		this.evictionPolicies = evictionPolicies;

		activeTriggerPolicies = new LinkedList<ActiveTriggerPolicy<IN>>();
		for (TriggerPolicy<IN> tp : triggerPolicies) {
			if (tp instanceof ActiveTriggerPolicy) {
				activeTriggerPolicies.add((ActiveTriggerPolicy<IN>) tp);
			}
		}

		activeEvictionPolicies = new LinkedList<ActiveEvictionPolicy<IN>>();
		for (EvictionPolicy<IN> ep : evictionPolicies) {
			if (ep instanceof ActiveEvictionPolicy) {
				activeEvictionPolicies.add((ActiveEvictionPolicy<IN>) ep);
			}
		}
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);
		for (ActiveTriggerPolicy<IN> tp : activeTriggerPolicies) {
			Runnable target=tp.createActiveTriggerRunnable(new WindowingCallback(tp));
			if (target!=null){
				Thread thread = new Thread(target);
				activePolicyTreads.add(thread);
				thread.start();
			}
		}
	};

	private class WindowingCallback implements ActiveTriggerCallback<IN> {
		private ActiveTriggerPolicy<IN> policy;

		public WindowingCallback(ActiveTriggerPolicy<IN> policy) {
			this.policy = policy;
		}

		@Override
		public void sendFakeElement(IN datapoint) {
			processFakeElement(datapoint, this.policy);
		}

	}

	@Override
	protected void immutableInvoke() throws Exception {
		// Prevent empty data streams
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		// Continuously run
		while (reuse != null) {
			processRealElement(reuse.getObject());

			// Recreate the reuse-StremRecord object and load next StreamRecord
			resetReuse();
			reuse = recordIterator.next(reuse);
		}

		// Stop all remaining threads from policies
		for (Thread t : activePolicyTreads) {
			t.interrupt();
		}

		// finally trigger the buffer.
		if (!buffer.isEmpty()) {
			currentTriggerPolicies.clear();
			for (TriggerPolicy<IN> policy : triggerPolicies) {
				currentTriggerPolicies.add(policy);
			}
			callUserFunctionAndLogException();
		}

	}

	@Override
	protected void mutableInvoke() throws Exception {
		if (LOG.isInfoEnabled()) {
			LOG.info("There is currently no mutable implementation of this operator. Immutable version is used.");
		}
		immutableInvoke();
	}

	/**
	 * This method processed an arrived fake element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link WindowingInvokable#processRealElement(Object)}
	 * 
	 * @param input
	 *            a fake input element
	 * @param currentPolicy
	 *            the policy which produced this fake element
	 */
	private synchronized void processFakeElement(IN input, TriggerPolicy<IN> currentPolicy) {
		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;
		for (ActiveEvictionPolicy<IN> evictionPolicy : activeEvictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEvictionWithFakeElement(input, buffer.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		for (int i = 0; i < currentMaxEviction; i++) {
			try {
				buffer.removeFirst();
			} catch (NoSuchElementException e) {
				// In case no more elements are in the buffer:
				// Prevent failure and stop deleting.
				break;
			}
		}

		// Set the current trigger
		currentTriggerPolicies.add(currentPolicy);

		// emit
		callUserFunctionAndLogException();

		// clear the flag collection
		currentTriggerPolicies.clear();
	}

	/**
	 * This method processed an arrived real element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link WindowingInvokable#processFakeElement(Object)}
	 * 
	 * @param input
	 *            a real input element
	 */
	private synchronized void processRealElement(IN input) {
		// Run the precalls to detect missed windows
		for (ActiveTriggerPolicy<IN> trigger : activeTriggerPolicies) {
			// Remark: In case multiple active triggers are present the ordering
			// of the different fake elements returned by this triggers becomes
			// a problem. This might lead to unexpected results...
			// Should we limit the number of active triggers to 0 or 1?
			IN[] result = trigger.preNotifyTrigger(input);
			for (IN in : result) {
				processFakeElement(in, trigger);
			}
		}

		// Remember if a trigger occurred
		boolean isTriggered = false;

		// Process the triggers
		for (TriggerPolicy<IN> triggerPolicy : triggerPolicies) {
			if (triggerPolicy.notifyTrigger(input)) {
				currentTriggerPolicies.add(triggerPolicy);
			}
		}

		// call user function
		if (!currentTriggerPolicies.isEmpty()) {
			// emit
			callUserFunctionAndLogException();

			// clear the flag collection
			currentTriggerPolicies.clear();

			// remember trigger
			isTriggered = true;
		}

		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;

		for (EvictionPolicy<IN> evictionPolicy : evictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEviction(input, isTriggered, buffer.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		for (int i = 0; i < currentMaxEviction; i++) {
			try {
				buffer.removeFirst();
			} catch (NoSuchElementException e) {
				// In case no more elements are in the buffer:
				// Prevent failure and stop deleting.
				break;
			}
		}

		// Add the current element to the buffer
		buffer.add(input);

	}

	@Override
	protected void callUserFunction() throws Exception {
		Iterator<IN> reducedIterator = buffer.iterator();
		IN reduced = null;

		while (reducedIterator.hasNext() && reduced == null) {
			reduced = reducedIterator.next();
		}

		while (reducedIterator.hasNext()) {
			IN next = reducedIterator.next();
			if (next != null) {
				reduced = reducer.reduce(reduced, next);
			}
		}
		if (reduced != null) {
			String[] tmp = new String[currentTriggerPolicies.size()];
			for (int i = 0; i < tmp.length; i++) {
				tmp[i] = currentTriggerPolicies.get(i).toString();
			}
			collector.collect(new Tuple2<IN, String[]>(reduced, tmp));
		}
	}

}
