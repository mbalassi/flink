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

package org.apache.flink.streaming.examples.windowing;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.util.keys.TupleKeySelector;
import org.apache.flink.util.Collector;

/**
 * This example shows the functionality of time based windows. It utilizes the
 * {@link ActiveTriggerPolicy} implementation in the {@link TimeTriggerPolicy}.
 * The example uses grouping in combination with an centralized active trigger
 * policy.
 */
public class GroupedTimeWindowingExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		// Prevent output from being blocked
		env.setBufferTimeout(100);

		// Make an empty list for distributed triggers:
		LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>> distributedTriggerPolicies = new LinkedList<CloneableTriggerPolicy<Tuple2<Integer, String>>>();

		// Make a count based eviction:
		LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>> distributedEvictionPolicies = new LinkedList<CloneableEvictionPolicy<Tuple2<Integer, String>>>();
		distributedEvictionPolicies.add(new CountEvictionPolicy<Tuple2<Integer, String>>(100));

		// Make a time based central triggers
		LinkedList<TriggerPolicy<Tuple2<Integer, String>>> centralTriggerPolicies = new LinkedList<TriggerPolicy<Tuple2<Integer, String>>>();
		centralTriggerPolicies.add(new TimeTriggerPolicy<Tuple2<Integer, String>>(1000L,
				new DefaultTimeStamp<Tuple2<Integer, String>>(),
				new Extractor<Long, Tuple2<Integer, String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> extract(Long in) {
						return new Tuple2<Integer, String>(in.intValue(), "fakeElement");
					}

				}));

		// This reduce function does a String concat.
		ReduceFunction<Tuple2<Integer, String>> reduceFunction = new ReduceFunction<Tuple2<Integer, String>>() {

			/**
			 * default version ID
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1,
					Tuple2<Integer, String> value2) throws Exception {
				return new Tuple2<Integer, String>(value1.f0 + value2.f0, value1.f1 + "|"
						+ value2.f1);
			}

		};

		DataStream<Tuple2<Tuple2<Integer, String>, String[]>> stream = env.addSource(
				new CountingSourceWithSleep()).groupedWindow(
				distributedTriggerPolicies, distributedEvictionPolicies, centralTriggerPolicies,
				reduceFunction, new TupleKeySelector<Tuple2<Integer, String>>(1));

		stream.print();

		env.execute();
	}

	/**
	 * This data source emits one element every 0.001 sec. The output is an
	 * Integer counting the output elements. As soon as the counter reaches
	 * 10000 it is reset to 0. On each reset the source waits 5 sec. before it
	 * restarts to produce elements. The produced elements are flagged with an
	 * group. The groups are selected with round robin.
	 */
	@SuppressWarnings("serial")
	private static class CountingSourceWithSleep implements SourceFunction<Tuple2<Integer, String>> {

		private int counter = 0;
		private String[] groups = { "a", "b", "c" };

		@Override
		public void invoke(Collector<Tuple2<Integer, String>> collector) throws Exception {
			// continuous emit
			while (true) {
				if (counter > 9999) {
					System.out.println("Source pauses now!");
					Thread.sleep(5000);
					System.out.println("Source continouse with emitting now!");
					counter = 0;
				}
				collector.collect(new Tuple2<Integer, String>(counter, groups[counter
						% groups.length]));

				// Wait 0.001 sec. before the next emit. Otherwise the source is
				// too fast for local tests and you might always see
				// SUM[k=1..9999](k) as result.
				Thread.sleep(1);

				counter++;
			}
		}

	}

}
