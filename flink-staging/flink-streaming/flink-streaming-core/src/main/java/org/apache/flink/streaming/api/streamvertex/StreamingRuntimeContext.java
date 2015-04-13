/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.streamvertex;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import akka.pattern.Patterns;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobmanager.CheckpointedStateRequest;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.OperatorState;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * Implementation of the {@link RuntimeContext}, created by runtime stream UDF
 * operators.
 */
public class StreamingRuntimeContext extends RuntimeUDFContext {

	private final Environment env;
	private final Map<String, OperatorState<?>> operatorStates;

	public StreamingRuntimeContext(String name, Environment env, ClassLoader userCodeClassLoader,
			ExecutionConfig executionConfig, Map<String, OperatorState<?>> operatorStates) {
		super(name, env.getNumberOfSubtasks(), env.getIndexInSubtaskGroup(), userCodeClassLoader,
				executionConfig, env.getCopyTask());
		this.env = env;
		this.operatorStates = operatorStates;
	}

	/**
	 * Returns the operator state registered by the given name for the operator.
	 * 
	 * @param name
	 *            Name of the operator state to be returned.
	 * @return The operator state.
	 */
	public OperatorState<?> getState(String name) {
		if (operatorStates == null) {
			throw new RuntimeException("No state has been registered for this operator.");
		} else {
			OperatorState<?> state = operatorStates.get(name);
			if (state != null) {
				return state;
			} else {
				throw new RuntimeException("No state has been registered for the name: " + name);
			}
		}
	}

	/**
	 * Returns whether there is a state stored by the given name
	 */
	public boolean containsState(String name) {
		return operatorStates.containsKey(name);
	}

	/**
	 * This is a beta feature </br></br> Register an operator state for this
	 * operator by the given name. This name can be used to retrieve the state
	 * during runtime using {@link StreamingRuntimeContext#getState(String)}. To
	 * obtain the {@link StreamingRuntimeContext} from the user-defined function
	 * use the {@link RichFunction#getRuntimeContext()} method.
	 * 
	 * @param name
	 *            The name of the operator state.
	 * @param state
	 *            The state to be registered for this name.
	 */
	public void registerState(String name, OperatorState<?> state) {
		if (state == null) {
			throw new RuntimeException("Cannot register null state");
		} else {
			if (operatorStates.containsKey(name)) {
				throw new RuntimeException("State is already registered");
			} else {
				operatorStates.put(name, state);
			}
		}
	}

	/**
	 * Returns the input split provider associated with the operator.
	 * 
	 * @return The input split provider.
	 */
	public InputSplitProvider getInputSplitProvider() {
		return env.getInputSplitProvider();
	}

	/**
	 * Returns the stub parameters associated with the {@link TaskConfig} of the
	 * operator.
	 * 
	 * @return The stub parameters.
	 */
	public Configuration getTaskStubParameters() {
		return new TaskConfig(env.getTaskConfiguration()).getStubParameters();
	}

	//FIXME: get a configured akka timeout from the config
	private Future getCheckpointedStates() {
		return Patterns.ask(env.getJobManager(),
				new CheckpointedStateRequest(env.getJobID(), env.getJobVertexId(), env.getIndexInSubtaskGroup()), 5000);
	}

	@SuppressWarnings("uncecked")
	public java.util.concurrent.Future<OperatorState<?>> getCheckpointedState(String name){

		return new StateFuture(getCheckpointedStates(), name);
	}

	private class StateFuture implements java.util.concurrent.Future<OperatorState<?>>{

		Future future;
		String stateId;

		StateFuture(Future future, String stateId){
			this.future = future;
			this.stateId = stateId;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return future.isCompleted();
		}

		@Override
		public OperatorState get() throws InterruptedException, ExecutionException {
			try {
				Map<String, OperatorState<?>> stateMap = ((LocalStateHandle) Await.result(future, Duration.apply(5000, TimeUnit.MILLISECONDS)))
						.getState(env.getUserClassLoader());
				return stateMap != null ? stateMap.get(stateId) : new OperatorState(null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public OperatorState get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return null;
		}
	}

}
