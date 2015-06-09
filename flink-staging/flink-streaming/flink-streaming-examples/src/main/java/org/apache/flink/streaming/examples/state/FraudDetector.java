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

package org.apache.flink.streaming.examples.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.examples.state.StateMachine.STATE;
import org.apache.flink.streaming.examples.state.StateMachine.TRANSITION;

/**
 * Fraud detector based on a state machine. State is persisted by Flink.
 */
public class FraudDetector {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(100);
		env.setNumberOfExecutionRetries(10);

		DataStream<Tuple3<STATE, TRANSITION, STATE>> transitions =
				env.addSource(new TransitionSource())
					.map(new StateMap())
					.writeAsCsv(args[0], FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("FraudDetector");
	}

	public static class TransitionSource implements ParallelSourceFunction<TRANSITION>,
													CheckpointedAsynchronously<STATE>{

		private volatile boolean isRunning = false;
		private StateMachine stateMachine = new StateMachine();
		private STATE currentState = STATE.IDLE;

		@Override
		public void run(SourceContext<TRANSITION> ctx) throws Exception {
			Object lock = ctx.getCheckpointLock();
			isRunning = true;
			while (isRunning){
				TRANSITION transition = stateMachine.generate();
				synchronized (lock) {
					currentState = stateMachine.getState();
					ctx.collect(transition);
					Thread.sleep(1000);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public STATE snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return currentState;
		}

		@Override
		public void restoreState(STATE state) {
			stateMachine.setState(state);
		}
	}

	public static class StateMap implements MapFunction<TRANSITION, Tuple3<STATE, TRANSITION, STATE>>,
											CheckpointedAsynchronously<STATE> {

		private StateMachine stateMachine = new StateMachine();
		private STATE currentState = STATE.IDLE;

		@Override
		public Tuple3<STATE, TRANSITION, STATE> map(TRANSITION transition) throws Exception {
			STATE oldState = currentState;
			currentState = stateMachine.transition(transition);
			return new Tuple3<STATE, TRANSITION, STATE>(oldState, transition, currentState);
		}

		@Override
		public STATE snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return currentState;
		}

		@Override
		public void restoreState(STATE state) {
			stateMachine.setState(state);
		}
	}

}
