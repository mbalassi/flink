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

package org.apache.flink.streaming.state;

import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.fail;

public class OperatorCheckPointITCase {

	private static final int PARALLELISM = 1;
	private static final long MEMORY_SIZE = 32;
	private static final long CHECKPOINT_INTERVAL = 200;

	Future<OperatorState<?>> checkpointedState;

	@Test
	public void testOperatorGetCheckpoint() {
		StreamExecutionEnvironment  env = new TestStreamEnvironment(PARALLELISM, MEMORY_SIZE);
		env.enableCheckpointing(CHECKPOINT_INTERVAL);
		env.getConfig().setNumberOfExecutionRetries(1);

		env.addSource(new StatefulGenerateSequence()).addSink(new SinkFunction<Long>() {
			@Override
			public void invoke(Long value) throws Exception {
				// do nothing
			}
		});

		try {
			env.execute();
		} catch (SuccessException e) {
			// expected behaviour
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class StatefulGenerateSequence extends RichParallelSourceFunction<Long> {

		private long counter = 0l;
		private volatile boolean isRunning = false;
		Future<OperatorState<?>> checkpointedState;

		@Override
		@SuppressWarnings("unchecked")
		public void run(Collector<Long> collector) throws Exception {
			isRunning = true;
			StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
			OperatorState<Long> collectedState = new OperatorState<Long>(0L);
			context.registerState("collected", collectedState);

			while (isRunning) {
				checkpointedState = context.getCheckpointedState("collected");
				collector.collect(counter);
				collectedState.update(counter);
				counter++;
				
				if (checkpointedState.get() != null)
					if (checkpointedState.get().getState() != null) {
						if ((Long) checkpointedState.get().getState() > 0) {
							isRunning = false;
						 }
					}

				checkpointedState = context.getCheckpointedState("collected");

			}

		}

		@Override
		public void cancel() {
			checkpointedState.cancel(true);
		}
	}

	public static class SuccessException extends Exception {
		private static final long serialVersionUID = 1L;
	}
}
