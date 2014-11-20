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
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.util.keys.TupleKeySelector;
import org.apache.flink.util.Collector;

/**
 * A minimal example to show grouped windowing based on policies.
 */
public class GroupedBasicExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		// This reduce function does a String concat.
		ReduceFunction<Tuple2<String, String>> reduceFunction = new ReduceFunction<Tuple2<String, String>>() {

			/**
			 * Auto generates version ID
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> reduce(Tuple2<String, String> value1,
					Tuple2<String, String> value2) throws Exception {
				return new Tuple2<String, String>(value1.f0 + "|" + value2.f0, value1.f1 + "|"
						+ value2.f1);
			}

		};

		// Make a count based trigger:
		LinkedList<CloneableTriggerPolicy<Tuple2<String, String>>> distributedTriggerPolicies = new LinkedList<CloneableTriggerPolicy<Tuple2<String, String>>>();
		distributedTriggerPolicies.add(new CountTriggerPolicy<Tuple2<String, String>>(2));

		// Make a count based eviction:
		LinkedList<CloneableEvictionPolicy<Tuple2<String, String>>> distributedEvictionPolicies = new LinkedList<CloneableEvictionPolicy<Tuple2<String, String>>>();
		distributedEvictionPolicies.add(new CountEvictionPolicy<Tuple2<String, String>>(3));

		// Make empty list for central triggers
		LinkedList<TriggerPolicy<Tuple2<String, String>>> centralTriggerPolicies = new LinkedList<TriggerPolicy<Tuple2<String, String>>>();

		DataStream<Tuple2<Tuple2<String, String>, String[]>> stream = env.addSource(
				new BasicSource()).groupedWindow(distributedTriggerPolicies,
				distributedEvictionPolicies, centralTriggerPolicies, reduceFunction,
				new TupleKeySelector<Tuple2<String, String>>(1));

		stream.print();

		env.execute();
	}

	public static class BasicSource implements SourceFunction<Tuple2<String, String>> {

		private static final long serialVersionUID = 1L;
		String str = new String("no.");
		String[] groups = { "a", "b", "c" };
		int counter = 0;

		@Override
		public void invoke(Collector<Tuple2<String, String>> out) throws Exception {
			// continuous emit
			while (true) {
				// Outputs elements of the defined groups using round robin
				out.collect(new Tuple2<String, String>(str + counter, groups[counter++
						% groups.length]));
			}
		}
	}

}
