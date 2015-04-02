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

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.invokable.operator.windowing.StreamDiscretizer;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.util.Collector;

public class StreamJoinNew {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		List<Tuple2<String, Integer>> input1 = new ArrayList<Tuple2<String, Integer>>();
		input1.add(new Tuple2<String, Integer>("a", 1));
		input1.add(new Tuple2<String, Integer>("b", 4));
		input1.add(new Tuple2<String, Integer>("b", 1));
		input1.add(new Tuple2<String, Integer>("a", 2));
		input1.add(new Tuple2<String, Integer>("c", 1));
		input1.add(new Tuple2<String, Integer>("d", 1));

		List<Tuple2<String, Integer>> input2 = new ArrayList<Tuple2<String, Integer>>();
		input2.add(new Tuple2<String, Integer>("a", -1));
		input2.add(new Tuple2<String, Integer>("b", -4));
		input2.add(new Tuple2<String, Integer>("b", -7));
		input2.add(new Tuple2<String, Integer>("a", -9));
		input2.add(new Tuple2<String, Integer>("c", -1));
		input2.add(new Tuple2<String, Integer>("d", -2));

		DataStream<Tuple2<String, Integer>> source1 = env.fromCollection(input1);
		DataStream<Tuple2<String, Integer>> source2 = env.fromCollection(input2);

		TypeInformation<WindowEvent<Tuple2<String, Integer>>> bufferEventType = new TupleTypeInfo(
				WindowEvent.class, source1.getType(), BasicTypeInfo.INT_TYPE_INFO);

		DataStream<WindowEvent<Tuple2<String, Integer>>> d1 = source1.transform("D1",
				bufferEventType, new StreamDiscretizer<Tuple2<String, Integer>>(
						new CountTriggerPolicy<Tuple2<String, Integer>>(1),
						new CountEvictionPolicy<Tuple2<String, Integer>>(2)));

		DataStream<WindowEvent<Tuple2<String, Integer>>> d2 = source2.transform("D2",
				bufferEventType, new StreamDiscretizer<Tuple2<String, Integer>>(
						new CountTriggerPolicy<Tuple2<String, Integer>>(1),
						new CountEvictionPolicy<Tuple2<String, Integer>>(2)));

		DataStream<Tuple2<Integer, Integer>> joined = d1.groupBy(0).connect(d2.groupBy(0))
				.flatMap(new JoinCoFunc());

		joined.print();

		// System.out.println(env.getExecutionPlan());
		env.execute();

	}

	public static class JoinCoFunc
			implements
			CoFlatMapFunction<WindowEvent<Tuple2<String, Integer>>, WindowEvent<Tuple2<String, Integer>>, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;
		Map<Object, List<Tuple2<String, Integer>>> map1 = new HashMap<Object, List<Tuple2<String, Integer>>>();
		Queue<Tuple2<String, Integer>> elements1 = new LinkedList<Tuple2<String, Integer>>();

		Map<Object, List<Tuple2<String, Integer>>> map2 = new HashMap<Object, List<Tuple2<String, Integer>>>();
		Queue<Tuple2<String, Integer>> elements2 = new LinkedList<Tuple2<String, Integer>>();

		KeySelector<Tuple2<String, Integer>, String> key = new KeySelector<Tuple2<String, Integer>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		};

		@Override
		public void flatMap1(WindowEvent<Tuple2<String, Integer>> value,
				Collector<Tuple2<Integer, Integer>> out) throws Exception {

			if (value.isElement()) {
				Object ckey = key.getKey(value.getElement());
				elements1.add(value.getElement());
				List<Tuple2<String, Integer>> current = map1.get(ckey);
				if (current == null) {
					current = new LinkedList<Tuple2<String, Integer>>();
					map1.put(ckey, current);
				}
				current.add(value.getElement());

				// Do the join on the other map
				List<Tuple2<String, Integer>> matching = map2.get(ckey);
				if (matching != null) {
					for (Tuple2<String, Integer> match : matching) {
						out.collect(new Tuple2<Integer, Integer>(value.getElement().f1, match.f1));
					}
				}

			} else if (value.isEviction()) {
				for (int i = 0; i < value.getEviction(); i++) {
					Tuple2<String, Integer> toRemove = elements1.remove();
					Object ckey = key.getKey(toRemove);
					List<Tuple2<String, Integer>> current = map1.get(ckey);
					current.remove(toRemove);
					if (current.isEmpty()) {
						map1.remove(ckey);
					}
				}
			}

		}

		@Override
		public void flatMap2(WindowEvent<Tuple2<String, Integer>> value,
				Collector<Tuple2<Integer, Integer>> out) throws Exception {
			if (value.isElement()) {
				Object ckey = key.getKey(value.getElement());
				elements2.add(value.getElement());
				List<Tuple2<String, Integer>> current = map2.get(ckey);
				if (current == null) {
					current = new LinkedList<Tuple2<String, Integer>>();
					map2.put(ckey, current);
				}
				current.add(value.getElement());

				// Do the join on the other map
				List<Tuple2<String, Integer>> matching = map1.get(ckey);
				if (matching != null) {
					for (Tuple2<String, Integer> match : matching) {
						out.collect(new Tuple2<Integer, Integer>(match.f1, value.getElement().f1));
					}
				}

			} else if (value.isEviction()) {
				for (int i = 0; i < value.getEviction(); i++) {
					Tuple2<String, Integer> toRemove = elements2.remove();
					Object ckey = key.getKey(toRemove);
					List<Tuple2<String, Integer>> current = map2.get(ckey);
					current.remove(toRemove);
					if (current.isEmpty()) {
						map2.remove(ckey);
					}
				}
			}

		}

	}

}
