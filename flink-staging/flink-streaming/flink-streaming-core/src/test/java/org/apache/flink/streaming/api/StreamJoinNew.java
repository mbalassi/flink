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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.temporal.StreamJoinOperator.DefaultJoinFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.operators.windowing.StreamDiscretizer;
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

		KeySelector<Tuple2<String, Integer>, String> key = new KeySelector<Tuple2<String, Integer>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		};

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

		DataStream<Tuple2<Integer, Integer>> joined = d1
				.groupBy(0)
				.connect(d2.groupBy(0))
				.flatMap(
						new JoinCoFunc(
								new DefaultJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>(),
								key, key))
				.returns("Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>>");

		System.out.println(joined.getType());

		joined.print();

		// System.out.println(env.getExecutionPlan());
		env.execute();

	}

	public static class JoinCoFunc<IN1, IN2, OUT> implements
			CoFlatMapFunction<WindowEvent<IN1>, WindowEvent<IN2>, OUT> {

		private static final long serialVersionUID = 1L;
		private JoinBuffer<IN1> buffer1;
		private JoinBuffer<IN2> buffer2;
		private JoinFunction<IN1, IN2, OUT> joinFunction;

		public JoinCoFunc(JoinFunction<IN1, IN2, OUT> joinFunction, KeySelector<IN1, ?> key1,
				KeySelector<IN2, ?> key2) {
			this.joinFunction = joinFunction;
			this.buffer1 = new JoinBuffer<IN1>(key1);
			this.buffer2 = new JoinBuffer<IN2>(key2);
		}

		@Override
		public void flatMap1(WindowEvent<IN1> value, Collector<OUT> out) throws Exception {
			if (value.isElement()) {
				buffer1.addElement(value.getElement());
			} else if (value.isEviction()) {
				buffer1.evict(value.getEviction());
			} else if (value.isTrigger()) {
				joinBuffers(out);
			}
		}

		@Override
		public void flatMap2(WindowEvent<IN2> value, Collector<OUT> out) throws Exception {
			if (value.isElement()) {
				buffer2.addElement(value.getElement());
			} else if (value.isEviction()) {
				buffer2.evict(value.getEviction());
			} else if (value.isTrigger()) {
				joinBuffers(out);
			}
		}

		private void joinBuffers(Collector<OUT> out) throws Exception {
			for (Entry<Object, List<IN1>> entry : buffer1.joinMap.entrySet()) {
				if (buffer2.joinMap.containsKey(entry.getKey())) {
					for (IN1 first : entry.getValue()) {
						for (IN2 second : buffer2.joinMap.get(entry.getKey())) {
							out.collect(joinFunction.join(first, second));
						}
					}
				}
			}
		}

		private class JoinBuffer<T> implements Serializable {
			private static final long serialVersionUID = 1L;

			private Map<Object, List<T>> joinMap = new HashMap<Object, List<T>>();
			private Queue<T> elements = new LinkedList<T>();
			private KeySelector<T, ?> key;

			public JoinBuffer(KeySelector<T, ?> key) {
				this.key = key;
			}

			public void addElement(T element) throws Exception {
				Object ckey = key.getKey(element);
				elements.add(element);
				List<T> current = joinMap.get(ckey);
				if (current == null) {
					current = new LinkedList<T>();
					joinMap.put(ckey, current);
				}
				current.add(element);
			}

			public void evict(int toEvict) throws Exception {
				for (int i = 0; i < toEvict; i++) {
					T toRemove = elements.remove();
					Object ckey = key.getKey(toRemove);
					List<T> current = joinMap.get(ckey);
					current.remove(toRemove);
					if (current.isEmpty()) {
						joinMap.remove(ckey);
					}
				}
			}

		}

	}

}
