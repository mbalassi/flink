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

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.KVStore.KV;
import org.apache.flink.streaming.api.KVStore.KVStore;
import org.apache.flink.streaming.api.KVStore.KVStore.KVOperationOutputs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class KVStreamExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		KVStore<String, Integer> store = new KVStore<>();

		DataStream<Tuple2<String, Integer>> putStream = env.socketTextStream("localhost", 9999).flatMap(
				new Parser());
		DataStream<String> getStream1 = env.socketTextStream("localhost", 9998);
		DataStream<String[]> getStream2 = env.socketTextStream("localhost", 9997).map(
				new MapFunction<String, String[]>() {

					@Override
					public String[] map(String value) throws Exception {
						return value.split(",");
					}

				});

		store.put(putStream);
		int id1 = store.remove(getStream1);
		int id2 = store.multiGet(getStream2);

		KVOperationOutputs<String, Integer> storeOutputs = store.getOutputs();

		storeOutputs.getKVStream(id1).print();
		storeOutputs.getKVArrayStream(id2).addSink(new SinkFunction<KV<String, Integer>[]>() {

			@Override
			public void invoke(KV<String, Integer>[] value) throws Exception {
				System.out.println(Arrays.toString(value));
			}
		});

		env.execute();

	}

	public static class Parser implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			try {
				String[] split = value.split(",");
				out.collect(Tuple2.of(split[0], Integer.valueOf(split[1])));
			} catch (Exception e) {

			}
		}
	}
}
