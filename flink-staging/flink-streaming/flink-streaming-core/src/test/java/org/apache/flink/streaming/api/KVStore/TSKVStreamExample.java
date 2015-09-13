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

package org.apache.flink.streaming.api.KVStore;

import org.apache.flink.streaming.api.KVStore.KVStore;
import org.apache.flink.streaming.api.KVStore.KVStoreOutput;
import org.apache.flink.streaming.api.KVStore.TimestampedKVStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.KV;

public class TSKVStreamExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().enableTimestamps();
		// Create a new Key-Value store
		KVStore<String, Integer> store = new TimestampedKVStore<>();

		DataStream<KV<String, Integer>> put1 = env.addSource(new PutSource1());
		DataStream<KV<String, Integer>> put2 = env.addSource(new PutSource2());

		DataStream<String> get = env.addSource(new GetSource());

		store.put(put1);
		store.put(put2);

		int id1 = store.get(get);

		// Finalize the KV store operations and get the result streams
		KVStoreOutput<String, Integer> storeOutputs = store.getOutputs();

		// Fetch the result streams for the 2 get queries using the assigned IDs
		// and print the results
		storeOutputs.getKVStream(id1).print();

		env.execute();
	}

	public static class PutSource1 implements SourceFunction<KV<String, Integer>>,
			EventTimeSourceFunction<KV<String, Integer>> {

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<KV<String, Integer>> ctx)
				throws Exception {

			ctx.collectWithTimestamp(KV.of("a", 2), 1);
			ctx.collectWithTimestamp(KV.of("b", 3), 4);
			ctx.emitWatermark(new Watermark(4L));
			ctx.collectWithTimestamp(KV.of("a", 1), 8);
			ctx.collectWithTimestamp(KV.of("c", 4), 12);
			ctx.emitWatermark(new Watermark(12L));
		}

		@Override
		public void cancel() {
		}

	}

	public static class PutSource2 implements SourceFunction<KV<String, Integer>>,
			EventTimeSourceFunction<KV<String, Integer>> {

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<KV<String, Integer>> ctx)
				throws Exception {

			ctx.collectWithTimestamp(KV.of("b", 1), 7);
			ctx.emitWatermark(new Watermark(7L));
			ctx.collectWithTimestamp(KV.of("c", 0), 10);
			ctx.emitWatermark(new Watermark(10L));
		}

		@Override
		public void cancel() {
		}

	}

	public static class GetSource implements SourceFunction<String>, EventTimeSourceFunction<String> {

		@Override
		public void run(
				org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<String> ctx)
				throws Exception {
			ctx.collectWithTimestamp("a", 2);
			ctx.collectWithTimestamp("b", 3);
			ctx.emitWatermark(new Watermark(3L));
			ctx.collectWithTimestamp("b", 5);
			ctx.collectWithTimestamp("b", 7);
			ctx.emitWatermark(new Watermark(7L));

			ctx.collectWithTimestamp("a", 9);
			ctx.collectWithTimestamp("c", 11);
			ctx.collectWithTimestamp("c", 13);
			ctx.emitWatermark(new Watermark(13L));

		}

		@Override
		public void cancel() {
			// TODO Auto-generated method stub

		}

	}
}
