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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.KVStore.KVUtils.KVKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;

@SuppressWarnings("rawtypes")
public class KVStore<K, V> {
	private List<Tuple2<DataStream<Tuple2<K, V>>, Integer>> put = new ArrayList<>();
	private List<Tuple2<DataStream<K>, Integer>> get = new ArrayList<>();
	private List<Tuple2<DataStream<K>, Integer>> remove = new ArrayList<>();
	private List<Tuple3<DataStream, KeySelector, Integer>> sget = new ArrayList<>();
	private List<Tuple2<DataStream<K[]>, Integer>> multiGet = new ArrayList<>();

	private List<DataStream<KVOperation<K, V>>> opStream = new ArrayList<>();
	private boolean finalized = false;

	private int queryCount = 0;

	public void put(DataStream<Tuple2<K, V>> stream) {
		checkNotFinalized();
		put.add(Tuple2.of(stream, ++queryCount));
	}

	public int get(DataStream<K> stream) {
		checkNotFinalized();
		get.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}
	
	public int remove(DataStream<K> stream) {
		checkNotFinalized();
		remove.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	public <X> int getWithKeySelector(DataStream<X> stream, KeySelector<X, K> keySelector) {
		checkNotFinalized();
		sget.add(Tuple3.of((DataStream) stream, (KeySelector) keySelector, ++queryCount));
		return queryCount;
	}

	public int multiGet(DataStream<K[]> stream) {
		checkNotFinalized();
		multiGet.add(Tuple2.of(stream, ++queryCount));
		return queryCount;
	}

	public void checkNotFinalized() {
		if (finalized) {
			throw new IllegalStateException("Cannot operate on the Store after getting the outputs.");
		}
	}

	@SuppressWarnings("unchecked")
	public KVOperationOutputs<K, V> getOutputs() {
		finalized = true;
		if (put.isEmpty()) {
			throw new RuntimeException("At least one Put stream needs to be added.");
		}
		final TupleTypeInfo<Tuple2<K, V>> tupleType = (TupleTypeInfo<Tuple2<K, V>>) put.get(0).f0.getType();
		final KVOperationTypeInfo<K, V> kvType = new KVOperationTypeInfo<K, V>(
				(TypeInformation<K>) tupleType.getTypeAt(0), (TypeInformation<V>) tupleType.getTypeAt(1));

		for (Tuple2<DataStream<Tuple2<K, V>>, Integer> query : put) {
			opStream.add(query.f0.groupBy(0).map(new KVUtils.ToPut<K, V>(query.f1)).returns(kvType));
		}
		for (Tuple2<DataStream<K>, Integer> query : get) {
			opStream.add(query.f0.groupBy(new KVUtils.SelfKeyExtractor<K>())
					.map(new KVUtils.ToGet<K, V>(query.f1)).returns(kvType));
		}
		for (Tuple2<DataStream<K>, Integer> query : remove) {
			opStream.add(query.f0.groupBy(new KVUtils.SelfKeyExtractor<K>())
					.map(new KVUtils.ToRemove<K, V>(query.f1)).returns(kvType));
		}
		for (Tuple3<DataStream, KeySelector, Integer> query : sget) {
			kvType.registerExtractor(query.f2, query.f0.getType(), query.f1);
			opStream.add(query.f0.groupBy(query.f1).map(new KVUtils.ToSGet<>(query.f2)).returns(kvType));
		}
		for (Tuple2<DataStream<K[]>, Integer> query : multiGet) {

			opStream.add(query.f0.flatMap(new KVUtils.ToMGet<K, V>(query.f1)).returns(kvType)
					.groupBy(new KVKeySelector<K, V>()));
		}

		DataStream<KVOperation<K, V>> input = opStream.get(0);
		for (int i = 1; i < opStream.size(); i++) {
			input = input.union(opStream.get(i));
		}

		SplitDataStream<KVOperation<K, V>> split = input.transform("KVStore", kvType,
				new KVStoreOperator<K, V>()).split(new KVUtils.IDOutputSelector<K, V>());

		Map<Integer, DataStream<KV<K, V>>> kvStreams = new HashMap<>();
		Map<Integer, DataStream> skvStreams = new HashMap<>();
		Map<Integer, DataStream<KV<K, V>[]>> mkvStreams = new HashMap<>();

		for (Tuple2<DataStream<K>, Integer> query : get) {
			DataStream<KV<K, V>> projected = split.select(query.f1.toString()).map(new KVUtils.ToKV<K, V>())
					.returns(new KVTypeInfo<K, V>(kvType.keyType, kvType.valueType));
			kvStreams.put(query.f1, projected);
		}
		
		for (Tuple2<DataStream<K>, Integer> query : remove) {
			DataStream<KV<K, V>> projected = split.select(query.f1.toString()).map(new KVUtils.ToKV<K, V>())
					.returns(new KVTypeInfo<K, V>(kvType.keyType, kvType.valueType));
			kvStreams.put(query.f1, projected);
		}

		for (Tuple3<DataStream, KeySelector, Integer> query : sget) {
			DataStream projected = split.select(query.f2.toString()).map(new KVUtils.ToSKV<K, V>())
					.returns(new KVTypeInfo(query.f0.getType(), kvType.valueType));
			skvStreams.put(query.f2, projected);
		}

		for (Tuple2<DataStream<K[]>, Integer> query : multiGet) {
			DataStream<KV<K, V>[]> projected = split
					.select(query.f1.toString())
					.groupBy(new KVUtils.OperationIDSelector<K, V>())
					.flatMap(new KVUtils.MGetMerge<K, V>())
					.returns(
							new KVArrayTypeInfo<K, V>(new KVTypeInfo<K, V>(kvType.keyType, kvType.valueType)));
			mkvStreams.put(query.f1, projected);
		}

		return new KVOperationOutputs<K, V>(kvStreams, skvStreams, mkvStreams);

	}

	public static class KVOperationOutputs<K, V> {

		private Map<Integer, DataStream<KV<K, V>>> kvStreams;
		private Map<Integer, DataStream> skvStreams;
		private Map<Integer, DataStream<KV<K, V>[]>> mkvStreams;

		public KVOperationOutputs(Map<Integer, DataStream<KV<K, V>>> kvStreams,
				Map<Integer, DataStream> skvStreams, Map<Integer, DataStream<KV<K, V>[]>> mkvStreams) {
			this.kvStreams = kvStreams;
			this.skvStreams = skvStreams;
			this.mkvStreams = mkvStreams;
		}

		public DataStream<KV<K, V>> getKVStream(int queryID) {
			if (kvStreams.containsKey(queryID)) {
				return kvStreams.get(queryID);
			} else {
				throw new IllegalArgumentException("Given query ID does not correspond to a KV stream.");
			}
		}

		@SuppressWarnings("unchecked")
		public <X> DataStream<KV<X, V>> getCustomKVStream(int queryID) {
			if (skvStreams.containsKey(queryID)) {
				return skvStreams.get(queryID);
			} else {
				throw new IllegalArgumentException(
						"Given query ID does not correspond to an extracted KV stream.");
			}
		}

		public <X> DataStream<KV<K, V>[]> getKVArrayStream(int queryID) {
			if (mkvStreams.containsKey(queryID)) {
				return mkvStreams.get(queryID);
			} else {
				throw new IllegalArgumentException("Given query ID does not correspond to a multi-KV stream.");
			}
		}

	}

}
