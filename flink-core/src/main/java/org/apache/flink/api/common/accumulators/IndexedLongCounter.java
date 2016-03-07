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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 */
@Public
public class IndexedLongCounter implements Accumulator<Tuple2<Integer, Long>, HashMap<Integer, Long>> {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, Long> localValue = new HashMap<>();
	
	@Override
	public void add(Tuple2<Integer, Long> value) {
		if (!localValue.containsKey(value.f0)){
			localValue.put(value.f0, value.f1);
		} else {
			localValue.put(value.f0, localValue.get(value.f0) + value.f1);
		}
	}

	@Override
	public HashMap<Integer, Long> getLocalValue() {
		return localValue;
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<Tuple2<Integer, Long>, HashMap<Integer, Long>> other) {
		HashMap<Integer, Long> otherValue = other.getLocalValue();
		for (Map.Entry<Integer, Long> pair : otherValue.entrySet()) {
			add(Tuple2.of(pair.getKey(), pair.getValue()));
		}
	}

	@Override
	public Accumulator<Tuple2<Integer, Long>, HashMap<Integer, Long>> clone() {
		IndexedLongCounter newInstance = new IndexedLongCounter();
		newInstance.localValue = new HashMap<>();
		return newInstance;
	}

	@Override
	public String toString() {
		return "LongArray Accumulator " + localValue;
	}
}
