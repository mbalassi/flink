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

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class GaborExample {
	
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

//		env.getConfig().disableObjectReuse();

		DataSet<Long> input = env.generateSequence(1,10);

		DataSet<Tuple2<Long, MyLong>> stream = input.map(new StoreFirstMapper())
			.map(new Negate());

		stream.print();
	}

	public static class StoreFirstMapper implements MapFunction<Long, Tuple2<Long, MyLong>>{

		private MyLong first = new MyLong(0L);

		@Override
		public Tuple2<Long, MyLong> map(Long value) throws Exception {
			if (first.f0 == 0) first.f0 = value;
			return Tuple2.of(value, first);
		}
	}

	public static class Negate implements MapFunction<Tuple2<Long, MyLong>, Tuple2<Long, MyLong>>{

		@Override
		public Tuple2<Long, MyLong> map(Tuple2<Long, MyLong> in) throws Exception {
			in.f0 = -1 * in.f0;
			in.f1.f0 = -1 * in.f1.f0;
			return in;
		}
	}

	public static class MyLong implements Serializable {
		public long f0;

		@Override
		public String toString() {
			return String.valueOf(f0);
		}

		public MyLong(long f0) {
			this.f0 = f0;
		}
	}
}
