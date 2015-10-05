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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 * 
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 * 
 */
public class WindowFoldExample {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.generateSequence(1, 100).map(new MapFunction<Long, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Long value) throws Exception {
				return Tuple2.of(value % 5, value);
			}
		}).keyBy(0).window(Count.of(20)).foldWindow(Tuple2.of(0L, new Long[]{}), new FoldFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>() {
			@Override
			public Tuple2<Long, Long[]> fold(Tuple2<Long, Long[]> accumulator, Tuple2<Long, Long> value) throws Exception {
				Long[] array = new Long[accumulator.f1.length + 1];
				for (int i = 0; i < accumulator.f1.length ; i++) {
					array[i] = accumulator.f1[i];
				}
				array[array.length - 1] = value.f1;
				return Tuple2.of(value.f0, array);
			}
		}).flatten().print();

		// execute program
		env.execute("Streaming WordCount");
	}
}
