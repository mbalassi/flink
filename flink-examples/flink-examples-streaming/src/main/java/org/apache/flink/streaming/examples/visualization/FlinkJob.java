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

package org.apache.flink.streaming.examples.visualization;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.examples.wordcount.WordCount;

import java.util.Random;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 *
 */
public class FlinkJob {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> rants = env.addSource(new FlinkRantGenerator()).name("Flink Rant Generator");

		// *************************************************************************
		// WindowWordCount on rants
		// *************************************************************************

		final int windowSize = params.getInt("window", 250);
		final int slideSize = params.getInt("slide", 150);

		DataStream<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		rants.flatMap(new WordCount.Tokenizer())
				.name("Tokenizer")
				// create windows of windowSize records slided every slideSize records
				.keyBy(0)
				.countWindow(windowSize, slideSize)
				// group by the tuple field "0" and sum up tuple field "1"
				.sum(1)
				.name("Keyed Window Sum");

		// print result
		counts.print();

		// *************************************************************************
		// Co-mentions
		// *************************************************************************

		DataStream<String> coMentions =
			rants.filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String value) throws Exception {
					return value.toLowerCase().contains("flink")
						&& value.toLowerCase().contains("berlin");
				}
			}).name("Flink-Berlin Comentions");

		coMentions.print();

		// execute program
		env.execute("WindowWordCount");
	}

	public static class FlinkRantGenerator implements ParallelSourceFunction<String>{

		private static String[] rants = {"Flink is awesome.",
										"Flink Forward is in Berlin.",
										"RBEA is a stream processor inside a stream processor."};

		private Random rnd;

		private volatile boolean isRunning;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			isRunning = true;
			rnd = new Random();

			while (isRunning) {
				ctx.collect(rants[rnd.nextInt(rants.length)]);
				Thread.sleep(100);
			}

		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
