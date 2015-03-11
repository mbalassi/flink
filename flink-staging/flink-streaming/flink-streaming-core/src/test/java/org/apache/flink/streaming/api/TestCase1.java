package org.apache.flink.streaming.api;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.source.GenSequenceFunction;
import org.apache.flink.streaming.api.invokable.operator.WindowInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowReduceInvokable;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class TestCase1 {

	/**
	 * The main program
	 * 
	 * @param args
	 *            0:maxValue for data source 1:window size 2:slide size
	 *            3:version
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		runTest(Long.parseLong(args[0]),Long.parseLong(args[1]),Long.parseLong(args[2]),Integer.parseInt(args[3]));
		
	}

	public static long runTest(long maxValue, long windowSize, long slideSize, int version) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		Timestamp<Long> timestamp = new TrivialTimestamp();

		DataStream<Long> testDataStream = env.generateSequence(0, maxValue);

		ReduceFunction<Long> maxLong = new ReduceFunction<Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				if (value1 > value2)
					return value1;
				else
					return value2;
			}

		};

		switch (version) {
		case 1:
			/**
			 * Version 1 => Current master
			 */
			testDataStream = testDataStream.window(Time.of(windowSize, timestamp))
					.every(Time.of(slideSize, timestamp)).max(0).flatten();
			break;
		case 2:
			LinkedList<TriggerPolicy<Long>> triggers = new LinkedList<TriggerPolicy<Long>>();
			LinkedList<EvictionPolicy<Long>> evictors = new LinkedList<EvictionPolicy<Long>>();
			triggers.add(new TimeTriggerPolicy<Long>(slideSize, new TimestampWrapper<Long>(
					timestamp, 0)));
			evictors.add(new TimeEvictionPolicy<Long>(windowSize, new TimestampWrapper<Long>(
					timestamp, 0)));

			testDataStream = testDataStream.transform("WindowInv", testDataStream.getType(),
					new WindowInvokable<Long, Long>(maxLong, triggers, evictors));
			break;
		case 3:
			/**
			 * Version 3 => 0.7.0 Version
			 */
			testDataStream = testDataStream.transform("WindowInv", testDataStream.getType(),
					new WindowReduceInvokable<Long>(maxLong, windowSize, slideSize,
							new TimestampWrapper<Long>(timestamp, 0)));

		}

		// testDataStream.print();

		long beginTime = System.currentTimeMillis();
		env.execute("Example");
		long duration = System.currentTimeMillis() - beginTime;
		System.out.println("**********************************************\n DURATION: "
				+ duration + "\n");
		return duration;
	}

	private static class TrivialTimestamp implements Timestamp<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Long value) {
			return value;
		}

	}
}