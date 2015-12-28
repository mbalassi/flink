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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.examples.java.ml.util.LinearRegressionData;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * This example implements a basic Linear Regression  to solve the y = theta0 + theta1*x problem using batch gradient descent algorithm.
 *
 * <p>
 * Linear Regression with BGD(batch gradient descent) algorithm is an iterative clustering algorithm and works as follows:<br>
 * Giving a data set and target set, the BGD try to find out the best parameters for the data set to fit the target set.
 * In each iteration, the algorithm computes the gradient of the cost function and use it to update all the parameters.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * With enough iteration, the algorithm can minimize the cost function and find the best parameters
 * This is the Wikipedia entry for the <a href = "http://en.wikipedia.org/wiki/Linear_regression">Linear regression</a> and <a href = "http://en.wikipedia.org/wiki/Gradient_descent">Gradient descent algorithm</a>.
 * 
 * <p>
 * This implementation works on one-dimensional data. And find the two-dimensional theta.<br>
 * It find the best Theta parameter to fit the target.
 * 
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character. The first one represent the X(the training data) and the second represent the Y(target).
 * Data points are separated by newline characters.<br>
 * For example <code>"-0.02 -0.04\n5.3 10.6\n"</code> gives two data points (x=-0.02, y=-0.04) and (x=5.3, y=10.6).
 * </ul>
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> Bulk iterations
 * <li> Broadcast variables in bulk iterations
 * <li> Custom Java objects (PoJos)
 * </ul>
 */
@SuppressWarnings("serial")
public class LinearRegression {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception{

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		List<Tuple3<Boolean, Data, Params>> typedList = new LinkedList<>();
		for (Object[] data : LinearRegressionData.DATA){
			typedList.add(Tuple3.of(true, new Data((Double) data[0], (Double) data[1]), new Params()));
		}
		typedList.add(Tuple3.of(false, new Data(), new Params(0.0, 0.0)));

		// get input x data from elements
		DataStream<Tuple3<Boolean, Data, Params>> data = env.fromCollection(typedList);

		IterativeStream<Tuple3<Boolean, Data, Params>> iteration = data.iterate(5000);

		SplitStream<Tuple3<Boolean, Data, Params>> step = iteration
				// compute a single step using every sample
				.flatMap(new SubUpdate())
				// sum up all the steps
				.flatMap(new UpdateAccumulator())
				// average the steps and update all parameters
				.map(new Update())
				//FIXME :)
				.shuffle()
				.split(new IterationSelector());

		iteration.closeWith(step.select("iterate"));

		step.select("output").writeAsText("/tmp/flink-out");

	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * A simple data sample, x means the input, and y means the target.
	 */
	public static class Data implements Serializable{
		public double x,y;

		public Data() {};

		public Data(double x ,double y){
			this.x = x;
			this.y = y;
		}

		@Override
		public String toString() {
			return "(" + x + "|" + y + ")";
		}

	}

	/**
	 * A set of parameters -- theta0, theta1.
	 */
	public static class Params implements Serializable{

		private double theta0,theta1;

		public Params(){};

		public Params(double x0, double x1){
			this.theta0 = x0;
			this.theta1 = x1;
		}

		@Override
		public String toString() {
			return theta0 + " " + theta1;
		}

		public double getTheta0() {
			return theta0;
		}

		public double getTheta1() {
			return theta1;
		}

		public void setTheta0(double theta0) {
			this.theta0 = theta0;
		}

		public void setTheta1(double theta1) {
			this.theta1 = theta1;
		}

		public Params div(Integer a){
			this.theta0 = theta0 / a ;
			this.theta1 = theta1 / a ;
			return this;
		}

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Compute a single BGD type update for every parameters.
	 */
	public static class SubUpdate extends RichFlatMapFunction<Tuple3<Boolean, Data, Params>, Tuple3<Boolean, Data, Tuple2<Params, Integer>>> {

		private Params parameter = new Params(0.0, 0.0);
		private int count = 1;

		@Override
		public void flatMap(Tuple3<Boolean, Data, Params> in,
							Collector<Tuple3<Boolean, Data, Tuple2<Params, Integer>>> collector) throws Exception {

			if (in.f0) {
				double theta_0 = parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in.f1.x)) - in.f1.y);
				double theta_1 = parameter.theta1 - 0.01 * (((parameter.theta0 + (parameter.theta1 * in.f1.x)) - in.f1.y) * in.f1.x);

				// updated params
				collector.collect(Tuple3.of(false, new Data(), Tuple2.of(new Params(theta_0, theta_1), count)));
				// data forward
				collector.collect(Tuple3.of(true, in.f1, Tuple2.of(new Params(0.0, 0.0), 0)));
			} else {
				parameter = in.f2;
			}
		}
	}

	/**  
	 * Accumulator all the update.
	 * */
	public static class UpdateAccumulator implements FlatMapFunction<Tuple3<Boolean, Data, Tuple2<Params, Integer>>,
		Tuple3<Boolean, Data, Tuple2<Params, Integer>>> {

		Tuple3<Boolean, Data, Tuple2<Params, Integer>> value = Tuple3.of(true, new Data(), Tuple2.of(new Params(0.0, 0.0), 0));

		@Override
		public void flatMap(Tuple3<Boolean, Data, Tuple2<Params, Integer>> in,
							Collector<Tuple3<Boolean, Data, Tuple2<Params, Integer>>> collector) throws Exception {
			if (in.f0){
				collector.collect(in);
			} else {
				Tuple2<Params, Integer> val1 = in.f2;
				Tuple2<Params, Integer> val2 = value.f2;

				double new_theta0 = val1.f0.theta0 + val2.f0.theta0;
				double new_theta1 = val1.f0.theta1 + val2.f0.theta1;
				Params result = new Params(new_theta0,new_theta1);

				collector.collect(Tuple3.of(false, new Data(), new Tuple2<>(result, val1.f1 + val2.f1)));
			}
		}
	}

	/**
	 * Compute the final update by average them.
	 */
	public static class Update implements MapFunction<Tuple3<Boolean, Data, Tuple2<Params, Integer>>, Tuple3<Boolean, Data, Params>> {
		@Override
		public Tuple3<Boolean, Data, Params> map(Tuple3<Boolean, Data, Tuple2<Params, Integer>> in) throws Exception {
			if(in.f0){
				return Tuple3.of(true, in.f1, new Params());
			} else {
				return Tuple3.of(false, in.f1, in.f2.f0.div(in.f2.f1));
			}
		}
	}

	public static class IterationSelector implements OutputSelector<Tuple3<Boolean, Data, Params>>{

		private Random rnd = new Random();

		@Override
		public Iterable<String> select(Tuple3<Boolean, Data, Params> value) {
			List<String> output = new ArrayList<>();
			if (rnd.nextInt(10) < 6){
				output.add("output");
			} else {
				output.add("iterate");
			}
			return output;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String dataPath = null;
	private static String outputPath = null;
	private static int numIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 3) {
				dataPath = programArguments[0];
				outputPath = programArguments[1];
				numIterations = Integer.parseInt(programArguments[2]);
			} else {
				System.err.println("Usage: LinearRegression <data path> <result path> <num iterations>");
				return false;
			}
		} else {
			System.out.println("Executing Linear Regression example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  We provide a data generator to create synthetic input files for this program.");
			System.out.println("  Usage: LinearRegression <data path> <result path> <num iterations>");
		}
		return true;
	}
}


