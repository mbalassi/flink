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
package org.apache.flink.streaming.scala.examples.Iteration

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * Example illustrating iterations in Flink streaming.
 *
 * <p>
 * The program sums up random numbers and counts additions it performs to reach
 * a specific threshold in an iterative streaming fashion.
 * </p>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>streaming iterations,
 * <li>buffer timeout to enhance latency,
 * <li>directed outputs.
 * </ul>
 */

object IterateExample {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************
  /* //TODO: fix bugs
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // StreamExecution
    val  env = StreamExecutionEnvironment.getExecutionEnvironment
                .setBufferTimeout(1)

    val it = env.fromCollection(generateStream).shuffle
    val output = it.iterate(stepFunction, 5000).map {t:(Double, Int) => t._2 }

    // emit result
    if (fileOutput) {
      output.writeAsText(outputPath, 1)
    }
    else {
      output.print
    }

    // execute the program
    env.execute("Streaming Iteration Example")
  }
  */
  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  /**
  *  generate a source stream of (0.0, 0)
  */
  def generateStream: Stream[(Double, Int)] ={
    Stream.from(0,1).map(x => (0.0,0))
  }

  /**
   * Iteration step function which takes a data stream of (Double , Integer) and
   * produces 2 split streams of (Double + random, Integer + 1).
   * One of them is output, the second will be fed back into the next iteration
   */
  /* //TODO: fix bugs
  def stepFunction (input: DataStream[(Double, Int)]) : (DataStream[(Double, Int)], DataStream[(Double, Int)]) = {
      def  Myselector: ((Double,Int))=> String  ={
          case (x: Double,_) if x > 2 =>"output"
          case _ => "iterate"
      }

      val step = input.map { t:(Double, Int) => ( t._1+ Random.nextDouble(), t._2 +1)}
                  .shuffle.split ( Myselector)
    ( step.select("iterate"), step.select("output"))

  }
 */
  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        outputPath = args(0)
      }
      else {
        System.err.println("Usage: IterateExample <result path>")
        false
      }
    }
    else {
      System.out.println("Executing IterateExample with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: IterateExample <result path>")
    }
    true
  }

  private var fileOutput : Boolean = false
  private var outputPath: String = null

}
