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

package org.apache.flink.streaming.scala.examples.ml

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.helper.Time
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
 * Skeleton for incremental machine learning algorithm consisting of a
 * pre-computed model, which gets updated for the new inputs and new input data
 * for which the job provides predictions.
 *
 * <p>
 * This may serve as a base of a number of algorithms, e.g. updating an
 * incremental Alternating Least Squares model while also providing the
 * predictions.
 * </p>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Connected streams
 * <li>CoFunctions
 * <li>Tuple data types
 * </ul>
 */
object IncrementalLearningSkeleton {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************
  
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val model = env.fromCollection(genTrainingDataSrc)
                      .window(Time.of(5000, TimeUnit.MILLISECONDS))
                      .reduceGroup(PartialModelBuilder _ )
    
    val prediction = env.fromCollection(genNewDataSrc())
                      .connect(model)
                      .map(map1 , map2)

    // emit result
    if (fileOutput) {
      prediction.writeAsText(outputPath, 1)
    }
    else {
      prediction.print
    }

    // execute program
    env.execute("Streaming Incremental Learning")
  }
  

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  /**
   * Feeds new training data for the partial model builder. By default it is
   * implemented as constantly emitting an infinite Integer stream of  1.
   */
  def genTrainingDataSrc() : Stream[Int] = {
    Stream.from(1,1).map(_ => {Thread.sleep(TRAINING_DATA_SLEEP_TIME); 1})
  }

  /**
   * Feeds new data for prediction. By default it is implemented as constantly
   * emitting the Integer 1 in a loop.
   */
  def genNewDataSrc(): Stream[Int]  = {
    Stream.from(1,1).map(_ => {Thread.sleep(NEW_DATA_SLEEEP_TIME); 1})
  }


  def map1 (tData : Int): Int ={
    def predict(inTuple: Int): Int = {
      return 0
    }
    // return predict
    predict(tData)
  }


  def map2(tData: Array[Double]): Int ={
    def getBatchModel(): Array[Double]={
       Array(1.0)
    }
    // update model
    val partialModel = tData
    val batchModel = getBatchModel()
    return 1
  }
  

  /**
   * Builds up-to-date partial models on new training data.
   */
  def PartialModelBuilder (ts: Iterable[Int], out:Collector[Array[Double]]) ={
    out.collect(Array[Double](1.0))
  }
  

  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************
  
  def parseParameters(args: Array[String]) = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        outputPath = args(0)
      }
      else {
        System.err.println("Usage: IncrementalLearningSkeleton <result path>")
        false
      }
    }
    else {
      System.out.println("Executing IncrementalLearningSkeleton with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: IncrementalLearningSkeleton <result path>")
    }
    true
    
  }
  
  private var fileOutput : Boolean= false
  private var outputPath : String = null
  private val TRAINING_DATA_SLEEP_TIME = 1000
  private val NEW_DATA_SLEEEP_TIME = 1000
}
