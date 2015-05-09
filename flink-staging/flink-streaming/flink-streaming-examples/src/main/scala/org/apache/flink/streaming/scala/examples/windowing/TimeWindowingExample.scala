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

package org.apache.flink.streaming.scala.examples.windowing

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.windowing.Time
import org.apache.flink.streaming.api.windowing.helper.Count
import org.apache.flink.streaming.api.scala._

/**
 * This example shows the functionality of time based windows. It utilizes the
 * {@link ActiveTriggerPolicy} implementation in the
 * {@link ActiveTimeTriggerPolicy}.
 */
object TimeWindowingExample {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val dstream = env.fromCollection(generateCountingSourceWithSleep)
                      .window(Count.of(100))
                      .every(Time.of(1000, TimeUnit.MILLISECONDS))
                      .groupBy(x => {if (x < 2)  0; else  1})
                      .reduce(_+_)
//                    .sum(0)
    // emit result
    if (fileOutput) {
      dstream.writeAsText(outputPath, 1)
    }
    else {
      dstream.print()
    }

    // execute the program
    
    env.execute("Time Windowing Example")
    
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************
  /**
   * This data source emit one element every 0.001 sec. The output is an
   * Integer counting the output elements. As soon as the counter reaches
   * 10000 it is reset to 0. On each reset the source waits 5 sec. before it
   * restarts to produce elements.
   */
  private def generateCountingSourceWithSleep (): Stream[Int]= {
    def mapper (x: Int): (Int) ={
      if (x%10000 ==0){
        System.out.println("Source pauses now!")
        Thread.sleep(5000)
        System.out.println("Source continues with emitting now!")
      }
      Thread.sleep(1)
      scala.util.Random.nextInt(9) +1
    }
    Stream.from(0,1).map(mapper)
  }

  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************
  private def parseParameters (args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        outputPath = args(0)
        System.out.println(outputPath)

      }else{
        System.err.println("Usage: TimeWindowingExample <result path>")
        return false
      }
    } else {
      System.out.println("Executing TimeWindowingExample with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: TimeWindowingExample <result path>")
    }
    true
  }

  private var fileOutput : Boolean = false
  private var outputPath : String =  null
}
