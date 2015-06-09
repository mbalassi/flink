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

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.windowing.Delta
import org.apache.flink.streaming.api.windowing.helper.Count
import org.apache.flink.streaming.api.scala._

/**
 * This example gives an impression about how to use delta policies. It also
 * shows how extractors can be used.
 */
object DeltaExtractExample {
  
  case class Counting (counter : Double, counter2: Double, note: String)
  
  def main(args: Array[String]) {

    // parse parameter
    if(!parseParameters(args)) {
      return;
    }

    //init stream environment 
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // process DataStream
    val dstream = env.fromCollection(countingStream)
                  .window(Delta.of(1.2, euclideanDistance,Counting(0d, 0d, "foo")))
                  .every(Count.of(2))
                  .reduce((Counting1, Counting2)=> Counting(Counting1.counter, Counting2.counter2, 
                                                    Counting1.note.concat("|").concat(Counting2.note)))
    // emit output
    if(fileOutput) {
      dstream.writeAsText(outputPath, 1)
    }else{
      dstream.print
    }
    
    env.execute("Delta Extract Example")

  }
  
  
  // util func
  private def countingStream(): Stream[Counting] = {
    def mapper (x: Int): Counting = {
      Counting(x%10000, x%10000+1, "V" + (x%10000).toString)
    }
    Stream.from(0,1).map(mapper)
  }
  
  private def euclideanDistance(point1 : Counting, point2: Counting): Double={
    val sum = Math.pow(point1.counter - point2.counter, 2)
                + Math.pow(point1.counter2 - point2.counter2, 2)
    Math.sqrt(sum)
  }
  
  private def parseParameters (args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        outputPath = args(0)
        System.out.println(outputPath)

      }else{
        System.err.println("Usage: DeltaExtractExample <result path>")
        return false
      }
    } else {
      System.out.println("Executing DeltaExtractExample with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: DeltaExtractExample <result path>")
    }
    true
  }
  
  private var fileOutput : Boolean = false
  private var outputPath : String =  null
  
}
