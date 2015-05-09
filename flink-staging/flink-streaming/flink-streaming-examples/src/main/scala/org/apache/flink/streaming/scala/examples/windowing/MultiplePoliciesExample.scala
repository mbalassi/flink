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
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.helper.Count
import org.apache.flink.util.Collector

/**
 * This example uses count based tumbling windowing with multiple eviction
 * policies at the same time.
 */
object MultiplePoliciesExample {
/* 
  //TODO: fix bugs

  def main(args: Array[String]) {

    if(!parseParameters(args)){
      return ;
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
  
  val  dstream = env.fromCollection(generateBasicSource())
                  .groupBy(x => x)
                  .window(Count.of(2))
                  .every(Count.of(2))
                  .reduceGroup(concatString _)
    
  // emit result
  if (fileOutput) {
    dstream.writeAsText(outputPath, 1)
  }
  else {
    dstream.print
  }

  // execute the program 
  env.execute("Multiple Policies Example")
  }
*/

  private def concatString (input: Iterable[String], out: Collector[String])  = {
    out.collect(input.mkString("|","|","|"))
  }

  private def generateBasicSource () : Stream[String] ={
    def emitStream(): Stream[String]  ={
      "streaming" #:: "flink" #:: emitStream()
    }
    emitStream()
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
        System.err.println("Usage: MultiplePoliciesExample <result path>")
        false
      }
    } else {
      System.out.println("Executing MultiplePoliciesExample with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: MultiplePoliciesExample <result path>")
    }
    true
  }

  private var fileOutput : Boolean = false
  private var outputPath : String =  null
}
