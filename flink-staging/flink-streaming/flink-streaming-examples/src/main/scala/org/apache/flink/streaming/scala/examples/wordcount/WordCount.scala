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
package org.apache.flink.streaming.scala.examples.wordcount

import org.apache.flink.examples.java.wordcount.util.WordCountData
import org.apache.flink.streaming.api.scala._
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

object WordCount {
  def main(args: Array[String]) {

    // unsuccessful parsing
    if(!parseParameters(args))
       return

    // init the env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // generate a sample data stream
    val text = getTextDataStream(env)

    // map and reduce
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    if (fileOutput) {
      counts.writeAsCsv(outputPath, 1)
    } else{
      counts.print()
    }

    env.execute("Scala WordCount example with Streaming")

  }

  private def parseParameters(args: Array[String]) : Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        textPath = args(0)
        outputPath = args(1)
      } else {
        System.err.println("Usage: WordCount <text path> <result path>")
        false
      }
    }else {
      System.out.println("Executing WordCount example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: WordCount <text path> <result path>")
    }
    true
  }

  private def getTextDataStream (env : StreamExecutionEnvironment) : DataStream[String] = {
    if (fileOutput) {
      env.readTextFile(textPath)
    } else{
      env.fromCollection(WordCountData.WORDS)
    }
  }

  private var fileOutput : Boolean = false
  private var textPath: String = null
  private var outputPath : String = null
}
