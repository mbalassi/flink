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

package org.apache.flink.stream.ml.examples

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.stream.ml.regression._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MLRExample {
  def main(args: Array[String]) {

    val strEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = strEnv.fromElements(DenseVector(Array(0,0)))

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.fromElements(LabeledVector(0.0, DenseVector(Array(0,0))),
      LabeledVector(1.0, DenseVector(Array(1,1))))

    val model = MultipleLinearRegression()

    model.fit(input)

    val result = model.predictStream(stream)

    result.print()

    strEnv.execute("Streaming Multiple Linear Regression")
  }
}
