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

package org.apache.flink.stream.ml.regression

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{ParameterMap, WeightVector}
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.pipeline.PredictOperation
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.stream.ml.pipeline.StreamPredictor

/** Streaming predictor for the [[MultipleLinearRegression]] batch learner.
  *
  * @param mlr base batch learner
  */
class StreamMultipleLinearRegression(val mlr : MultipleLinearRegression) extends
  StreamPredictor[StreamMultipleLinearRegression, MultipleLinearRegression](mlr){
}

object StreamMultipleLinearRegression {

  // ========================================== Factory methods ====================================
  def apply(svm: MultipleLinearRegression): StreamMultipleLinearRegression = {
    new StreamMultipleLinearRegression(svm)
  }

  // ========================================== Operations =========================================


  /** Provides the operation that makes the predictions for individual examples.
    *
    * @tparam T
    * @return A PredictOperation, through which it is possible to predict a value, given a
    *         feature vector
    */
  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[StreamMultipleLinearRegression, WeightVector, T, Double]() {

      val batchBase = MultipleLinearRegression.predictVectors[T]

      override def getModel(self: StreamMultipleLinearRegression, predictParameters: ParameterMap)
      : DataSet[WeightVector] = {
        batchBase.getModel(self.mlr, predictParameters)
      }
      override def predict(value: T, model: WeightVector): Double = {
        batchBase.predict(value, model)
      }
    }
  }
}
