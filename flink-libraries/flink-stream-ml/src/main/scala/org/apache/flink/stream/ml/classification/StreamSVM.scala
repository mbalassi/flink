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

package org.apache.flink.stream.ml.classification

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.ml.pipeline.PredictOperation
import org.apache.flink.stream.ml.pipeline.StreamPredictor

/** Streaming predictor for the [[SVM]] batch learner.
  *
  * @param svm base batch learner
  */
class StreamSVM(val svm : SVM) extends StreamPredictor[StreamSVM, SVM](svm){}

object StreamSVM{

  // ========================================== Factory methods ====================================
  def apply(svm: SVM): StreamSVM = {
    new StreamSVM(svm)
  }

  // ========================================== Operations =========================================

  /** Provides the operation that makes the predictions for individual examples.
    *
    * @tparam T
    * @return A PredictOperation, through which it is possible to predict a value, given a
    *         feature vector
    */
  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[StreamSVM, DenseVector, T, Double](){

      val batchBase = SVM.predictVectors[T]

      override def getModel(self: StreamSVM, predictParameters: ParameterMap):
        DataSet[DenseVector] = {
        batchBase.getModel(self.svm, predictParameters)
      }

      override def predict(value: T, model: DenseVector): Double = {
        batchBase.predict(value, model)
      }
    }
  }
}
