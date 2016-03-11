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

package org.apache.flink.stream.ml.recommendation

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.stream.ml.common.StreamMLTools
import org.apache.flink.stream.ml.pipeline.{PredictDataStreamOperation, StreamPredictor}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.language.implicitConversions

/** Streaming predictor for the [[ALS]] batch learner.
  *
  * @param als base batch learner
  */
class StreamALS(val als: ALS) extends StreamPredictor[StreamALS, ALS](als){
}

object StreamALS {

  // ========================================== Factory methods ====================================
  def apply(svm: ALS): StreamALS = {
    new StreamALS(svm)
  }

  // ========================================== Operations =========================================

  /** Predict operation which calculates the matrix entry for the given indices  */
  implicit val predictRatingStream =
    new PredictDataStreamOperation[StreamALS, (Int, Int), (Int, Int, Double)] {
      override def predictDataStream(
                                      instance: StreamALS,
                                      predictParameters: ParameterMap,
                                      input: DataStream[(Int, Int)])
      : DataStream[(Int, Int, Double)] = {

      instance.als.factorsOption match {
        case Some((userFactors, itemFactors)) => {

          // Persist batch model to disk
          // TODO use dfs location read from Configuration
          val userPersistLocation = "/tmp/" + "flink-stream-ml-als-q-" +
            StreamMLTools.generatePersistFolderSuffix()
          val itemPersistLocation = "/tmp/" + "flink-stream-ml-als-p-" +
            StreamMLTools.generatePersistFolderSuffix()

          val userOutputFormat = new TypeSerializerOutputFormat[ALS.Factors]
          userOutputFormat.setInputType(createTypeInformation[ALS.Factors],
            userFactors.getExecutionEnvironment.getConfig)

          val itemOutputFormat = new TypeSerializerOutputFormat[ALS.Factors]
          itemOutputFormat.setInputType(createTypeInformation[ALS.Factors],
            userFactors.getExecutionEnvironment.getConfig)

          userFactors.write(userOutputFormat, userPersistLocation, writeMode = WriteMode.OVERWRITE)
          itemFactors.write(itemOutputFormat, itemPersistLocation, writeMode = WriteMode.OVERWRITE)

          userFactors.getExecutionEnvironment.execute()

          // Map the input stream with the model read from disk in the open function
          input.map(new MapWithModel(userPersistLocation, itemPersistLocation))
        }

        case None => throw new RuntimeException("The ALS model has not been fitted to data. " +
          "Prior to predicting values, it has to be trained on data.")
      }
    }
  }

  class MapWithModel(userPersistLocation : String, itemPersistLocation : String)
    extends RichMapFunction[(Int, Int), (Int, Int, Double)] {

    // Currently we assume that the model fits in memory and is static
    // for further steps a (spillable) state is needed
    val q : mutable.Map[Int, Array[Double]] = new mutable.HashMap()
    val p : mutable.Map[Int, Array[Double]] = new mutable.HashMap()
    var factors : Int = _

    //TODO consider graceful handling of persist file already being cleaned up by parallel subtask
    override def open(parameters: Configuration): Unit = {
      val inputFormat =
        new TypeSerializerInputFormat[ALS.Factors](createTypeInformation[ALS.Factors])
      readFactorFile(inputFormat, userPersistLocation, q)
      readFactorFile(inputFormat, itemPersistLocation, p)
      factors = q.head._2.length
    }

    override def map(value: (Int, Int)): (Int, Int, Double) = {
      (value._1, value._2, blas.ddot(factors, q(value._1), 1, p(value._2), 1))
    }

    // clean up temporary model persist paths from the first subtask
    override def close(): Unit = {
      if (getRuntimeContext.getIndexOfThisSubtask == 0){
        StreamMLTools.cleanUpLocation(userPersistLocation)
        StreamMLTools.cleanUpLocation(itemPersistLocation)
      }
    }

    def readFactorFile(inputFormat: FileInputFormat[ALS.Factors], file : String,
                       factors : mutable.Map[Int, Array[Double]]) = {
      inputFormat.setFilePath(file)
      val splits = inputFormat.createInputSplits(1)
      for (split <- splits){
        inputFormat.open(split)
        var reuse = ALS.Factors(0, Array())
        while (!inputFormat.reachedEnd()){
          reuse = inputFormat.nextRecord(reuse)
          factors += (reuse.id -> reuse.factors)
        }
      }
    }
  }

}
