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

package org.apache.flink.stream.ml.pipeline

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.io.BinaryInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.memory.DataInputView
import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.ml.pipeline.{Estimator, PredictOperation}
import org.apache.flink.stream.ml.common.StreamMLTools
import org.apache.flink.streaming.api.scala._

/** Predictor abstract base class for Flink's StreamML pipeline operators.
  *
  * A [[StreamPredictor]] calculates predictions for a stream of testing data based on the model it
  * learned during the fit operation (training phase). In order to do that, the implementing class
  * has to provide:
  *   - an [[Estimator]] as a field and thus pull a [[org.apache.flink.ml.pipeline.FitOperation]],
  *   - a [[PredictDataStreamOperation]] or a [[PredictOperation]] implementation for the correct
  *     types.
  *
  * The implicit values should be put into the scope of the companion object of the implementing
  * class to make them retrievable for the Scala compiler.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self Type of the implementing class
  */
abstract class StreamPredictor[Self, BatchBase <: Estimator[BatchBase]](batchBase : BatchBase) {

  that: Self =>

  /**
    * Exposes the [[WithParameters]] from the underlying batch [[Estimator]].
    */
  val parameters = batchBase.parameters

  /** Predicts for testing data stream according the learned model. The implementing class has to
    * provide a corresponding implementation of [[PredictDataStreamOperation]] which contains the
    * prediction logic.
    *
    * @param testing Testing data which shall be predicted
    * @param predictParameters Additional parameters for the prediction
    * @param predictor [[PredictDataStreamOperation]] which encapsulates the prediction logic
    * @tparam Testing Type of the testing data
    * @tparam Prediction Type of the prediction data
    * @return stream of predicted values
    */
  def predictStream[Testing, Prediction](
      testing: DataStream[Testing],
      predictParameters: ParameterMap = ParameterMap.Empty)(implicit
      predictor: PredictDataStreamOperation[Self, Testing, Prediction])
    : DataStream[Prediction] = {
    StreamMLTools.registerStreamMLTypes(testing.executionEnvironment)
    predictor.predictDataStream(this, predictParameters, testing)
  }
}

object StreamPredictor {

  /** Default [[PredictDataStreamOperation]] which takes a [[PredictOperation]] to calculate
    * a tuple of testing element and its prediction value.
    *
    * Note: We have to put the TypeInformation implicit values for Testing, Model and
    * PredictionValue after the PredictOperation implicit parameter. Otherwise, if it's defined as
    * a context bound, then the Scala compiler does not find the implicit [[PredictOperation]]
    * value.
    *
    * @param predictOperation
    * @param testingTypeInformation
    * @param predictionValueTypeInformation
    * @tparam Instance
    * @tparam Model
    * @tparam Testing
    * @tparam PredictionValue
    * @return
    */
  implicit def defaultPredictDataStreamOperation[
  Instance <: StreamPredictor[Instance, _],
  Testing,
  Model,
  PredictionValue](
                    implicit predictOperation:
                      PredictOperation[Instance, Model, Testing, PredictionValue],
                    testingTypeInformation: TypeInformation[Testing],
                    modelTypeInformation: TypeInformation[Model],
                    predictionValueTypeInformation: TypeInformation[PredictionValue])
  : PredictDataStreamOperation[Instance, Testing, (Testing, PredictionValue)] = {
    new PredictDataStreamOperation[Instance, Testing, (Testing, PredictionValue)] {
      override def predictDataStream(
                                   instance: Instance,
                                   predictParameters: ParameterMap,
                                   input: DataStream[Testing])
      : DataStream[(Testing, PredictionValue)] = {
        val resultingParameters = instance.parameters ++ predictParameters

        val model = predictOperation.getModel(instance, resultingParameters)
        StreamMLTools.registerFlinkMLModelTypes(model.getExecutionEnvironment)

        // TODO use dfs location read from Configuration
        val persistLocation = "/tmp/" + "flink-stream-ml-model-" +
          StreamMLTools.generatePersistFolderSuffix()

        val outputFormat = new TypeSerializerOutputFormat[Model]
        outputFormat.setInputType(createTypeInformation[Model],
          model.getExecutionEnvironment.getConfig)

        model.write(outputFormat, persistLocation, writeMode = WriteMode.OVERWRITE)

        model.getExecutionEnvironment.execute("Persist StreamML model to disk")

        implicit val resultTypeInformation = createTypeInformation[(Testing, PredictionValue)]

        input.map(new PredictorMap[Instance, Model, Testing, PredictionValue](
          predictOperation, persistLocation)
        )
      }
    }
  }

  class PredictorMap[Instance, Model : TypeInformation, Testing, PredictionValue](
    predictOperation: PredictOperation[Instance, Model, Testing, PredictionValue],
    persistLocation : String)
    extends RichMapFunction[Testing, (Testing, PredictionValue)] {
    var model : Model = _

    //TODO consider graceful handling of persist file already being cleaned up by parallel subtask
    override def open(parameters: Configuration): Unit = {
      val config = getRuntimeContext.getExecutionConfig
      val modelSerializer = createTypeInformation[Model].createSerializer(config)

      //do not use TypeSerializerInputFormat, that creates a new Config
      val inputFormat =
        new BinaryInputFormat[Model]{
          override protected def deserialize(reuse: Model, dataInput: DataInputView): Model = {
            modelSerializer.deserialize(reuse, dataInput)
          }
        }
      inputFormat.setFilePath(persistLocation)
      val splits = inputFormat.createInputSplits(1)

      for (split <- splits){
        inputFormat.open(split)
        val reuse = modelSerializer.createInstance()
        if (!inputFormat.reachedEnd()){
          model = inputFormat.nextRecord(reuse)
        } else {
          throw new RuntimeException("Persist path did not contain valid model.")
        }
      }
    }

    override def map(value: Testing): (Testing, PredictionValue) = {
      (value, predictOperation.predict(value, model))
    }

    // clean up temporary model persist paths from the first subtask
    override def close(): Unit = {
      if (getRuntimeContext.getIndexOfThisSubtask == 0){
        StreamMLTools.cleanUpLocation("/tmp/flink-binary-model")
      }
    }
  }
}

/** Type class for the predict operation of [[StreamPredictor]]. This predict operation works on
  * DataStreams.
  *
  * [[StreamPredictor]]s have to implement this trait. The implementation has to be made available
  * as an implicit value or function in the scope of their companion objects.
  *
  * The first type parameter is the type of the implementing [[StreamPredictor]] class so that the
  * Scala compiler includes the companion object of this class in the search scope for the implicit
  * values.
  *
  * @tparam Self Type of [[StreamPredictor]] implementing class
  * @tparam Testing Type of testing data
  * @tparam Prediction Type of predicted data
  */
trait PredictDataStreamOperation[Self, Testing, Prediction] extends Serializable{

  /** Calculates the predictions for all elements in the [[DataStream]] input
    *
    * @param instance The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @param input The DataStream containing the unlabeled examples
    * @return
    */
  def predictDataStream(
      instance: Self,
      predictParameters: ParameterMap,
      input: DataStream[Testing])
    : DataStream[Prediction]
}


