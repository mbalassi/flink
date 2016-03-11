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

package org.apache.flink.stream.ml.common

import java.net.URI

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.{Path, FileSystem}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.StringUtils

import scala.util.Random

/**
  * Contains a set of convenience functions for Flink's stream machine learning library.
  */
object StreamMLTools {

  /** Registers the different StreamML related types for Kryo serialization.
    *
    * @param env
    */
  def registerStreamMLTypes(env: StreamExecutionEnvironment): Unit = {

    // Vector types
    env.registerType(classOf[org.apache.flink.ml.math.DenseVector])
    env.registerType(classOf[org.apache.flink.ml.math.SparseVector])

    // Matrix types
    env.registerType(classOf[org.apache.flink.ml.math.DenseMatrix])
    env.registerType(classOf[org.apache.flink.ml.math.SparseMatrix])

    // Breeze Vector types
    env.registerType(classOf[breeze.linalg.DenseVector[_]])
    env.registerType(classOf[breeze.linalg.SparseVector[_]])

    // Breeze specialized types
    env.registerType(breeze.linalg.DenseVector.zeros[Double](0).getClass)
    env.registerType(breeze.linalg.SparseVector.zeros[Double](0).getClass)

    // Breeze Matrix types
    env.registerType(classOf[breeze.linalg.DenseMatrix[Double]])
    env.registerType(classOf[breeze.linalg.CSCMatrix[Double]])

    // Breeze specialized types
    env.registerType(breeze.linalg.DenseMatrix.zeros[Double](0, 0).getClass)
    env.registerType(breeze.linalg.CSCMatrix.zeros[Double](0, 0).getClass)

    // Model types
    env.registerType(classOf[org.apache.flink.ml.common.WeightVector])
    env.registerType(classOf[org.apache.flink.ml.common.LabeledVector])
  }

  /** Registers the different FlinkML related model types for Kryo serialization.
    *
    * @param env
    */
  def registerFlinkMLModelTypes(env: ExecutionEnvironment): Unit = {
    // Model types
    env.registerType(classOf[org.apache.flink.ml.common.WeightVector])
    env.registerType(classOf[org.apache.flink.ml.common.LabeledVector])
  }

  /** Generates a 32 byte long random string to act as a suffix for temporary persist paths.
    */
  def generatePersistFolderSuffix() : String = {
    val rndBytes = Array.ofDim[Byte](32)
    Random.nextBytes(rndBytes)
    StringUtils.byteToHexString(rndBytes)
  }

  /** Cleans up temporary persist paths recursively.
    */
  def cleanUpLocation(location : String) = {
    val fileSystem = FileSystem.get(new URI(location))
    val path = new Path(location)
    if (fileSystem.exists(path)){
      fileSystem.delete(path, true)
    }
  }
}
