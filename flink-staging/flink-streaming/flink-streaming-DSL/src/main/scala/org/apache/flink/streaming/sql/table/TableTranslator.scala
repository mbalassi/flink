package org.apache.flink.streaming.sql.table

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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import java.lang.reflect.Modifier


import scala.language.reflectiveCalls

abstract class TableTranslator {

  type Representation[A] <: { def getType(): TypeInformation[A] }

  /**
   * Creates a [[Table]] from a DataSet or a DataStream (the underlying representation).
   */
  def createTable[A](
          repr: Representation[A],
          inputType: CompositeType[A],
          expressions: Array[Expression],
          resultFields: Seq[(String, TypeInformation[_])]): Table[this.type]


  def createTable[A](
                      repr: Representation[A],
                      fields: Array[Expression],
                      checkDeterministicFields: Boolean = true): Table[this.type] = {

    // shortcut for DataSet[Row] or DataStream[Row]
    repr.getType() match {
      case rowTypeInfo: RowTypeInfo =>
        val expressions = rowTypeInfo.getFieldNames map {
          name => (name, rowTypeInfo.getTypeAt(name))
        }
        new Table( Root(repr, expressions), this)

      case c: CompositeType[A] => // us ok

      case tpe => throw new ExpressionException("Only DataSets or DataStreams of composite type" +
        "can be transformed to a Table. These would be tuples, case classes and " +
        "POJOs. Type is: " + tpe)

    }

    val clazz = repr.getType().getTypeClass
    if (clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) {
      throw new ExpressionException("Cannot create Table from DataSet or DataStream of type " +
        clazz.getName + ". Only top-level classes or static members classes " +
        " are supported.")
    }

    val inputType = repr.getType().asInstanceOf[CompositeType[A]]

    val newFieldNames = Array("field1","field2","field3")


    val resultFields: Seq[(String, TypeInformation[_])] = newFieldNames.zipWithIndex map {
      case (name, index) => (name, inputType.getTypeAt(index))
    }
    createTable(repr, inputType, Array(Literal(10),Literal(11),Literal(12)), resultFields)
  }

}
