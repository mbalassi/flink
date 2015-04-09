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
package org.apache.flink.streaming.sql.table

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Base class for all code generation classes. This provides the functionality for generating
  * code from an [[Expression]] tree. Derived classes must embed this in a lambda function
  * to form an executable code block.
  *
  * @param inputs List of input variable names with corresponding [[TypeInformation]].
  * @param nullCheck Whether the generated code should include checks for NULL values.
  * @param cl The ClassLoader that is used to create the Scala reflection ToolBox
  * @tparam R The type of the generated code block. In most cases a lambda function such
  *           as "(IN1, IN2) => OUT".
  */
abstract class ExpressionCodeGenerator[R](
                                           inputs: Seq[(String, CompositeType[_])],
                                           val nullCheck: Boolean = false,
                                           cl: ClassLoader) {
  object ReflectionLock
  protected val log = LoggerFactory.getLogger(classOf[ExpressionCodeGenerator[_]])
  
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}

  if (cl == null) {
    throw new IllegalArgumentException("ClassLoader must not be null.")
  }

  import scala.tools.reflect.ToolBox

  protected val (mirror, toolBox) = ReflectionLock.synchronized {
    val mirror = runtimeMirror(cl)
    (mirror, mirror.mkToolBox())
  }

  // This is to be implemented by subclasses, we have it like this
  // so that we only call it from here with the Scala Reflection Lock.
  protected def generateInternal(): R

  final def generate(): R = {
    ReflectionLock.synchronized {
      generateInternal()
    }
  }
  
  
  
  
  
  
  





  // We don't have c.freshName
  // According to http://docs.scala-lang.org/overviews/quasiquotes/hygiene.html
  // it's coming for 2.11. We can't wait that long...
  def freshTermName(name: String): TermName = {
    newTermName(s"$name$$${freshNameCounter.getAndIncrement}")
  }

  val freshNameCounter = new AtomicInteger

  case class GeneratedExpression(code: Seq[Tree], resultTerm: TermName, nullTerm: TermName)



  sealed abstract class FieldAccessor

  case class ObjectFieldAccessor(fieldName: String) extends FieldAccessor

  case class ObjectMethodAccessor(methodName: String) extends FieldAccessor

  case class ProductAccessor(i: Int) extends FieldAccessor

  def fieldAccessorFor(elementType: CompositeType[_], fieldName: String): FieldAccessor = {
    elementType match {
      case ri: RowTypeInfo =>
        ProductAccessor(elementType.getFieldIndex(fieldName))

      case cc: CaseClassTypeInfo[_] =>
        ObjectFieldAccessor(fieldName)

      case javaTup: TupleTypeInfo[_] =>
        ObjectFieldAccessor(fieldName)

      case pj: PojoTypeInfo[_] =>
        ObjectFieldAccessor(fieldName)

      case proxy: RenamingProxyTypeInfo[_] =>
        val underlying = proxy.getUnderlyingType
        val fieldIndex = proxy.getFieldIndex(fieldName)
        fieldAccessorFor(underlying, underlying.getFieldNames()(fieldIndex))
    }
  }

  protected def defaultPrimitive(tpe: TypeInformation[_]) = tpe match {
    case BasicTypeInfo.INT_TYPE_INFO => ru.Literal(Constant(-1))
    case BasicTypeInfo.LONG_TYPE_INFO => ru.Literal(Constant(1L))
    case BasicTypeInfo.SHORT_TYPE_INFO => ru.Literal(Constant(-1.toShort))
    case BasicTypeInfo.BYTE_TYPE_INFO => ru.Literal(Constant(-1.toByte))
    case BasicTypeInfo.FLOAT_TYPE_INFO => ru.Literal(Constant(-1.0.toFloat))
    case BasicTypeInfo.DOUBLE_TYPE_INFO => ru.Literal(Constant(-1.toDouble))
    case BasicTypeInfo.BOOLEAN_TYPE_INFO => ru.Literal(Constant(false))
    case BasicTypeInfo.STRING_TYPE_INFO => ru.Literal(Constant("<empty>"))
    case BasicTypeInfo.CHAR_TYPE_INFO => ru.Literal(Constant('\0'))
    case _ => ru.Literal(Constant(null))
  }


  protected def typeTermForTypeInfo(typeInfo: TypeInformation[_]): Tree = {
    val tpe = typeForTypeInfo(typeInfo)
    tq"$tpe"
  }

  // We need two separate methods here because typeForTypeInfo is recursive when generating
  // the type for a type with generic parameters.
  protected def typeForTypeInfo(tpe: TypeInformation[_]): Type = tpe match {

    // From PrimitiveArrayTypeInfo we would get class "int[]", scala reflections
    // does not seem to like this, so we manually give the correct type here.
    case PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Int]]
    case PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Long]]
    case PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Short]]
    case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Byte]]
    case PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Float]]
    case PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Double]]
    case PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Boolean]]
    case PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Char]]

    case _ =>
      val clazz = mirror.staticClass(tpe.getTypeClass.getCanonicalName)

      clazz.selfType.erasure match {
        case ExistentialType(_, underlying) => underlying

        case tpe@TypeRef(prefix, sym, Nil) =>
          // Non-generic type, just return the type
          tpe

        case TypeRef(prefix, sym, emptyParams) =>
          val genericTypeInfos = tpe.getGenericParameters.asScala
          if (emptyParams.length != genericTypeInfos.length) {
            throw new RuntimeException("Number of type parameters does not match.")
          }
          val typeParams = genericTypeInfos.map(typeForTypeInfo)
          // TODO: remove, added only for migration of the line below, as suggested by the compiler
          import compat._
          TypeRef(prefix, sym, typeParams.toList)
      }

  }

}
