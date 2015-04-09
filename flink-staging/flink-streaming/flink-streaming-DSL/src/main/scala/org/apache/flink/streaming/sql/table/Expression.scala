package org.apache.flink.streaming.sql.table

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

abstract class Expression extends Product {
  def children: Seq[Expression]

  def name: String = Expression.freshName("expression")

  def typeInfo: TypeInformation[_]
}

object Expression {
  def freshName(prefix: String): String = {
    s"$prefix-${freshNameCounter.getAndIncrement}"
  }

  val freshNameCounter = new AtomicInteger
}

object Literal {
  def apply(l: Any): Literal = l match {
    case i:Int => Literal(i, BasicTypeInfo.INT_TYPE_INFO)
    case l:Long => Literal(l, BasicTypeInfo.LONG_TYPE_INFO)
    case d: Double => Literal(d, BasicTypeInfo.DOUBLE_TYPE_INFO)
    case f: Float => Literal(f, BasicTypeInfo.FLOAT_TYPE_INFO)
    case str: String => Literal(str, BasicTypeInfo.STRING_TYPE_INFO)
    case bool: Boolean => Literal(bool, BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }
}

case class Literal(value: Any, tpe: TypeInformation[_]) extends Expression {
  def children = Nil
  def expr = this
  def typeInfo = tpe

  override def toString = s"$value"
}
