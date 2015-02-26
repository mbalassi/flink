package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random
import org.apache.flink.streaming.api.scala._
import scala.language.postfixOps
import org.apache.flink.api.scala.expressions._


object TestLINQ {
  case class Name(id: Long, name :String)

  def main (args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("ABCD", 2), ("ABCD", 1)).as('a, 'b)
      .select('a.substring('b))
    
    ds print
    
    env.execute()
  }
}
