package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.fsql.SQLContext


object Main {
  def main(args: Array[String]) {
    val sqlContext = new SQLContext()
    println(sqlContext.sql("create schema myschema (speed int)"))
    println(sqlContext.sql("create schema myschema2 (time long) extends myschema"))

  }

}