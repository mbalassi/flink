package org.apache.flink.streaming.examples.flink

import org.apache.flink.examples.java.wordcount.util.WordCountData
import org.apache.flink.streaming.api.scala._

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}



object CreateStream {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*val textFromFile = getTextDataStream(env)

    val cars = textFromFile.map(_.toLowerCase)
    cars print()
    env.execute()*/
    
    
    val textFromSocket = getSocketTextStream(env)
    textFromSocket.map(_.toLowerCase.length) print()
    env.execute()


  }

  // file 
  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }

  // host
  
  def getSocketTextStream(env : StreamExecutionEnvironment) : DataStream[String] = {
    env.socketTextStream(hostName, port)
  }

  // subselect

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"
  private val hostName :String= "localhost"
  private val port :Int= 2015
}



// csv 
//http://super-csv.github.io/super-csv/examples_reading_variable_cols.html


