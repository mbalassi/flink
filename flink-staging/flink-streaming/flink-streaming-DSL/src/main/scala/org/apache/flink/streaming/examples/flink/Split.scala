package org.apache.flink.streaming.examples.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * Created by kidio on 18/05/15.
 */
object Split {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text1 = getTextDataStream(env)
    var stream1 = text1.map(s => 0)
    val text2 = getTextDataStream(env)
    val stream2 = text2.map(s => 1)

    stream1 = stream1.merge(stream2)

    val splitStream = stream1.split(
      (num: Int) => num match {
        case 1 => List("yes")
        case 0 => List("no")
      }
    )
    
    val output1 = splitStream.select("yes")
    output1 print()

    env.execute()
  }

  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }
  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"

}

