package org.apache.flink.streaming.examples.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * Created by kidio on 18/05/15.
 */
object Merge {
  def main(args: Array[String]) {
    case class num(i: Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text1 = getTextDataStream(env)
    var stream1 = text1.map(s => new Tuple1(0))
    val text2 = getTextDataStream(env)
    val stream2 = text2.map(s => new Tuple1(1))

    stream1 = stream1.merge(stream2)

    stream1 print()

    env.execute()
  }

  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }
  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"
}
