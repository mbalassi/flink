package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.fsql.SQLContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.fsql.macros.ArrMappable


object Main {
//  case class SimpleCarEvent( speed: Int, time: Long) extends Serializable
  case class Car (plate: Int, price: Int) extends Serializable

  def main(args: Array[String]) {
    val sqlContext = new SQLContext()
    println(sqlContext.sql("create schema carSchema2 (speed int)"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    
    val cars = getTextDataStream(env)

      /**
       * * create stream
       */
    // create schema
     println(sqlContext.sql("create schema carSchema (plate Int)"))
    // create real DataStream
    val simpleCars = getCarStream(env)


    val rowCar = simpleCars
//
//    // register a new stream from source stream
    val newStream = sqlContext.sql("create stream CarStream (plate Int, price Int) source stream ('rowCar')")
    //println(sqlContext.sql("create schema carSchema3 (pedal String)"))

    println(newStream)
    //val newStream2 = sqlContext.sql("create stream CarStream (plate Int) source stream ('rowCar')")
    
  // newStream2.asInstanceOf[DataStream[_]] print

    /**
     *  
     *  SELECT
     * */
    

    
    val dStream = sqlContext.sql("select plate from CarStream")
    println(dStream)

    env.execute()

  }
  

  /*     implicit def toRow[M](stream: DataStream[M]): DataStream[Row] = {

      import org.apache.flink.streaming.experimental.ArrMappable
      def mapify[T: ArrMappable](t: T) = implicitly[ArrMappable[T]].toMap(t)

      stream.map((x:M) => Row(mapify(x)))

    }*/
  

  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }
  
  def getCarStream (env : StreamExecutionEnvironment) : DataStream [Car] = {
    Seq(Car(8888,2)).toStream
  }

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"


}