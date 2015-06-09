package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.fsql.{Row, SQLContext}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.fsql.macros.ArrMappable


object Main {
//  case class SimpleCarEvent( speed: Int, time: Long) extends Serializable
  case class Car (plate: Int, price: Int) extends Serializable

  def main(args: Array[String]) {
    val sqlContext = new SQLContext()

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
    
    
    /*val dStream = sqlContext.sql("select plate, price + 1 from CarStream")
    
    
    println(dStream.asInstanceOf[DataStream[_]].getType())

    val fStream = sqlContext.sql("select plate from CarStream where plate < price and plate > price")


    fStream.asInstanceOf[DataStream[_]] print*/

    val stream5 = sqlContext.sql("select plate, price from CarStream")
    println(stream5.asInstanceOf[DataStream[_]].getType().getGenericParameters.toArray.toList.map(x => x.toString))
    println(    stream5.asInstanceOf[DataStream[_]].getType().isBasicType)
    

    val stream6 = sqlContext.sql("select * from (select plate,price from CarStream) as p")
    /*
    
    
      println(stream6.asInstanceOf[DataStream[_]].getType())
      stream6.asInstanceOf[DataStream[_]] print
    */
    println("Stream6: "+ stream6)
    
    
    
    
    
    
    //println(sqlContext.streamsMap)
    
    
    
    
    
    
    env.execute()
    
  }

  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }
  
  def getCarStream (env : StreamExecutionEnvironment) : DataStream [Car] = {
    Seq(Car(8888,2)).toStream
  }

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"

}