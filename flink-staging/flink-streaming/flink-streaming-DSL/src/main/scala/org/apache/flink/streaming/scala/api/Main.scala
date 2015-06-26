package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.fsql.{Row, SQLContext}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.fsql.macros.ArrMappable

object Main {
//  case class SimpleCarEvent( speed: Int, time: Long) extends Serializable
  case class Car (carID: Int, price: Int) extends Serializable

  def main(args: Array[String]) {
    val sqlContext = new SQLContext()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val cars = getTextDataStream(env)

      /**
       * * create stream
       */
    // create schema
     println(sqlContext.sql("create schema carSchema (carID Int, price Int)"))
    // create real DataStream
    val simpleCars = getCarStream(env)

    
    
    val rowCar = simpleCars
//
//    // register a new stream from source stream
    val newStream = sqlContext.sql("create stream CarStream carSchema source stream ('rowCar')")
    //println(sqlContext.sql("create schema carSchema3 (pedal String)"))

    println(newStream)
    //val newStream2 = sqlContext.sql("create stream CarStream (carID Int) source stream ('rowCar')")
    
  // newStream2.asInstanceOf[DataStream[_]] print

    /**
     *  
     *  SELECT
     * */
    
    
    
    /*val dStream = sqlContext.sql("select carID, price + 1 from CarStream")
    
    
    println(dStream.asInstanceOf[DataStream[_]].getType())


    val fStream = sqlContext.sql("select carID from CarStream where carID < price and carID > price")

    fStream.asInstanceOf[DataStream[_]] print*/

    val stream5 = sqlContext.sql("select carID, c.price * 0.05 as tax from CarStream as c")
    //println(stream5.asInstanceOf[DataStream[_]].getType().getGenericParameters.toArray.toList.map(x => x.toString))
    //println(    stream5.asInstanceOf[DataStream[_]].getType().isBasicType)


    val stream6 = sqlContext.sql("select c.pr * 1.05 as fullTax from (select pr from (select carID , price  as pr from CarStream) as d) as c")
//  val stream6 = sqlContext.sql("select c.carID from (select carID , price from CarStream)[Size 1] as c")

        /*
          println(stream6.asInstanceOf[DataStream[_]].getType())
          stream6.asInstanceOf[DataStream[_]] print

          println("Stream6: "+ stream6)
        */

    //println(stream6.asInstanceOf[DataStream[_]].getType())
    //stream6.asInstanceOf[DataStream[_]] print

    //val stream7 = sqlContext.validate("select c.pr from (select  pr from (select carID , price + 1 as pr from CarStream) as d) as c")
    //println(stream7)
    
    val stream8 = sqlContext.sql("select * from CarStream where price > 10 and price < 15")
    //stream8.asInstanceOf[DataStream[_]] print


    val stream9 = sqlContext.sql("select sum(price) from CarStream [size 5 every 2 millisec partitioned on carID] as c")
    stream9.asInstanceOf[DataStream[_]] print

    

    env.execute()
    
  }

  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }
  
  def getCarStream (env : StreamExecutionEnvironment) : DataStream [Car] = {
    Seq(Car(8888,1),Car(8888,2),Car(8888,3),Car(8888,4),Car(8888,5),Car(8888,6),Car(8888,7),Car(8888,8),Car(8888,9),Car(8888,10),Car(8888,11),Car(8888,12),Car(8888,13),Car(8888,14),Car(8888,15),Car(8888,16),Car(8888,17),Car(8888,18),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2),Car(8888,2))
      .toStream
  }

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"

}