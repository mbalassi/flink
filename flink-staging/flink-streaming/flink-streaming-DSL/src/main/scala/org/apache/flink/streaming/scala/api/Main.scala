package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.experimental.Row
import org.apache.flink.streaming.fsql.SQLContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._




object Main {
  case class SimpleCarEvent( speed: Int, time: Long) extends Serializable
  case class Car (name: String)

  def main(args: Array[String]) {
    val sqlContext = new SQLContext()
    //println(sqlContext.sql("create schema myschema (speed int)"))
    //println(sqlContext.sql("create schema myschema2 (time long) extends myschema"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    
    val cars = getTextDataStream(env)
    
    //sqlContext.streamsMap += ("cars" -> cars.map(car => Row(Array(car))))

    import org.apache.flink.streaming.experimental.ArrMappable
    def mapify[T: ArrMappable](t: T) = implicitly[ArrMappable[T]].toMap(t)

    val rowCar = cars.map(car => Car(car)).map( car=>  Row(mapify(car)))
    sqlContext.streamsMap += ("cars" -> rowCar)

    val h = sqlContext.sql("select * from cars")
    
    
    
    h.asInstanceOf[DataStream[_]] print

    
    
    
    
    
    
    env.execute()

  }



  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }
  
  def getCarStream (env : StreamExecutionEnvironment) : DataStream [Car] = {
    Seq(Car("Lexus")).toStream
  }

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"


}