package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.experimental.{ArrMappable, Row}
import org.apache.flink.streaming.fsql.SQLContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._




object Main {
//  case class SimpleCarEvent( speed: Int, time: Long) extends Serializable
  case class Car (plate: Int) extends Serializable

  def main(args: Array[String]) {
    val sqlContext = new SQLContext()
    println(sqlContext.sql("create schema carSchema2 (speed int)"))
    //println(sqlContext.sql("create schema myschema2 (time long) extends myschema"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    
    val cars = getTextDataStream(env)
    
    //sqlContext.streamsMap += ("cars" -> cars.map(car => Row(Array(car))))
      /**
       * convert stream of case class to stream of Row
       */

/*  import org.apache.flink.streaming.experimental.ArrMappable
    def mapify[T: ArrMappable](t: T) = implicitly[ArrMappable[T]].toMap(t)

    val rowCar = cars.map(car => Car(car)).map( car=>  Row(mapify(car)))
    sqlContext.streamsMap += ("cars" -> rowCar)

    val h = sqlContext.sql("select * from cars")
    h.asInstanceOf[DataStream[_]] print*/

      /**
       * * create stream
       */
    // create schema
     println(sqlContext.sql("create schema carSchema (pedal Int)"))
    // create real DataStream
    val simpleCars = getCarStream(env)

    
    import org.apache.flink.streaming.experimental.ArrMappable
    def mapify[T: ArrMappable](t: T) = implicitly[ArrMappable[T]].toMap(t)

    val rowCar = simpleCars.map( car=>  Row(mapify(car)))
    rowCar print
    
    // register a new stream from source stream
    val newStream = sqlContext.sql("create stream CarStream carSchema source stream ('rowCar')")
    
    println(sqlContext.schemas)
    println(sqlContext.streamsMap)
    println(sqlContext.streamSchemaMap)

    
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
    Seq(Car(8888)).toStream
  }

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"


}