package org.apache.flink.streaming.examples.flink

import org.apache.flink.api.common.typeinfo.{TypeInformation, BasicTypeInfo}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.experimental.{RowTypeInfo, Row, Tuples}
import scala.reflect.runtime.universe._

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


//http://www.strongtyped.io/blog/2014/05/23/case-class-related-macros/

object CreateStream {
  case class Field(typeInfo : Type)
  case class CarEvent( speed: Int, time: Long) extends Serializable




  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    
    
    val textFromFile = getTextDataStream(env)

    val cars = textFromFile.map(_.split(",")).map(
        arr => { arrayToRow(arr, schema)}
    ).filter(_.isDefined).map(_.get)
    
    
    val e = cars.map(r => r.productElement(3))
    e print

    val m = runtimeMirror(getClass.getClassLoader)


    println(weakTypeOf[CarEvent].members.filter(!_.isMethod).map(x=>(x.name,m.staticClass("java.lang."+{ var value = x.typeSignature.toString; if (value=="Int") "Integer" else value})))) // each stream go with a schema (RowTypeInfor)
    println(cars.getType())
    env.execute()
    
    /*val resultFields = inputType.getFieldNames.map(UnresolvedFieldReference)
    as(resultFields: _*)
    */
    
  /*  val textFromSocket = getSocketTextStream(env)
    textFromSocket.map(_.toLowerCase.length) print()
    env.execute()*/

  }
  
  
  

  
  def arrayToRow (arr : Array[String], schema: Array[_<:BasicTypeInfo[_]]): Option[Row] ={
    val row =  new Row(arr.length)

    for (i <- 0 until arr.length)  {
      
      val castedValue = try {
        schema(i) match {
          case BasicTypeInfo.STRING_TYPE_INFO => arr(i).toString
          case BasicTypeInfo.BOOLEAN_TYPE_INFO => arr(i).toBoolean
          case BasicTypeInfo.BYTE_TYPE_INFO => arr(i).toByte
          case BasicTypeInfo.SHORT_TYPE_INFO => arr(i).toShort
          case BasicTypeInfo.INT_TYPE_INFO => arr(i).toInt
          case BasicTypeInfo.LONG_TYPE_INFO => arr(i).toLong
          case BasicTypeInfo.FLOAT_TYPE_INFO  => arr(i).toFloat
          case BasicTypeInfo.DOUBLE_TYPE_INFO => arr(i).toDouble
          case BasicTypeInfo.CHAR_TYPE_INFO => arr(i).toCharArray.head
          //case BasicTypeInfo.DATE_TYPE_INFO =>                      //TODO: date type
        }
      } catch {
        case e: Exception => e.printStackTrace(); return None // TODO //println("wrong format at line:"+ arr.mkString(","))
      }

        row.setField(i, castedValue)
      }

      Some(row)
    
      // transform
      // check create Select

  }
  

  def arrayToTuple(arr: Array[String]) ={
    (arr(1),arr(2))
  }

  // file 
  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }

  // host
  
  def getSocketTextStream(env : StreamExecutionEnvironment) : DataStream[String] = {
    env.socketTextStream(hostName, port)
  }

  def genCarStream(): DataStream[CarEvent] = {
    Seq(CarEvent(1,6),CarEvent(4,11), CarEvent(20,13),CarEvent(80,14),CarEvent(100,18),CarEvent(1000,22),CarEvent(9,25),CarEvent(500,34),CarEvent(1,39),CarEvent(1,50)).toStream//,CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream

  }
  // subselect
  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long)

  private val inputPath: String = "./flink-staging/flink-streaming/flink-streaming-DSL/src/main/scala/org/apache/flink/streaming/util/carEvent.txt"
  private val hostName :String= "localhost"
  private val port :Int= 2015
  private val schema = Seq(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO).toArray

  
}



// csv 
//http://super-csv.github.io/super-csv/examples_reading_variable_cols.html




/*
* object Expression {
  def freshName(prefix: String): String = {
    s"$prefix-${freshNameCounter.getAndIncrement}"
  }

  val freshNameCounter = new AtomicInteger
}

* */


/*
  override def translate[A](op: PlanNode)(implicit tpe: TypeInformation[A]): DataStream[A] = {
// TRANSLATE FROM ROW -> A
// also have a snippet of code : any -> Row

createTable  :  A -> Row
 */



/*  def rowToCarEvent[T: WeakTypeTag](row: Row)= {
//    weakTypeOf[T].members.filter(!_.isMethod).toList
    weakTypeOf[CarEvent].members.filter(!_.isMethod).toList

  }*/