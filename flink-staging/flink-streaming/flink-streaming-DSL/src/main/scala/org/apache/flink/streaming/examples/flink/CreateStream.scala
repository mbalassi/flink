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
  case class Schema(name: Option[String], fields: List[StructField])
  case class StructField( name : String,
                          dataType: BasicTypeInfo[_])
  case class SimpleCarEvent( speed: Int, time: Long) extends Serializable


  


  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val listOfField = Array("carId", "speed", "distance", "time").zip(schema).toList.map {  case (s:String, t:BasicTypeInfo[_]) => StructField(s,t)}
    val carSchema = Schema(Some("CarEvent"), listOfField)
    
    /**
     * * from file to Row stream
     */
    val textFromFile = getTextDataStream(env)
    

    val cars = textFromFile.map(_.split(",")).map(
        arr => { arrayToRow(arr, carSchema.fields.map(_.dataType).toArray)}
    ).filter(_.isDefined).map(_.get)
    //val e = cars.map(r => r.productElement(3))
    //e print

    
    
    /**
     * * From Socket to Row stream
     */
    /*  val textFromSocket = getSocketTextStream(env)
    textFromSocket.map(_.toLowerCase.length) print()
    */

    /**
     * * From case class to Row Stream
     */

    println(carSchema)
    val carsClass = cars.map(r => rowToCarEvent(r))
    // carsClass print

    // checking Car with schema
       // class CarEvent infor
      val m = runtimeMirror(getClass.getClassLoader)
      println(weakTypeOf[CarEvent].members.filter(!_.isMethod).map(x=>(x.name,m.staticClass("java.lang."+{ var value = x.typeSignature.toString; if (value=="Int") "Integer" else value})))) // each stream go with a schema (RowTypeInfor)
      val carTypeList = (weakTypeOf[CarEvent].members.filter(!_.isMethod).foldRight(List[String]())((field, list)=> list :+ field.typeSignature.toString).map(_.take(3)))
      val schemaTypeList = (listOfField.map(_.dataType.toString.take(3)))
      println(carTypeList == schemaTypeList)
    
      
    val fields = weakTypeOf[CarEvent].decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramLists.head.map(x => weakTypeOf[CarEvent].decl(x.name.toTermName).typeSignature)
    println(fields)
    
    // convert from Car to Row
    import org.apache.flink.streaming.experimental.ArrMappable
    def mapify[T: ArrMappable](t: T) = implicitly[ArrMappable[T]].toMap(t)
      
    val rowCar = carsClass.map(car => Row(mapify(car)))
    //rowCar print
    
    // get information from case class
    /*val m = runtimeMirror(getClass.getClassLoader)
    println(weakTypeOf[CarEvent].members.filter(!_.isMethod).map(x=>(x.name,m.staticClass("java.lang."+{ var value = x.typeSignature.toString; if (value=="Int") "Integer" else value})))) // each stream go with a schema (RowTypeInfor)
    println(cars.getType())*/

    
    
    /*val resultFields = inputType.getFieldNames.map(UnresolvedFieldReference)
    as(resultFields: _*)
    */

    /**
     * * From tuple to Row 
     */
    def mapify2[T: ArrMappable](t: T) = implicitly[ArrMappable[T]].toTuple(t)

    val tupleCar = carsClass.map(car => mapify2(car))
    
    val rowCar2 = tupleCar.map(x => Row(x.productIterator.toArray))
    
    rowCar2 print
    
    println(rowCar2.getType)
    
    env.execute()
  }
  
  
  
  
  
  def rowToCarEvent (row: Row): CarEvent ={
    CarEvent(row.productElement(0).asInstanceOf[Int], row.productElement(1).asInstanceOf[Int],row.productElement(2).asInstanceOf[Double],row.productElement(3).asInstanceOf[Long])
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
  


  // file 
  def getTextDataStream (env : StreamExecutionEnvironment): DataStream[String] ={
    env.readTextFile(inputPath)
  }

  // host
  
  def getSocketTextStream(env : StreamExecutionEnvironment) : DataStream[String] = {
    env.socketTextStream(hostName, port)
  }

  def genCarStream(): DataStream[SimpleCarEvent] = {
    Seq(SimpleCarEvent(1,6),SimpleCarEvent(4,11), SimpleCarEvent(20,13),SimpleCarEvent(80,14),SimpleCarEvent(100,18),
      SimpleCarEvent(1000,22),SimpleCarEvent(9,25),SimpleCarEvent(500,34),SimpleCarEvent(1,39),SimpleCarEvent(1,50)).toStream//,CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream
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