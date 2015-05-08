package org.apache.flink.fsql
import scala.reflect.runtime.universe._
import java.sql.{Types=> jdbcType}



/**
 * the base type of all data type 
 * 
 */
abstract  class DataType {
  

}

case class PrimitiveType (scalaType: Type, jdbcType: Int)


case class StructType (fields: Array[StructField])  extends Seq[StructField] {
  
  /** returl all the field names in an array.   */
  def fieldNames: Array[String] = fields map {_.name}

  /** */
  private lazy val fieldNamesSet : Set[String] = fieldNames.toSet
  
  
  /** */
  private lazy val nameToField : Map[String, StructField] = fields map {f => (f.name -> f)} toMap
  
  /** 
   * Extracts a [[StructField]]  of the given name. If the [[StructType]] object does not
   * have a name matching the given name , `null` will be returned instead
   */
  def apply(name : String) : StructField = {
    nameToField getOrElse(name, 
      throw new IllegalArgumentException(s"""Field "$name" does not exists"""))
  }
  
  //Todo
  def merge (left : StructType, right: StructType) : StructType = StructType.this 
  
  /*
  * val newFields = ArrayBuffer.empty[StructField]

        leftFields.foreach {
          case leftField @ StructField(leftName, leftType, leftNullable, _) =>
            rightFields
              .find(_.name == leftName)
              .map { case rightField @ StructField(_, rightType, rightNullable, _) =>
                leftField.copy(
                  dataType = merge(leftType, rightType),
                  nullable = leftNullable || rightNullable)
              }
              .orElse(Some(leftField))
              .foreach(newFields += _)
        }

        rightFields
          .filterNot(f => leftFields.map(_.name).contains(f.name))
          .foreach(newFields += _)

        StructType(newFields)
  */

  override  def apply (fieldIndex : Int ) = fields(fieldIndex)
  override  def length : Int = fields.length
  override def iterator: Iterator[StructField] = fields.iterator
  
  /** */

}

object StructType

/**
 * A field inside a StructType* 
 */
case class StructField(
  name : String,
  dataType: PrimitiveType, 
  nullable: Boolean = true) {
  
  override def toString : String = s"StructField($name, ${dataType.scalaType}, $nullable )"
  
}


 object DataTypeTest {
   def main(args: Array[String]) {
     print(new StructField("name", PrimitiveType(typeOf[Int], jdbcType.INTEGER)))

   }
   
 }