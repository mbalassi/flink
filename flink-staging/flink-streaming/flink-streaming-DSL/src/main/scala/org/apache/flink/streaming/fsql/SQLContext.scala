package org.apache.flink.streaming.fsql

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.fsql.Ast._
import org.apache.flink.streaming.fsql.macros.FsqlMacros

import scala.collection.mutable
import scala.collection.mutable.{HashMap, Map}




class SQLContext extends Serializable {
  
  self =>

  var schemas : Map[String, Schema] = new  HashMap[String, Schema]()
  var streamsMap : Map[String, DataStream[org.apache.flink.streaming.fsql.Row]] = new HashMap[String, DataStream[org.apache.flink.streaming.fsql.Row]]()
  val streamSchemaMap : Map[String, String] = new mutable.HashMap[String, String]()

  import scala.language.experimental.macros
  def sql(queryString: String) : Any = macro FsqlMacros.fsqlImpl

  def parse(p : FsqlParser, str :String) : ?[Ast.Statement[Option[String]]]= {
    p.parseAllWith(p.stmt, str)
  }

  def validate(queryString: String) : Any = {
    val result = (for {
      st <- parse(new FsqlParser{}, queryString)
      rslv <- resolvedStreams(st)

    } yield rslv).fold( fail => throw new IllegalArgumentException("cannot parser"), rslv => rslv )
    result.asInstanceOf[Ast.Select[Stream]].getType(self)//.projection.head.expr.asInstanceOf[ArithExpr[Stream]].rhs.asInstanceOf[ArithExpr[Stream]].rhs.getType(self)
  }

}




/*
  *  val people =
    *    sc.textFile("examples/src/main/resources/people.txt").map(
    *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
  *  val dataFrame = sqlContext.createDataFrame(people, schema)
  *  dataFrame.printSchema
  *  // root
  *  // |-- name: string (nullable = false)
  *  // |-- age: integer (nullable = true)
  *
  *  dataFrame.registerTempTable("people")
  *  sqlContext.sql("select name from people").collect.foreach(println)
  * }}}
*/
/*
@DeveloperApi
def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
// TODO: use MutableProjection when rowRDD is another DataFrame and the applied
// schema differs from the existing schema on any field data type.
val logicalPlan = LogicalRDD(schema.toAttributes, rowRDD)(self)
DataFrame(this, logicalPlan)
}

@DeveloperApi
def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
createDataFrame(rowRDD.rdd, schema)
}
*/
