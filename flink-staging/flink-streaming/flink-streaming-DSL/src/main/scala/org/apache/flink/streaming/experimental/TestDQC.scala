package org.apache.flink.streaming.experimental


import scala.reflect.macros._
import scala.language.experimental.macros
object TestDQC {

  implicit class DynSQLContext(sc: StringContext) {
    def sql(exprs: Any*) :String = macro DQC.dynsqlImpl
  }
  
  def main(args: Array[String]) {
    val name = "name"
    //println(sql"select age from person p where $name = ?")
  }


}
