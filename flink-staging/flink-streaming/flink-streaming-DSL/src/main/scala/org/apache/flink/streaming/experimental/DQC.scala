package org.apache.flink.streaming.experimental

import scala.reflect.macros._
import scala.language.experimental.macros
object DQC{



  def dynsqlImpl
  (c: Context)(exprs: c.Expr[Any]*): c.Expr[String] = {

    import c.universe._

    def append(t1: Tree, t2: Tree) = Apply(Select(t1, newTermName("+").encoded), List(t2))

    //val Expr(Apply(_, List(Apply(_, parts)))) = c.prefix
    val (Apply(_, List(Apply(_, parts)))) = c.prefix.tree


    val select = parts.head // tree
    val sqlExpr = exprs.zip(parts.tail).foldLeft(select) {
        case (acc, (Expr(expr), part)) => append(acc, append(expr, part))
      }

    val sql = select match {
      case Literal(Constant(sql: String)) => sql
      case _ => c.abort(c.enclosingPosition, "Expected String literal as first part of interpolation")
    }

    //reify{c.Expr[String](c.prefix.tree).splice}
    //reify{c.Expr[String](select).splice}
    //
    c.literal(showRaw(c.prefix.tree))
  }
}

/*
object Test {

  def main(args: Array[String]) {
    import DQC._
    val name = "name"
    println(sql"select agen from person p where $name = ?")
  }
  


}*/
