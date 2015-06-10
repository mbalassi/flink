/*
package org.apache.flink.streaming.experimental

/**
 * Created by kidio on 10/06/15.
 */

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

object Macros {
  def getValue(varName: String):Unit = macro getValueImpl

  def getValueImpl(c: Context)(varName: c.Expr[Any]): c.Tree = {
    import c.universe._
    
    varName.tree match {
      case  Literal(Constant(x: String)) => q"""
                                                    if(${TermName(x)}.isInstanceOf[String])
                                                      ${TermName(x)}
                                                    else
                                                      ${TermName(x)}
                                                   """
      

      case Select(_, TermName(x: String))=> getValueImpl(c)((c.literal(x)))
      case _ => c.abort(c.enclosingPosition, s"parameter ${showRaw(varName.tree)} should be a string")
    }
  }
}



// Note: must compile Macros object before Test object
// Use  ':paste' if in RPL
object Test extends App {
 import Macros._
 val x = 100
 val y = "x"
 println(getValue(y))
  
}*/
