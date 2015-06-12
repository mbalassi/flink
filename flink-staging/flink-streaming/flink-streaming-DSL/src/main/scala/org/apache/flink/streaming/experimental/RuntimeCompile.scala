package org.apache.flink.streaming.experimental

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox
case class Car [T](speed : T)

object RuntimeCompile extends App{
  val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
  val x = "Int"
  val t = tq"${TypeName(x)}"
  val q = q"""
            def f(x:_root_.org.apache.flink.streaming.experimental.Car[$t]) = 1
            f _
          """
  val func = toolbox.compile(q)
  val realFunc = func()
  println(realFunc.asInstanceOf[Function1[Car[_],_]](Car(1)).asInstanceOf[Int]  +1)
}