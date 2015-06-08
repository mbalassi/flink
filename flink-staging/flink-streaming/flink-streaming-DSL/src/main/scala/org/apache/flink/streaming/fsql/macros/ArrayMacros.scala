package org.apache.flink.streaming.fsql.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context


trait ArrMappable[T]{
  def toMap(t:T): Array[Any]
  def fromMap(map: Map[String,Any]): T
  def toTuple (t:T): Product
}

object ArrMappable{
  implicit def materializeMappable[T] : ArrMappable[T] = macro materializeMappableImpl[T]

  def materializeMappableImpl[T: c.WeakTypeTag](c: Context): c.Expr[ArrMappable[T]]= {
    import c.universe._
    val tpe = weakTypeOf[T]
    val companion = tpe.typeSymbol.companion

    val fields = tpe.decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get.paramLists.head


    val (toMapParams, fromMapParams) = fields.map { field =>
      val name = field.name.toTermName
      val decoded = name.decodedName.toString
      val returnType = tpe.decl(name).typeSignature

      (q"t.$name" , q"map($decoded).asInstanceOf[$returnType]")
    }.unzip


    val (toTuple, tpes) = fields.map { field =>
      val name = field.name.toTermName
      val returnType = tpe.decl(name).typeSignature
      (q"t.$name", q"$returnType")
    }.unzip
    
    
    


    c.Expr[ArrMappable[T]] { q"""
      new ArrMappable[$tpe] {
        def toMap(t: $tpe): Array[Any] = Array(..$toMapParams)
        def fromMap(map: Map[String,Any]): $tpe = $companion(..$fromMapParams)
        def toTuple(t: $tpe) : (..$tpes) = ((..$toTuple))
      }
    """ }
  }
  

  
}



//def fromMap(arr: Array[Any]): $tpe = $companion(..$fromMapParams)