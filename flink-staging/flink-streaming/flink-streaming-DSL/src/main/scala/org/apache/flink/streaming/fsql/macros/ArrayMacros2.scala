package org.apache.flink.streaming.fsql.macros


import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context


trait ArrMappable2[T]{
  def toTuple (t:T): org.apache.flink.streaming.fsql.Row
}

object ArrMappable2{
  implicit def materializeMappable[T] : ArrMappable2[T] = macro materializeMappableImpl[T]

  def materializeMappableImpl[T: c.WeakTypeTag](c: Context): c.Expr[ArrMappable2[T]]= {
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
    
    
    


    c.Expr[ArrMappable2[T]] { q"""
      new ArrMappable2[$tpe] {
        def toTuple(t: $tpe):org.apache.flink.streaming.fsql.Row  = org.apache.flink.streaming.fsql.Row(Array(..$toTuple))
      }
    """ }
  }
  

  
}



//def fromMap(arr: Array[Any]): $tpe = $companion(..$fromMapParams)