package org.apache.flink.streaming.experimental
import scala.reflect.macros.blackbox.Context
import scala.language.experimental.macros

/**
 * Created by kidio on 04/06/15.
 */

trait ToTuple[Z,T] {
  def toTuple(z: Z): T
}
object ToTuple {

  implicit def toTupleMacro[Z,T] : ToTuple[Z,T] = macro toTupleMacroImpl[Z,T]
  
  def toTupleMacroImpl[Z: c.WeakTypeTag,T](c:Context): c.Expr[ToTuple[Z,T]] = {
    import c.universe._
    val tpe : Type = weakTypeOf[Z]
    val (nmes, tpes) = getFieldNamesAndTypes(c)(tpe).unzip
    
    val fldSels : Iterable[Select] = nmes.map { nme =>
        q"""
           z.$nme
        """
    }
    
    val toTuple : Tree = 
      q"""
        new ToTuple[$tpe, (..$tpes)] {
          def toTuple(z:$tpe) = (..$fldSels)
        }
      """
    c.Expr[ToTuple[Z,T]](toTuple)
  }
  
  def getFieldNamesAndTypes(c:Context)(tpe: c.universe.Type) : Iterable[(c.universe.TermName,c.universe.Type)] = {
    import c.universe._
    
    object CaseField {
      def unapply (trmSym: TermSymbol): Option[(TermName, Type)] ={
        if(trmSym.isVal && trmSym.isCaseAccessor)
          Some((newTermName(trmSym.name.toString.trim).toTermName,trmSym.typeSignature))
        else
          None
      }
      
    }
    tpe.declarations.collect {
      case CaseField(nme, tpe) => (nme,tpe)
      
    }
    
  }
}
