package org.apache.flink.streaming.fsql


import org.apache.flink.streaming.api.scala.DataStream

import scala.reflect.macros.blackbox.Context
import scala.language.experimental.macros

object FsqlMacros {


  import Ast._

  

  def compile(c: Context, parse: (FsqlParser, String) => ?[Ast.Statement[Option[String]]], sql: String)
             (sqlExpr: c.Tree): c.Expr[Any] ={
    
    import c.universe._

 
    def toPosition(f: Failure[_]) = {
      val lineOffset = sql.split("\n").take(f.line - 1).map(_.length).sum
      c.enclosingPosition.withPoint(wrappingPos(List(c.prefix.tree)).startOrPoint + f.column + lineOffset)
    }
    
    val result = (for {
      st <- parse(new FsqlParser{}, sql)
    } yield st).fold( fail => c.abort(toPosition(fail), fail.message), st => generateCode(c, st)  )

    result

    /*val result2 : CreateSchema[Option[String]]= result.asInstanceOf[CreateSchema[Option[String]]]


    /*
        c.prefix.tree
        println(result.getOrElse("fail").asInstanceOf[Ast.CreateSchema[Option[String]]].getSchema(context))
        println(context.schemas.head)*/

    val tpe = weakTypeOf[CreateSchema[Option[String]]]


    c.Expr[Any](q"""
                    
      $result2.getSchema(${c.prefix.tree})

      ${c.prefix.tree}.schemas.head
      
    """)*/
  }

  def generateCode  (c: Context, statement : Statement[Option[String]]): c.Expr[Any] = {
    import c.universe._

    val schemaSyn3 = weakTypeOf[StructField].typeSymbol.companion

    implicit val lift3 = Liftable[StructField] { (s:StructField) =>
      q"$schemaSyn3(${s.name}, ${s.dataType}, ${s.nullable})"
    }

    val schemaSyn2 = weakTypeOf[Schema].typeSymbol.companion

    implicit val lift2 = Liftable[Schema] {
      p:Schema => q"$schemaSyn2(${p.name}, List(..${p.fields}))"
    }

    val schemaSyn1 = weakTypeOf[CreateSchema[Option[String]]].typeSymbol.companion

    implicit val lift1 = Liftable[CreateSchema[Option[String]]] { p:CreateSchema[Option[String]] =>
      q"$schemaSyn1(${p.s}, ${p.schema}, ${p.parentSchema})"
    }
/** !!! Important

    if (statement.isInstanceOf[CreateSchema[Option[String]]]) {
        val result2 = statement.asInstanceOf[CreateSchema[Option[String]]]
  
  
        val tpe = weakTypeOf[CreateSchema[Option[String]]]
  
  
        c.Expr[Any]( q"""
                      
        $result2.getSchema(${c.prefix.tree})
  
        ${c.prefix.tree}.schemas
        
      """)
    } else if (statement.isInstanceOf[Ast.Select[Option[String]]]){
      
      val result = statement.asInstanceOf[Ast.Select[Option[String]]]
      val nameOfString = result.streamReference.name

      c.Expr[Any]( q"""       
        val  p = ${c.prefix.tree}.streamsMap($nameOfString)
        p print
      """)
      
    } else {

      c.Expr[Any]( q"""
        nothing
      """)
      
    }
 **/
    statement match {


      case creatSchema@Ast.CreateSchema(_,_,_) =>
        val tpe = weakTypeOf[CreateSchema[Option[String]]]

        c.Expr[Any]( q"""

            $creatSchema.getSchema(${c.prefix.tree})

        """) //${c.prefix.tree}.schemas
      
      case select@Ast.Select(_,_,_,_) =>
        val nameOfString = select.streamReference.name

        c.Expr[Any]( q"""
            ${c.prefix.tree}.streamsMap($nameOfString).map(x => "1")
          """)
        
      //"create stream CarStream (speed int) source stream ('cars')",
      //CreateStream(CarStream,Schema(None,List(StructField(speed,int,true))),Some(StreamSource(cars)))
      
      //create stream CarStream carSchema source stream ('cars')
      //CreateStream(CarStream,Schema(Some(carSchema),List()),Some(StreamSource(cars)))
        
      // TODO: check same member type : schema and row
      case createStream@Ast.CreateStream(streamName,schema,source) =>
        var schemaName = ""
        val newSchema = schema.name match {
          case None =>
            schemaName = streamName
            generateCode(c, CreateSchema(streamName, schema, None))
          
          case Some(name) =>
            schemaName = name
            c.Expr[Any](q"")
        }
            
            // case : source stream
            // get original stream
            val realStream = newTermName(source.get.asInstanceOf[StreamSource[Option[String]]].streamName)
            c.Expr[Any](q"$realStream")
            
            // TODO: convert to row stream
            
            val putSourceStreamToMap = q"${c.prefix.tree}.streamsMap += ($streamName -> $realStream)"
            c.Expr[Any](q"$putSourceStreamToMap")
            
            // put to Map
            val putSchemaStreamToMap = q"${c.prefix.tree}.streamSchemaMap += ($streamName -> $schemaName)"

        
            
            c.Expr[Any] (
              q"""
                 $newSchema
                 $putSourceStreamToMap
                 $putSchemaStreamToMap
               """)
            c.Expr[Any](q"$realStream")

        // well done with add stream of case class
        val listOfTypes = schema.fields.map(_.dataType.toString.toLowerCase.take(3))
        c.Expr[Any](q"""
            $newSchema
            val classfieldsType = $realStream.getType.getTypeClass.getDeclaredFields.toList.map(_.getType.toString.toLowerCase.take(3))
            val schemaFieldsType = ${c.prefix.tree}.schemas($schemaName).fields.map(_.dataType.toString.toLowerCase.take(3))
            if (classfieldsType == schemaFieldsType){
              $putSourceStreamToMap
              $putSchemaStreamToMap
            } else {
              throw new IllegalArgumentException("class and schema do not match")
            
            }
            
            
        """)










      case _ => c.abort(c.enclosingPosition, "not a case")
      }
  }

  def fsqlImpl(c: Context)(queryString: c.Expr[String]) : c.Expr[Any] ={

    import c.universe._

    val sql = queryString.tree match {
      case Literal(Constant(sql: String))  => sql
      case _ => c.abort(c.enclosingPosition, "Argument to macro must be a String literal")
    }
    compile(c, (parser, s) => parser.parseAllWith(parser.stmt, s), sql)(Literal(Constant(sql)))
    
//    c.literal(show(c.prefix.tree))
  }

}


/**
 * 
 *  using mirror in macro, String -> symbol
 *
 * * http://docs.scala-lang.org/overviews/reflection/environment-universes-mirrors.html
 * 
 *
import scala.reflect.macros.Context
case class Location(filename: String, line: Int, column: Int)
object Macros {
  def currentLocation: Location = macro impl
  def impl(c: Context): c.Expr[Location] = {
    import c.universe._
    val pos = c.macroApplication.pos
    val clsLocation = c.mirror.staticModule("Location") // get symbol of "Location" object
    c.Expr(Apply(Ident(clsLocation), List(Literal(Constant(pos.source.path)), Literal(Constant(pos.line)), Literal(Constant(pos.column)))))
  }
} 
 */





