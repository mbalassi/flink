package org.apache.flink.streaming.fsql.macros


import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.fsql._

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

object FsqlMacros {


  import org.apache.flink.streaming.fsql.Ast._

  

  def compile(c: Context, parse: (FsqlParser, String) => ?[Ast.Statement[Option[String]]], sql: String)
             (sqlExpr: c.Tree): c.Expr[Any] = {
    
    import c.universe._

 
    def toPosition(f: Failure[_]) = {
      val lineOffset = sql.split("\n").take(f.line - 1).map(_.length).sum
      c.enclosingPosition.withPoint(wrappingPos(List(c.prefix.tree)).startOrPoint + f.column + lineOffset)
    }
    
    val result = (for {
      st <- parse(new FsqlParser{}, sql)
      rslv <- resolvedStreams(st)
    } yield rslv).fold( fail => c.abort(toPosition(fail), fail.message), rslv => generateCode(c, rslv)  )

    result


  }

  def generateCode  (c: Context, statement : Statement[Stream]): c.Expr[Any] = {
    import c.universe._

    val schemaSyn3 = weakTypeOf[StructField].typeSymbol.companion

    implicit val lift3 = Liftable[StructField] { (s: StructField) =>
      q"$schemaSyn3(${s.name}, ${s.dataType}, ${s.nullable})"
    }

    val schemaSyn2 = weakTypeOf[Schema].typeSymbol.companion

    implicit val lift2 = Liftable[Schema] {
      p: Schema => q"$schemaSyn2(${p.name}, List(..${p.fields}))"
    }

    val schemaSyn1 = weakTypeOf[CreateSchema[Stream]].typeSymbol.companion

    implicit val lift1 = Liftable[CreateSchema[Stream]] { p: CreateSchema[Stream] =>
      q"$schemaSyn1(${p.s}, ${p.schema}, ${p.parentSchema})"
    }

    statement match {


      case createSchema@Ast.CreateSchema(_, _, _) =>
        val tpe = weakTypeOf[CreateSchema[Stream]]

        c.Expr[Any]( q"""

            $createSchema.getSchema(${c.prefix.tree})

        """)


      //"create stream CarStream (speed int) source stream ('cars')",
      //CreateStream(CarStream,Schema(None,List(StructField(speed,int,true))),Some(StreamSource(cars)))

      //create stream CarStream carSchema source stream ('cars')
      //CreateStream(CarStream,Schema(Some(carSchema),List()),Some(StreamSource(cars)))

      // TODO: check same member type : schema and row
      case createStream@Ast.CreateStream(streamName, schema, source) =>
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
        val realStream = newTermName(source.get.asInstanceOf[StreamSource[Stream]].streamName)
        c.Expr[Any](q"$realStream")


        val putSourceStreamToMap = q"${c.prefix.tree}.streamsMap += ($streamName -> $realStream)"
        c.Expr[Any](q"$putSourceStreamToMap")

        // put to Map
        val putSchemaStreamToMap = q"${c.prefix.tree}.streamSchemaMap += ($streamName -> $schemaName)"




        // convert stream to Row
        val classToRow = q"""
                             import org.apache.flink.streaming.fsql.macros.ArrMappable2
                             def toTuple[T: ArrMappable2](z: T) = implicitly[ArrMappable2[T]].toTuple(z)

                              val newRowStream = $realStream.map(x=>toTuple(x))
                              ${c.prefix.tree}.streamsMap += ($streamName -> newRowStream)
                              newRowStream
                             """


        // well done with add stream of case class
        c.Expr[Any]( q"""
            $newSchema
            val classfieldsType = $realStream.getType.getTypeClass.getDeclaredFields.toList.map(_.getType.toString.toLowerCase.take(3))
            val schemaFieldsType = ${c.prefix.tree}.schemas($schemaName).fields.map(_.dataType.toString.toLowerCase.take(3))
            if (classfieldsType == schemaFieldsType){


              $putSchemaStreamToMap

              $classToRow

            } else {
              throw new IllegalArgumentException("class and schema do not match")
            }
        """)

      case sel@Ast.Select(proj, streamRefs, where, groupBy) => {

        /**
         * *  Projection
         * @param expr
         * @return
         */
        def genProject(expr: Ast.Expr[Stream]): c.Tree = {
          expr match {
            case col@Ast.Column(name, stream) => {

              q"""
                  val schemaName = ${c.prefix.tree}.streamSchemaMap(${stream.asInstanceOf[Stream].name})
                 val schema = ${c.prefix.tree}.schemas(schemaName)
                 val position = schema.fields.map(_.name).indexOf($name)
                 val fieldType = schema.fields.find(_.name == $name).get.dataType
                 r.productElement(position).asInstanceOf[Int] + 1
              """
            }

            case all@Ast.AllColumns(_) => q"r"

            case cons@Ast.Constant(tpe, value) => {
              tpe._2 match {
                case BasicTypeInfo.LONG_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Long]}.asInstanceOf[Long]
                  """
                case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Boolean]}.asInstanceOf[Boolean]
                  """
                case _  => 
                  q"""
                      ${value.asInstanceOf[String]}
                  """
                
              }
              
            }

            case arth@Ast.ArithExpr(lsh, op, rsh) => {
              op match {
                case "+" => q"${genProject(lsh)} + ${genProject(rsh)} "
                case "-" => q"${genProject(lsh)} - ${genProject(rsh)} "
                case "*" => q"${genProject(lsh)} * ${genProject(rsh)} "
                case "/" => q"${genProject(lsh)} / ${genProject(rsh)} "

              }
            }
            case _ => c.abort(c.enclosingPosition, "not support not column")
          }

          /*
          val iType  = tq"Int"
          q"r.asInstanceOf[Row].productElement(0).asInstanceOf[$iType] + 1"*/
        }
        val elements = proj.map( p => genProject(p.expr))

        def mapFunc (elements: List[c.Tree]) : c.Tree=
          q"""
             (r: org.apache.flink.streaming.fsql.Row) => {
              (..$elements)
             }
          """

        /**
         * * Predicate 
         * @param predicate
         * @return
         */
        def genPredicate(predicate: Ast.Predicate[Stream]) : c.Tree = {
          predicate match {
            case Comparison0(expr) => genProject(expr)
            case Comparison2(lsh, op, rsh) =>{
              op match {
                case Eq => q"${genProject(lsh)} == ${genProject(rsh)}"
                case Lt => q"${genProject(lsh)} < ${genProject(rsh)}"
                case Le => q"${genProject(lsh)} <= ${genProject(rsh)}"
                case Gt => q"${genProject(lsh)} > ${genProject(rsh)}"
                case Ge => q"${genProject(lsh)} >= ${genProject(rsh)}"
              }
            }
            case And(p1,p2) => q"${genPredicate(p1)} && ${genPredicate(p2)} "
            case Or(p1,p2) => q"${genPredicate(p1)}  || ${genPredicate(p2)} "

            case _ => c.abort(c.enclosingPosition, "do not support other predicate")
          }
          
        }
        
        val defaultPre = Comparison0[Stream](Ast.Constant((scala.reflect.runtime.universe.typeOf[Boolean] , 
                          BasicTypeInfo.BOOLEAN_TYPE_INFO), true).asInstanceOf[Ast.Expr[Stream]])
        val predicateTree = genPredicate(where.getOrElse(Where(defaultPre)).predicate)


        /**
         *      groupBy
         * */
        
         // TODO

        /**
         *  Window Spec 
         */
        
        def genWindow (windowSpec: Ast.WindowSpec[Stream]): c.Tree = {
          val value = windowSpec.window.policyBased.value
          val window = TermName("window")
          q"org.apache.flink.streaming.api.windowing.helper.Count.of($value)"
        }
        
        
        
        def genOptWindow (ws : Option[Ast.WindowSpec[Stream]]): c.Tree = {
          ws.fold(q"")(w => genWindow(w))
        }
        
        streamRefs match {
          case conc@Ast.ConcreteStream(s, w, None) =>

            val dstreamTree = 
              q"""
                   ${c.prefix.tree}.streamsMap(${s.name}).filter(${mapFunc(List(predicateTree))}).map(${mapFunc(elements)})

               """
            //                   ${c.prefix.tree}.streamsMap(${s.name}).map($mapFunc)

            val window = TermName("window")
            c.Expr[Any](
              q"$dstreamTree.$window(${genOptWindow(w)})"
            )

          case _ => c.abort(c.enclosingPosition, "concrete Stream only")
        }
      }
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





