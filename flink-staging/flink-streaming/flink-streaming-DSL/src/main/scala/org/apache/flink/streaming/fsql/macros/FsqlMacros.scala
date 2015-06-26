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
    val timer = Timer(true)
    val result = (for {
      st <- timer("parser", 2, parse(new FsqlParser{}, sql))
      rw <- timer("rewrite", 2, rewriteQuery(st))
      rslv <- timer("resolve", 2, resolvedStreams(rw))
    } yield rslv).fold( fail => c.abort(toPosition(fail), fail.message), rw => timer("gencode", 2, generateCode(c, rw))  )
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
               //todo: Int ?, can use pattern matching for fieldType
              q"""
                 val schemaName = ${c.prefix.tree}.streamSchemaMap(${stream.asInstanceOf[Stream].name})
                 val schema = ${c.prefix.tree}.schemas(schemaName)
                 val position = schema.fields.map(_.name).indexOf($name)
                 val fieldType = schema.fields.find(_.name == $name).get.dataType
                 r.productElement(position).asInstanceOf[Int]
              """
            }

            case all@Ast.AllColumns(_) => q"r"

            case cons@Ast.Constant(tpe, value, typeName) => {
              tpe._2 match {
                case BasicTypeInfo.LONG_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Long]}.asInstanceOf[Long]
                  """
                case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Boolean]}.asInstanceOf[Boolean]
                  """
                case BasicTypeInfo.DOUBLE_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Double]}.asInstanceOf[Double]
                  """
                case BasicTypeInfo.INT_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Int]}.asInstanceOf[Int]
                  """

                case BasicTypeInfo.BYTE_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Byte]}.asInstanceOf[Byte]
                  """
                case BasicTypeInfo.SHORT_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Short]}.asInstanceOf[Short]
                  """
                case BasicTypeInfo.FLOAT_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Float]}.asInstanceOf[Float]
                  """
                case BasicTypeInfo.CHAR_TYPE_INFO =>
                  q"""
                      ${value.asInstanceOf[Char]}.asInstanceOf[Char]
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
            case _ => c.abort(c.enclosingPosition, "not support not column !")
          }

          /*
          val iType  = tq"Int"
          q"r.asInstanceOf[Row].productElement(0).asInstanceOf[$iType] + 1"*/
        }

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
                case _ => throw new Exception("not support yet") //TODO
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
          /**
           *  Concrete Stream 
           */
          case conc@Ast.ConcreteStream(s, None, None) =>

            val elements = proj.map( p => genProject(p.expr))

            val dstreamTree = 
              q"""
                   ${c.prefix.tree}.streamsMap(${s.name}).filter(${mapFunc(List(predicateTree))}).map(${mapFunc(elements)})
               """
            c.Expr[Any](q"$dstreamTree")


          case Ast.ConcreteStream(s, w, None) if w.isDefined   =>

            val window = TermName("window")

            val dstreamTree2 =
              q"""
                   ${c.prefix.tree}.streamsMap(${s.name}).filter(${mapFunc(List(predicateTree))})
               """

            val windowedStream = q"$dstreamTree2.$window(${genOptWindow(w)})"


            val resultTree = proj.head.expr match {
              case f@Ast.Function(n,params) if n == "sum" =>
                val Column(name,stream) = params.head.asInstanceOf[Column[Stream]]
                val sum = TermName("max")
                
                q"""
                  val schemaName = ${c.prefix.tree}.streamSchemaMap(${stream.asInstanceOf[Stream].name})
                  val schema = ${c.prefix.tree}.schemas(schemaName)
                  val position = schema.fields.map(_.name).indexOf($name)
                  val mapFun : (Iterable[Row],org.apache.flink.util.Collector[Any]) => Unit = {
                  case x => {
                    val sum = x._1.toIterator.map(_.productElement(position).asInstanceOf[Int]).$sum
                    x._2.collect(sum)
                  }
                }
                
                  $windowedStream.mapWindow(mapFun).flatten
                """
              case _ =>
                throw new Exception("do not support Not Aggregation func")
            }

            c.Expr[Any](
              q"$resultTree"
            )

          /**
           * * Derived Stream
           */
          case derivedStream@Ast.DerivedStream(name, winSpec,subSelect,join) =>{
            // gen subSelect -> add to SQLContext
            val subStreamTree = generateCode(c,subSelect).tree

            val fieldNames = subSelect.projection.map(x=> x.aliasName)
            val subQueryTree = 
              q"""
                // gen dataStream (tuple)
                val tempStream = $subStreamTree
                
                // gen Schema
                val types = tempStream.asInstanceOf[DataStream[_]].getType()
                val fieldTypes = 
                  if (types.isBasicType){
                    List(types)
                  } else
                  types.getGenericParameters.toArray.toList.map(x => x.toString).map {
                  x => if (x=="Int") "int" else x.toLowerCase
                  
                  }
                val structFields = $fieldNames.zip(fieldTypes).map(x => org.apache.flink.streaming.fsql.Ast.StructField(x._1, x._2.toString))
                val schema = org.apache.flink.streaming.fsql.Ast.Schema(Some($name), structFields)
                
                // convert dataStream to Row
                
                val rowStream = 
                  if (types.isBasicType)
                    tempStream.map(x=> org.apache.flink.streaming.fsql.Row(Array[Any](x)))
                  else
                   
                    tempStream.map(x=>org.apache.flink.streaming.fsql.Row((x,0).productIterator.toArray.head.asInstanceOf[Product].productIterator.toArray))
                
                // put 
                if(${c.prefix.tree}.streamsMap.contains($name))
                  throw new IllegalArgumentException(" temporary Stream exists!")
                ${c.prefix.tree}.streamsMap +=($name -> rowStream)
                
                ${c.prefix.tree}.schemas    +=($name -> schema)
                
                ${c.prefix.tree}.streamSchemaMap    +=($name -> $name)

                
                """
           
            val x = ConcreteStream(Stream(name,None,true), winSpec, join)
            val allTree = generateCode(c,sel.copy(streamReference = x))
            
            c.Expr[Any](
              q"""
                $subQueryTree
                $allTree
              """
            )
          } // end Derived Stream
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
