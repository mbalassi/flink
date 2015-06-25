package org.apache.flink.streaming.fsql

import java.sql.{Types => JdbcTypes}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo

import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.parsing.combinator._

trait FsqlParser extends RegexParsers  with Ast.Unresolved with PackratParsers{

  import Ast._

  def parseAllWith(p: Parser[Statement], sql: String) = getParsingResult(parseAll(p, sql))

  def getParsingResult(res: ParseResult[Statement]) = res match {
    case Success(r, q) => ok(r)
    case err: NoSuccess => fail(err.msg, err.next.pos.column, err.next.pos.line)
  }

  /**
   * * Top statement
   */
  lazy val stmt = (selectStmtSyntax| createSchemaStmtSyntax | createStreamStmtSyntax | MergeStmtSyntax  | insertStmtSyntax| splitStmtSyntax)// |  | splitStmt | MergeStmt

  /**
   * *  STATEMENT: createSchemaStmtSyntax
   * *
   */

  lazy val createSchemaStmtSyntax: PackratParser[Statement] = "create".i ~> "schema".i ~> ident ~ new_schema ~ opt("extends".i ~> ident) ^^ {
    case i ~ n ~ e => CreateSchema[Option[String]](i, n, e)
  }

  lazy val typedColumn = (ident ~ dataType) ^^ {
    case n ~ t => StructField(n, t.toLowerCase)
  }
  lazy val anonymous_schema:Parser[Schema] = "(" ~> rep1sep(typedColumn, ",") <~ ")" ^^ { case columns => Schema(None, columns)}
  lazy val new_schema: Parser[Schema] =   anonymous_schema|ident ^^ { case i => Schema(Some(i), List())}

  /**
   *  STATEMENT: createStreamStmtSyntax
   * *
   */

  lazy val createStreamStmtSyntax: PackratParser[Statement] =
    "create".i ~> "stream".i ~> ident ~ new_schema ~ opt(source) ^^ {
      case i ~ schema ~ source => CreateStream(i, schema, source)
    }

  // source
  lazy val source: PackratParser[Source] = raw_source | derived_source
  lazy val derived_source = "as".i ~> ((selectStmtSyntax ^^ (s => SubSelectSource(s)))| (MergeStmtSyntax ^^ (m => MergedSource(m.asInstanceOf[Merge[Option[String]]]))))
  lazy val raw_source = "source".i ~> (host_source | file_source | stream_source)
  lazy val host_source = "socket" ~> "(" ~> stringLit ~ "," ~ integer ~ opt(","~> stringLit) <~ ")" ^^ {
    case host ~ _ ~ port ~ delimiter=> HostSource[Option[String]](host, port, delimiter)
  }

  lazy val file_source = "file" ~> "(" ~> stringLit ~ opt(","~> stringLit) <~ ")" ^^ {
    case path ~ delimiter => FileSource[Option[String]](path, delimiter)
  }

  lazy val stream_source = "stream" ~> "(" ~> stringLit <~ ")" ^^ {
    case stream => StreamSource[Option[String]](stream)
  }

  /**
   * STATEMENT: selectStmtSyntax
   **/
  lazy val selectStmtSyntax = selectClause ~ fromClause ~ opt(whereClause) ~opt(groupBy) ^^ {
    case s ~ f ~ w ~ g => Select(s, f,w, g)
  }

  // select clause
  lazy val selectClause = "select".i ~> repsep(named, ",")

  /**
   *  CLAUSE : NAMED  (PROJECTION)
   */
  lazy val named = expr ~ opt(opt("as".i) ~> ident) ^^ {
    case (c@Column(n, _)) ~ a       => Entry(n, a, c)
    case (c@AllColumns(_)) ~ a      => Entry("*", a, c)
    case (e@ArithExpr(_, _, _)) ~ a => Entry("<constant>", a, e)
    case (c@Constant(_, _,_)) ~ a     => Entry("<constant", a, c)
    case (f@Function(n, _)) ~ a     => Entry( n , a, f)
    case (c@Case(_,_)) ~ a          => Entry("case", a, c)

    // extra
    case (i@Input()) ~ a                 => Entry("?", a, i)
  }
  /*lazy val named = opt("distinct".i) ~> (comparison | arith | simpleTerm) ~ opt(opt("as".i) ~> ident) ^^ {
    case (c@Constant(_, _)) ~ a          => Named("<constant>", a, c)
    case (f@Function(n, _)) ~ a          => Named(n, a, f)
    case (c@Column(n, _)) ~ a            => Named(n, a, c)
    case (i@Input()) ~ a                 => Named("?", a, i)
    case (c@AllColumns(_)) ~ a           => Named("*", a, c)
    case (e@ArithExpr(_, _, _)) ~ a      => Named("<constant>", a, e)
    case (c@Comparison1(_, _)) ~ a       => Named("<constant>", a, c)
    case (c@Comparison2(_, _, _)) ~ a    => Named("<constant>", a, c)
    case (c@Comparison3(_, _, _, _)) ~ a => Named("<constant>", a, c)
    case (s@Subselect(_)) ~ a            => Named("subselect", a, s) // may not use
    case (c@Case(_, _)) ~ a              => Named("case", a, c)
  }*/

  // expr  (previous: term)
  lazy val expr = arithExpr | simpleExpr
  lazy val exprs: PackratParser[Expr] = "(" ~> repsep(expr, ",") <~ ")" ^^ ExprList.apply

  // TODO: improve arithExpr
  lazy val arithExpr: PackratParser[Expr] = (simpleExpr | arithParens) * (
    "+" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "+", rhs)}
      | "-" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "-", rhs)}
      | "*" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "*", rhs)}
      | "/" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "/", rhs)}
      | "%" ^^^ { (lhs: Expr, rhs: Expr) => ArithExpr(lhs, "%", rhs)}
    )

  lazy val arithParens: PackratParser[Expr] = "(" ~> arithExpr <~ ")"

  lazy val simpleExpr: PackratParser[Expr] = (
    caseExpr|
      functionExpr |
      stringLit ^^ constS |
      numericLit ^^ (n => if (n.contains(".")) constD(n.toDouble) else constI(n.toInt))|
      extraTerms|
      allColumns |
      column |
      "?"        ^^^ Input[Option[String]]()|
      optParens(simpleExpr)
    )

  lazy val extraTerms : PackratParser[Expr] = failure("expect an expression")
  lazy val allColumns =
    opt(ident <~ ".") <~ "*" ^^ (schema => AllColumns(schema))
  lazy val column = (
    ident ~ "." ~ ident ^^ { case s ~ _ ~ c => col(c, Some(s))}
      | ident ^^ (c => col(c, None))
    )

  private def col(name: String, schema: Option[String]) =
    Column(name, schema)

  /**
   *  CLAUSE: FROM
   */
  lazy val fromClause = "from".i ~> streamReference


  // stream Reference
  lazy val streamReference : Parser[StreamReference] =  derivedStream | joinedWindowStream | rawStream

  // raw (Windowed)Stream
  /*lazy val rawStream = ident ~ opt("as".i ~> ident)  ^^ {
    case n ~ a => Stream(n, a)
  }*/

  //lazy val rawStream = stream ^^ {case s => ConcreteStream(s, None)}
  lazy val rawStream = optParens(ident ~ opt(windowSpec) ~ opt("as".i ~> ident)) ^^ {
    case n ~ w ~ a => ConcreteStream(Stream(n, a), w, None)
  }


  lazy val windowSpec = "[" ~> window ~ opt(every) ~ opt(partition) <~ "]" ^^ {
    case w ~ e ~ p => WindowSpec(w, e, p)
  }


  lazy val window = "size".i ~> policyBased ^^ Window.apply
  lazy val every = "every".i ~> policyBased ^^ Every.apply
  lazy val policyBased: PackratParser[PolicyBased] = integer ~ opt(timeUnit) ~ opt("on".i ~> column) ^^ { //TODO: should be multiple columns
    case i ~ t ~ c => PolicyBased(i, t, c)
  }

  lazy val partition = "partitioned".i ~> "on".i ~> rep1sep(column, ",") ^^ Partition.apply

  /*  lazy val groupBy = "group".i ~> "by".i ~> rep1sep(expr, ",")  ^^ {
    case exprs => GroupBy(exprs)
  }*/

  lazy val derivedStream = subselect ~ opt(windowSpec) ~ opt("as".i) ~ ident ~ opt(joinType) ^^ {
    case s ~ w ~_ ~ i ~ j => DerivedStream(i,w, s.select , j)
  }


  // TODO: subselect cannot be a WindowStream(must be flattened)
  lazy val subselect = "("~> selectStmtSyntax <~ ")" ^^ SubSelect.apply

  // joinedWindowStream
  // TODO: stream/derivedStream
  lazy val joinedWindowStream = rawStream ~ opt(joinType) ^^ {  case c ~ j => c.copy(join = j)}
  lazy val joinType: PackratParser[Join] =  crossJoin | qualifiedJoin

  lazy val crossJoin = ("cross".i ~> "join".i ~> optParens(streamReference)) ^^ {
    s => Join(s, None, Cross)
  }
  lazy val qualifiedJoin = ("left".i ~> "join".i ~> optParens(streamReference) ~ opt(joinSpec)) ^^ {
    case s ~ j => Join(s, j , LeftOuter)
  }

  lazy val joinSpec :PackratParser[JoinSpec] = conditionJoin | namedColumnsJoin
  lazy val conditionJoin = "on".i ~> predicate  ^^ QualifiedJoin.apply
  lazy val namedColumnsJoin = "using".i ~> ident ^^ {
    col => NamedColumnJoin[Option[String]](col)
  }

  /**
   *  CLAUSE: WHERE
   */
  lazy val whereClause = "where".i ~> predicate ^^ Where.apply

  lazy val predicate: PackratParser[Predicate] = (simplePredicate | parens | notPredicate) * (
    "and".i ^^^ { (p1: Predicate, p2: Predicate) => And(p1, p2)}
      | "or".i ^^^ { (p1: Predicate, p2: Predicate) => Or(p1, p2)}
    )

  lazy val parens: PackratParser[Predicate] = "(" ~> predicate <~ ")"
  lazy val notPredicate: PackratParser[Predicate] = "not".i ~> predicate ^^ Not.apply

  lazy val simplePredicate: PackratParser[Predicate] = (
    expr ~ "=" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Eq, rhs)}
      | expr ~ "!=" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Neq, rhs)}
      | expr ~ "<>" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Neq, rhs)}
      | expr ~ "<" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Lt, rhs)}
      | expr ~ ">" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Gt, rhs)}
      | expr ~ "<=" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Le, rhs)}
      | expr ~ ">=" ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Ge, rhs)}
      | expr ~ "like".i ~ expr ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, Like, rhs)}
      //| expr ~ "in".i ~ (exprs | subselect) ^^ { case lhs ~ _ ~ rhs => Comparison2(lhs, In, rhs) }
      //| expr ~ "not".i ~ "in".i ~ (exprs | subselect) ^^ { case lhs ~ _ ~ _ ~ rhs => Comparison2(lhs, NotIn, rhs) }
      | expr ~ "between".i ~ expr ~ "and".i ~ expr ^^ { case t1 ~ _ ~ t2 ~ _ ~ t3 => Comparison3(t1, Between, t2, t3)}
      | expr ~ "not".i ~ "between".i ~ expr ~ "and".i ~ expr ^^ { case t1 ~ _ ~ _ ~ t2 ~ _ ~ t3 => Comparison3(t1, NotBetween, t2, t3)}
      | expr <~ "is".i ~ "null".i ^^ { t => Comparison1(t, IsNull)}
      | expr <~ "is".i ~ "not".i ~ "null".i ^^ { t => Comparison1(t, IsNotNull)}
    )

  // function
  lazy val functionExpr : PackratParser[Function] =
    ident ~ "(" ~ repsep(expr, ",") ~ ")" ^^ {
      case name ~ _ ~ params ~ _ => Function(name, params)
    }

  // case
  lazy val caseExpr = "case".i ~> rep(caseCondition) ~ opt(caseElse) <~ "end".i  ^^ {
    case conds ~ elze => Case(conds, elze)
  }

  lazy val caseCondition = "when".i ~> predicate ~ "then".i ~ expr ^^ {
    case p ~ _ ~ e => (p , e)
  }

  lazy val caseElse = "else".i ~> expr

  /**
   *  CLAUSE: GROUPBY
   *  Not supported yet: with rollup, collate
   */
  lazy val groupBy = "group".i ~> "by".i ~> rep1sep(column, ",")  ^^ {
    case fields => GroupBy(fields)
  }


  /**
   *  STATEMENT : INSERT 
   * */
  lazy val insertStmtSyntax : PackratParser[Statement]= "insert".i ~> "into".i ~> ident  ~ derived_source ^^ {
    case i  ~ s => Insert(i,s)
  }

  lazy val colNames = "(" ~> repsep(ident, ",") <~ ")"
  //  lazy val selectValue = optParens(selectStmtSyntax) ^^ SelectedInput.apply
  //  lazy val sourceStream = stream ^^ MergedStream.apply


  /**
   * * STATEMENT : MERGE 
   */

  lazy val MergeStmtSyntax : PackratParser[Statement]= "merge".i ~> ident ~ "," ~ rep1sep(ident, ",") ^^ { // make sure there are at least 2 stream
    case head ~_~ tail => Merge[Option[String]](head::tail)
  }



  /**
   *  STATEMENT: SPLIT
   */
  lazy val splitStmtSyntax : PackratParser[Statement] = "on" ~> ident ~ rep1sep(insertStmtSyntax, ";") ^^ {
    case i ~ branches => Split(i, branches.map(_.asInstanceOf[Insert[Option[String]]]))
  }

  /**
   * ==============================================
   * Utilities
   * ==============================================
   */

  def optParens[A](p: PackratParser[A]): PackratParser[A] = (
    "(" ~> p <~ ")" | p
    )


  /**
   * ==============================================
   * LEXICAL
   * ==============================================
   */

  /**
   * reserved keyword
   */

  lazy val reserved =
    ("select".i | "delete".i | "insert".i | "update".i | "from".i | "into".i | "where".i | "as".i |
      "and".i | "or".i | "join".i | "inner".i | "outer".i | "left".i | "right".i | "on".i | "group".i |
      "by".i | "having".i | "limit".i | "offset".i | "order".i | "asc".i | "desc".i | "distinct".i |
      "is".i | "not".i | "null".i | "between".i | "in".i | "exists".i | "values".i | "create".i |
      "set".i | "union".i | "except".i | "intersect".i |

      "window".i | "schema".i|"stream".i|
      "every".i| "size".i| "partitioned".i |
      "cross".i | "join".i | "left".i|
      "socket".i|"host".i|"file".i
      )

  implicit class KeywordOpts(kw: String) {
    def i = keyword(kw)
  }

  def keyword(kw: String): Parser[String] = ("(?i)" + kw + "\\b").r

  /**
   * identity
   */
  lazy val ident = rawIdent | quotedIdent

  lazy val rawIdent = not(reserved) ~> identValue

  lazy val quotedIdent = quoteChar ~> identValue <~ quoteChar

  def quoteChar: Parser[String] = "\""

  lazy val identValue: Parser[String] = "[a-zA-Z][a-zA-Z0-9_-]*".r


  /**
   * basic type
   */
  lazy val stringLit = "'" ~ """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""".r ~ "'" ^^ { case _ ~ s ~ _ => s}
  //  ("\""+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"\"").r

  lazy val numericLit: Parser[String] = """(\d+(\.\d*)?|\d*\.\d+)""".r // decimalNumber

  lazy val integer: Parser[Int] = """-?\d+""".r ^^ (s => s.toInt)

  /**
   * Basic type name
   */

  lazy val dataType = "int".i | "string".i | "double".i | "date".i | "byte".i | "short".i | "long".i | "float".i | "char".i | "boolean".i
  lazy val timeUnit = "microsec".i | "milisec".i | "sec".i | "min".i | "h".i | "d".i

  /**
   * Constant
   */
  def constB(b: Boolean) = const((typeOf[Boolean], BasicTypeInfo.BOOLEAN_TYPE_INFO), b, "boolean") //JdbcTypes.BOOLEAN

  def constS(s: String) = const((typeOf[String], BasicTypeInfo.STRING_TYPE_INFO), s, "string") //JdbcTypes.VARCHAR

  def constD(d: Double) = const((typeOf[Double], BasicTypeInfo.DOUBLE_TYPE_INFO), d, "double") // JdbcTypes.DOUBLE

  def constL(l: Long) = const((typeOf[Long], BasicTypeInfo.LONG_TYPE_INFO), l, "long") //JdbcTypes.BIGINT

  def constI(l: Int) = const((typeOf[Int], BasicTypeInfo.INT_TYPE_INFO), l, "int")

  def constNull = const((typeOf[AnyRef], BasicTypeInfo.VOID_TYPE_INFO), null, "null") //JdbcTypes.JAVA_OBJECT

  def const(tpe: (Type, BasicTypeInfo[_]), x: Any, typeName :String = "") = Constant[Option[String]](tpe, x, typeName)

  /**
   * ==============================================
   * SIMPLE TEST
   * ==============================================
   */

  def printCreateSchemaParser = parseAll(createSchemaStmtSyntax, "create schema name1 (a boolean) extends parents") match {
    case Success(r, n) => println(r)
    case _ => print("nothing")
  }

  def printCreateStreamParser = parseAll(createStreamStmtSyntax, "create stream name1 name2 source file ('path')") match {
    case Success(r, n) => println(r)
    case _ => print("nothing")
  }
}

object Test2 extends FsqlParser {

  def parser: (FsqlParser, String) => ?[Ast.Statement[Option[String]]] = (p: FsqlParser, s: String) => p.parseAllWith(p.stmt, s)

  def main(args: Array[String]) {

    /*
        println(parseAllWith(stmt, " select (age + p.hight) * 2 from person p where age >3 and hight <1 or weight = 2"))
        println("-------" * 10)
        */

    println("#########" * 10)

    val timer = Timer(true)
    
    val queries_0 = Array (
      // create schema
      "create schema mySchema0 (speed int, time long)",
      "create schema mySchema1 mySchema0",
      "create schema mySchema2 (id int) extends mySchema0",

      // creat stream 
      "create stream CarStream (speed int) source stream ('cars')",
      "create stream CarStream carSchema source stream ('cars')",
      "create stream myStream(time long) as select p.id from oldStream as p",
      "create stream myStream(time long) as merge x1, x2, x3",

      //merge
      "merge x1, x2, x3",
      // split
      "on x insert into x1 as select f1 from x where f1 = 3; insert into x1  as select f1 from x where f1 = 5",
    
    // select
      "select s.Quantity * Price, StockTick.Symbol from StockTick as s",
      "select id, s.speed, stream1.time from stream1 [size 3] as s cross join stream2 [size 3]",
      "select id from stream1 [size 3] as s1 left join suoi [size 3 partitioned on s] as s2 on s1.time=s2.thoigian" ,
      "select id from stream1 [size 3] as s1 left join suoi [size 3] as s2 on s1.time=s2.thoigian",
      "select id from (select p.id as id from oldStream2 as p) [size 3 partitioned on s] as q",
      "select id from stream1 [size 3] as s1 left join suoi [size 3] as s2 on s1.time=s2.thoigian",
      "Select Count(*) From Bid[Size 1] Where  item_id <= 200 or item_id >= 100",
      "select count(price) from (select plate , price from CarStream)[Size 1] as c",
      "select count(price) from (select plate , price  from CarStream [Size 1]) as c",
      "select * from (select plate , price from (select plate , price from (select plate , price from CarStream [Size 1] ) as e ) as d ) as c",
      "select c.plate + 1000/2.0 from (select plate as pr from (select plate , price + 1 as pr from CarStream) as d) as c",
      "Select count(*) From (Select * From Bid Where item_id >= 100 and item_id <= 200) [Size 1] p"
    
    )

    val queries_1 = Array (
      "create schema mySchema0 (speed int, time long)",
      "create schema mySchema1 mySchema0",
      "create schema mySchema2 (id int) extends mySchema0",
      "CREATE SCHEMA StockTickSchema (symbol String, sourceTimestamp Long, price Double, quantity Int, exchange String)",
      "CREATE SCHEMA StockTickSchema2 StockTickSchema",
      "CREATE SCHEMA StockTickSchema3 (id Int) EXTENDS StockTickSchema"
    )

    val queries_2 = Array (
      "create stream CarStream (speed int) source stream ('cars')",
      "create stream CarStream carSchema source stream ('cars')",
      "create stream myStream(time long) as select p.id from oldStream as p",
      "create stream myStream(time long) as merge x1, x2, x3",
      "CREATE STREAM StockTick StockTickSchema",
      "CREATE STREAM StockTick (symbol String, price Double, quantity Int)",
      "CREATE STREAM StockTick StockTickSchema source SOCKET ('98.138.253.109', 2000,';')",
      "CREATE STREAM StockTick StockTickSchema source Stream ('stockDataStream')",
      "CREATE STREAM StockTick (symbol String, price Double, quantity Int) source file ('//server/file.txt')",
      "CREATE STREAM StockPrice (symbol String, price Double) AS SELECT symbol, price FROM StockTick"
    )

    val queries_3 = Array (
      "select s.Quantity * Price + 10.2, StockTick.Symbol from StockTick as s",
      "select s.Quantity * Price + 10.2, s.Symbol from StockTick as s where s.price > 90.5 and Quantity <= 100000",
      "select c.price * 1000 from (select plate , price  from CarStream ) as c",
      "select max(s1.price), avg(s2.quantity) from stream1 [size 3 sec] as s1 cross join stream2 [size 3 sec] as s2",
      "select time, min(price)+1 from stream1 [size 3] as s1 left join stream2 [size 3] as s2 on s1.time=s2.time",
      "select id from (select p.id as id from oldStream2 as p) [size 3 partitioned on s] as q",
      "select  from stream1 [size 3 on s] as s1 left join stream2 [size 3 on s] as s2 on s1.time=s2.thoigian",
      "select id from stream1 [size 3 min] as s1 left join stream2 [size 3 min] as s2 using time",
      "Select Count(*) From Bid[Size 1 partitioned on field1] Where  item_id <= 200 or item_id >= 100 group by item_id",
      "select count(price) from (select plate , price  from CarStream [Size 1]) as c",
      "select count(price) from (select plate , price  from CarStream) [Size 1] as c",
      "select * from (select plate , price from (select plate , price from (select plate , price from CarStream [Size 1] ) as e ) as d ) as c",
      "select c.plate + 1000/2.0 from (select plate as pr from (select plate , price + 1 as pr from CarStream) as d) as c",
      "Select count(*), max(item_id) From (Select * From Bid Where item_id >= 100 and item_id <= 200) [Size 10] p"
    )

    val queries_4 = Array (
      "MERGE stockTickFromNYSE, stockTickFromAMEX, stockTickFromNASDAQ",
      "merge x1, x2, x3"
    )

    val queries_5 = Array (
      "INSERT INTO StockTick AS MERGE stockTickFromNYSE, stockTickFromAMEX",
      "INSERT INTO StockTick AS SELECT symbol, price FROM StockTick where quantity > 100000"
    )

    val queries_6 = Array (
      "on x insert into x1 as select f1 from x where f1 > 3 or f1 < 1 ; insert into x1  as select f1 from x where f1 <=3 and f >=1"
    )

    val context = new SQLContext()

    (0 to 2*queries_3.size-1).map { i =>
      timer(queries_3(i%(queries_3.size))+"\n",2,"")
      val result2 = for {
        st <- timer("parser_"+i%(queries_3.size), 2, parser(new FsqlParser {}, queries_3(i%(queries_3.size))))//i%(queries.size-1)
        rw <- timer("rewrite", 2, Ast.rewriteQuery(st))
        reslv <- timer("resolve", 2, Ast.resolvedStreams(rw))
      } yield st

       println(result2.fold(fail => throw new Exception(fail.message), rslv => rslv))
    }
    //asInstanceOf[Ast.Select[Stream]].streamReference.asInstanceOf[Ast.DerivedStream[Stream]].subSelect.
  }
}