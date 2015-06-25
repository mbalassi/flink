package org.apache.flink.streaming.experimental


import java.sql.{Types => JdbcTypes}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo

import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.parsing.combinator._

trait FsqlParser extends RegexParsers with PackratParsers{


  def parseAllWith(p: Parser[Any], sql: String) = getParsingResult(parseAll(p, sql))

  def getParsingResult(res: ParseResult[Any]) = res match {
    case Success(r, q) => r
    case err: NoSuccess => (err.msg, err.next.pos.column, err.next.pos.line)
  }

  /**
   * * Top statement
   */
  lazy val stmt = (selectStmtSyntax| createSchemaStmtSyntax | createStreamStmtSyntax | MergeStmtSyntax  | insertStmtSyntax| splitStmtSyntax)// |  | splitStmt | MergeStmt

  /**
   * *  STATEMENT: createSchemaStmtSyntax
   * *
   */

  lazy val createSchemaStmtSyntax: PackratParser[Any] = "create".i ~> "schema".i ~> ident ~ new_schema ~ opt("extends".i ~> ident) 

  lazy val typedColumn = (ident ~ dataType)
  
  lazy val anonymous_schema:Parser[Any] = "(" ~> rep1sep(typedColumn, ",") <~ ")" 
  lazy val new_schema: Parser[Any] =   anonymous_schema|ident 

  /**
   *  STATEMENT: createStreamStmtSyntax
   * *
   */

  lazy val createStreamStmtSyntax: PackratParser[Any] =
    "create".i ~> "stream".i ~> ident ~ new_schema ~ opt(source) 
  // source
  lazy val source: PackratParser[Any] = raw_source | derived_source
  lazy val derived_source = "as".i ~> (selectStmtSyntax |MergeStmtSyntax )
  lazy val raw_source = "source".i ~> (host_source | file_source | stream_source)
  lazy val host_source = "host" ~> "(" ~> stringLit ~ "," ~ integer ~ opt(","~> stringLit) <~ ")" 

  lazy val file_source = "file" ~> "(" ~> stringLit ~ opt(","~> stringLit) <~ ")"

  lazy val stream_source = "stream" ~> "(" ~> stringLit <~ ")" 

  /**
   * STATEMENT: selectStmtSyntax
   **/
  lazy val selectStmtSyntax = selectClause ~ fromClause ~ opt(whereClause) ~opt(groupBy) 

  // select clause
  lazy val selectClause = "select".i ~> repsep(named, ",")

  /**
   *  CLAUSE : NAMED  (PROJECTION)
   */
  lazy val named = expr ~ opt(opt("as".i) ~> ident) 

  // expr  (previous: term)
  lazy val expr = arithExpr | simpleExpr
  lazy val exprs: PackratParser[Any] = "(" ~> repsep(expr, ",") <~ ")" 

  // TODO: improve arithExpr
  lazy val arithExpr: PackratParser[Any] = (simpleExpr | arithParens) * (
    "+" ^^^ { (lhs: Any, rhs: Any) => (lhs, "+", rhs)}
      | "-" ^^^ { (lhs: Any, rhs: Any) => (lhs, "-", rhs)}
      | "*" ^^^ { (lhs: Any, rhs: Any) => (lhs, "*", rhs)}
      | "/" ^^^ { (lhs: Any, rhs: Any) => (lhs, "/", rhs)}
      | "%" ^^^ { (lhs: Any, rhs: Any) => (lhs, "%", rhs)}
    )


  lazy val arithParens: PackratParser[Any] = "(" ~> arithExpr <~ ")"

  lazy val simpleExpr: PackratParser[Any] = (
    caseExpr|
      functionExpr |
      stringLit  |
      numericLit|
      extraTerms|
      allColumns |
      column |
      "?"        |
      optParens(simpleExpr)
    )

  lazy val extraTerms : PackratParser[Any] = failure("expect an expression")
  lazy val allColumns =
    opt(ident <~ ".") <~ "*"
  lazy val column = (
    ident ~ "." ~ ident
      | ident
    )


  /**
   *  CLAUSE: FROM
   */
  lazy val fromClause = "from".i ~> streamReference


  // stream Reference
  lazy val streamReference : Parser[Any] =  derivedStream | joinedWindowStream | rawStream

  // raw (Windowed)Stream
  /*lazy val rawStream = ident ~ opt("as".i ~> ident)  ^^ {
    case n ~ a => Stream(n, a)
  }*/

  //lazy val rawStream = stream ^^ {case s => ConcreteStream(s, None)}
  lazy val rawStream = optParens(ident ~ opt(windowSpec) ~ opt("as".i ~> ident))

  lazy val windowSpec = "[" ~> window ~ opt(every) ~ opt(partition) <~ "]"


  lazy val window = "size".i ~> policyBased
  lazy val every = "every".i ~> policyBased
  lazy val policyBased: PackratParser[Any] = integer ~ opt(timeUnit) ~ opt("on".i ~> column)

  lazy val partition = "partitioned".i ~> "on".i ~> rep1sep(column, ",")

  /*  lazy val groupBy = "group".i ~> "by".i ~> rep1sep(expr, ",")  ^^ {
    case exprs => GroupBy(exprs)
  }*/

  lazy val derivedStream = subselect ~ opt(windowSpec) ~ opt("as".i) ~ ident ~ opt(joinType)

  // TODO: subselect cannot be a WindowStream(must be flattened)
  lazy val subselect = "("~> selectStmtSyntax <~ ")"

  // joinedWindowStream
  lazy val joinedWindowStream = rawStream ~ opt(joinType)
  lazy val joinType: PackratParser[Any] =  crossJoin | qualifiedJoin

  lazy val crossJoin = ("cross".i ~> "join".i ~> optParens(streamReference)) 
  
  lazy val qualifiedJoin = ("left".i ~> "join".i ~> optParens(streamReference) ~ opt(joinSpec)) 

  lazy val joinSpec :PackratParser[Any] = conditionJoin | namedColumnsJoin
  lazy val conditionJoin = "on".i ~> predicate  
  lazy val namedColumnsJoin = "using".i ~> ident 

  /**
   *  CLAUSE: WHERE
   */
  lazy val whereClause = "where".i ~> predicate 

  lazy val predicate: PackratParser[Any] = (simplePredicate | parens | notPredicate) * (
    "and".i ^^^ { (p1: Any, p2: Any) => (p1,"And", p2)}
      | "or".i ^^^ { (p1: Any, p2: Any) => (p1,"Or", p2)}
    )

  lazy val parens: PackratParser[Any] = "(" ~> predicate <~ ")"
  lazy val notPredicate: PackratParser[Any] = "not".i ~> predicate

  lazy val simplePredicate: PackratParser[Any] = (
    expr ~ "=" ~ expr 
      | expr ~ "!=" ~ expr 
      | expr ~ "<>" ~ expr 
      | expr ~ "<" ~ expr 
      | expr ~ ">" ~ expr 
      | expr ~ "<=" ~ expr
      | expr ~ ">=" ~ expr
      | expr ~ "like".i ~ expr
      | expr ~ "in".i ~ exprs 
      | expr ~ "not".i ~ "in".i ~ exprs
      | expr ~ "between".i ~ expr ~ "and".i ~ expr
      | expr ~ "not".i ~ "between".i ~ expr ~ "and".i ~ expr
      | expr <~ "is".i ~ "null".i 
      | expr <~ "is".i ~ "not".i ~ "null".i
    )

  // function
  lazy val functionExpr : PackratParser[Any] =
    ident ~ "(" ~ repsep(expr, ",") ~ ")" 

  // case
  lazy val caseExpr = "case".i ~> rep(caseCondition) ~ opt(caseElse) <~ "end".i  

  lazy val caseCondition = "when".i ~> predicate ~ "then".i ~ expr ^^ {
    case p ~ _ ~ e => (p , e)
  }

  lazy val caseElse = "else".i ~> expr

  /**
   *  CLAUSE: GROUPBY
   *  Not supported yet: with rollup, collate
   */
  lazy val groupBy = "group".i ~> "by".i ~> rep1sep(column, ",")  


  /**
   *  STATEMENT : INSERT 
   * */
  lazy val insertStmtSyntax : PackratParser[Any]= "insert".i ~> "into".i ~> ident  ~ derived_source

  lazy val colNames = "(" ~> repsep(ident, ",") <~ ")"
  //  lazy val selectValue = optParens(selectStmtSyntax) ^^ SelectedInput.apply
  //  lazy val sourceStream = stream ^^ MergedStream.apply


  /**
   * * STATEMENT : MERGE 
   */

  lazy val MergeStmtSyntax : PackratParser[Any]= "merge".i ~> ident ~ "," ~ rep1sep(ident, ",")



  /**
   *  STATEMENT: SPLIT
   */
  lazy val splitStmtSyntax : PackratParser[Any] = "on" ~> ident ~ rep1sep(insertStmtSyntax, ";")
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

      "window".i | "schema".i|
      "every".i| "size".i| "partitioned".i |
      "cross".i | "join".i | "left".i
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

  def printSelectParser = parseAll(selectStmtSyntax, "select s.Quantity * Price, StockTick.Symbol from StockTick as s") match {
    case Success(r, n) => println(r)
    case e => e
  }
  
  
}

object Test2 extends FsqlParser {

  //def parser: (FsqlParser, String) => ?[Ast.Statement[Option[String]]] = (p: FsqlParser, s: String) => p.parseAllWith(p.stmt, s)

  def main(args: Array[String]) {

    /*
        println(parseAllWith(stmt, " select (age + p.hight) * 2 from person p where age >3 and hight <1 or weight = 2"))
        println("-------" * 10)
        */

    println("#########" * 10)

    val queries = Array (
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
      "select id, s.speed, stream.time from stream [size 3]as s cross join stream2[size 3]",
      "select id, s.speed, stream.time from stream [size 3]as s cross join stream2[size 3]",
      "select id from stream [size 3] as s1 left join suoi [size 3 partitioned on s] as s2 on s1.time=s2.thoigian" ,
      "select id from stream [size 3] as s1 left join suoi [size 3] as s2 on s1.time=s2.thoigian",
      "select id from (select p.id as id from oldStream2 as p) [size 3 partitioned on s] as q",
      "select id from stream [size 3] as s1 left join suoi [size 3] as s2 on s1.time=s2.thoigian",
      "Select Count(*) From Bid[Size 1] Where  item_id <= 200 or item_id >= 100",
      "select count(price) from (select plate , price from CarStream)[Size 1] as c",
      "select count(price) from (select plate , price  from CarStream [Size 1]) as c",
      "select * from (select plate , price from (select plate , price from (select plate , price from CarStream [Size 1] ) as e ) as d ) as c",
      "select c.plate + 1000/2.0 from (select plate as pr from (select plate , price + 1 as pr from CarStream) as d) as c",
      "Select count(*) From (Select * From Bid Where item_id >= 100 and item_id <= 200) [Size 1] p",
      "Select count(*) From (Select * From Bid Where item_id >= 100 and item_id <= 200) [Size 1] p",
      "select * from stream"

    )

    printSelectParser
    
  }
}