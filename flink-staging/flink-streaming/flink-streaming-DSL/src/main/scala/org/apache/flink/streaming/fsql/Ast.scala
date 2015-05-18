package org.apache.flink.streaming.fsql

import scala.reflect.runtime.universe._

private[fsql] object Ast {

  /**
   * *  UNRESOLVED
   */

  trait Unresolved {
    type Expr = Ast.Expr[Option[String]]
    type Named  = Ast.Predicate[Option[String]]
    type Statement = Ast.Statement[Option[String]]
    type Source = Ast.Source[Option[String]]
    //type WindowedStream = Ast.WindowedStream[Option[String]]
    type Select = Ast.Select[Option[String]]
    type Predicate = Ast.Predicate[Option[String]]
    type PolicyBased = Ast.PolicyBased[Option[String]]
    type Where = Ast.Where[Option[String]]
    type StreamReference = Ast.StreamReferences[Option[String]]
    type ConcreteStream  = Ast.ConcreteStream[Option[String]]
    type DerivedStream  = Ast.DerivedStream[Option[String]]
    type Join           = Ast.Join[Option[String]]
    type JoinSpec       = Ast.JoinSpec[Option[String]]
    type Function       = Ast.Function[Option[String]]

  }
  object Unresolved extends  Unresolved

  /**
   *  STATEMENT
   * @tparam T
   */

  sealed trait Statement[T]{
    def streams : List[Stream]
    def isQuery = false
  }

          /**
           *  CREATE A NEW SCHEMA
           * * @tparam T
           */
  sealed trait newSchema
  case class anonymousSchema(value: List[StructField])  extends newSchema
  case class namedSchema (name : String) extends newSchema

  case class CreateSchema[T](s: String, schema: Schema, parentSchema: Option[String]) extends Statement[T] {
    def streams = Nil
  }


  case class StructField(
                             name : String,
                             dataType: String,
                             nullable: Boolean = true) {

    //override def toString : String = s"StructField($name, ${dataType.scalaType}, $nullable )"

  }
  case class Schema(name: Option[String], fields: List[StructField])


          /**
           *  CREATE A NEW STREAM
           * */

  // create a new stream
  case class CreateStream[T](name : String, schema : Schema, source: Option[Source[T]]) extends Statement[T] {
       def streams = Stream(name,None) :: (source.fold(List[Stream]())(s => s.streams))
  }

  sealed  trait Source[T] {
    def streams: List[Stream]
  }
  case class HostSource[T](host: String, port : Int) extends Source[T] {
    def streams = Nil
    
  }
  case class FileSource[T](fileName: String) extends Source[T] {
    def streams = Nil
    
  }
  case class DerivedSource[T](subSelect: Select[T]) extends Source[T]{
    def streams = subSelect.streams
  }
  
  
  sealed trait StreamReferences[T]{

    def streams :List[Stream]
    def name: String
  }
  case class ConcreteStream[T] (stream: Stream, windowSpec: Option[WindowSpec[T]], join: Option[Join[T]]) extends  StreamReferences[T]{
    def streams = stream :: join.fold(List[Stream]())(_.stream.streams)
    def name = stream.name
    
  }
  
  case class DerivedStream[T] (name : String, windowSpec: Option[WindowSpec[T]],subSelect: Select[T], join: Option[Join[T]]) extends StreamReferences[T]{
    def streams = Stream(name, None) :: join.fold(List[Stream]())(_.stream.streams)
    
  }
  
  case class Stream(name : String, alias: Option[String])
  //case class WindowedStream[T](stream: Stream, windowSpec: Option[WindowSpec[T]])
  case class Named[T](name: String, alias: Option[String], expr: Expr[T]){
    def aliasName = alias getOrElse name
  }

          /**
           *    SELECT
           * */

  case class Select[T](projection: List[Named[T]] ,
                       streamReference: StreamReferences[T],
                       where: Option[Where[T]],
                       groupBy: Option[GroupBy[T]]
                      ) extends Statement[T] {
            
    def streams = streamReference.streams
    override def isQuery = true
  }
  
  case class Where[T](predicate: Predicate[T])

          /**
           *  WINDOW
           */
  
  case class WindowSpec[T](window: Window[T], every: Option[Every[T]], partition: Option[Partition[T]])
  case class Window[T](policyBased: PolicyBased[T])
  case class Every[T](policyBased: PolicyBased[T])
  case class Partition[T](field: Column[T])
  case class PolicyBased[T] (value: Int, timeUnit: Option[String], onField: Option[Column[T]])


          /**
           * * JOIN
           */
  
  case class Join[T] (stream: StreamReferences[T], joinSpec: Option[JoinSpec[T]], joinDesc: JoinDesc) {}
  
  sealed trait JoinDesc
  case object Cross extends JoinDesc
  case object LeftOuter extends JoinDesc
  
  sealed trait JoinSpec[T]
  case class NamedColumnJoin[T] (columns: String) extends JoinSpec[T]
  case class QualifiedJoin[T](predicate: Predicate[T]) extends JoinSpec[T]
  

          /**
           * * EXPRESSION
           */
  // Expression (previously : Term)
  sealed trait Expr[T]
  case class Constant[T](tpe: (Type, Int), value : Any) extends Expr[T]
  case class Column[T](name: String, stream : T) extends Expr[T]
  case class AllColumns[T](schema: T) extends Expr[T]
  case class Function[T](name: String, params:List[Expr[T]]) extends Expr[T]
  case class ArithExpr[T](lhs:Expr[T], op: String, rhs:Expr[T]) extends Expr[T]
  case class Input[T]() extends Expr[T]
  case class SubSelect[T](select: Select[T]) extends Expr[T]
  case class ExprList[T](exprs: List[Expr[T]]) extends Expr[T]
  case class Case[T](conditions: List[(Predicate[T], Expr[T])], elze: Option[Expr[T]]) extends Expr[T]


          /**
           * * OPERATOR
           */
  // Operator
  sealed trait Operator1
  case object IsNull extends Operator1
  case object IsNotNull extends Operator1
  case object Exists extends Operator1
  case object NotExists extends Operator1

  sealed trait Operator2
  case object Eq extends Operator2
  case object Neq extends Operator2
  case object Lt extends Operator2
  case object Gt extends Operator2
  case object Le extends Operator2
  case object Ge extends Operator2
  case object In extends Operator2
  case object NotIn extends Operator2
  case object Like extends Operator2

  sealed trait Operator3
  case object Between extends Operator3
  case object NotBetween extends Operator3

          /**
           * * PREDICATE
           */
  // Predicate
  sealed trait Predicate[T] {
    def find(p: Predicate[T] => Boolean): Option[Predicate[T]] = {
      if (p(this)) Some(this)
      else 
        this match {
          case And(e1, e2) => e1.find(p) orElse e2.find(p)
          case Or(e1, e2)  => e1.find(p) orElse e2.find(p)
          case _ => None
        }
    }
  }
  
  case class And[T](p1: Predicate[T], p2:Predicate[T]) extends  Predicate[T]
  case class Or[T](p1: Predicate[T], p2:Predicate[T]) extends  Predicate[T]
  case class Not[T](p: Predicate[T]) extends Predicate[T]
  
  sealed trait SimplePredicate[T] extends Predicate[T]
  case class Comparison0[T](boolExpr: Expr[T]) extends SimplePredicate[T]
  case class Comparison1[T](expr: Expr[T], op: Operator1) extends SimplePredicate[T]
  case class Comparison2[T](lhs: Expr[T], op: Operator2, rhs: Expr[T]) extends SimplePredicate[T]
  case class Comparison3[T](t: Expr[T], op: Operator3, value1: Expr[T], value2: Expr[T]) extends SimplePredicate[T]

  
  case class GroupBy[T](exprs: List[Expr[T]], having: Option[Having[T]])
  case class Having[T](predicate: Predicate[T])


  /**
   *  INSERT 
   */
  
  //case class  Insert[T](stream: WindowedStream[T], colNames: Option[List[String]], source: Source[T])



  /**************************************************************************************************
   * * 
   *                            RESOLVE
   * * 
   * * *************************************************************************************/

  trait Resolved {
    type Expr = Ast.Expr[Stream]
    type Named  = Ast.Predicate[Stream]
    type Statement = Ast.Statement[Stream]
    type Source = Ast.Source[Stream]
    //type WindowedStream = Ast.WindowedStream[Schema]
    type Select = Ast.Select[Stream]
    type Predicate = Ast.Predicate[Stream]
    type PolicyBased = Ast.PolicyBased[Stream]
    type Where = Ast.Where[Stream]
    type StreamReference = Ast.StreamReferences[Stream]
    type ConcreteStream  = Ast.ConcreteStream[Stream]
    type DerivedStream  = Ast.DerivedStream[Stream]
    type Join           = Ast.Join[Stream]
    type JoinSpec       = Ast.JoinSpec[Stream]
    type Function       = Ast.Function[Stream]
  }
  object Resolved extends  Resolved
  
  

  def resolvedStreams(stmt : Statement[Option[String]]): ?[Statement[Stream]]
  = stmt match {
    case s@Select(_,_,_,_) => resolveSelect(s)(stmt.streams)
    case CreateSchema(s,schema,p) => CreateSchema[Stream](s,schema,p).ok
    case cs@CreateStream(n,schema,source) => resolveCreateStream(cs)()
  }
  
  def resolveSelect (select: Select[Option[String]])(env: List[Stream] = select.streams)= {
    val r = new ResolveEnv(env)
    for {
      p <- r.resolveProj(select.projection)
      s <- r.resolveStreamRef(select.streamReference)
      w <- r.resolveWhereOpt(select.where)
      g <- r.resolveGroupByOpt(select.groupBy)
    } yield select.copy(projection =  p, streamReference =  s, where = w, groupBy = g)
  }
  
  def resolveCreateStream(createStream : CreateStream[Option[String]])(env: List[Stream] = createStream.streams)  = {
    val r = new ResolveEnv(env)
    resolveSourceOpt(createStream.source) map (s => createStream.copy(source = s))
  }
  
  def resolveSourceOpt (sourceOpt: Option[Source[Option[String]]])= 
    sequenceO(sourceOpt map resolveSource)
  
  def resolveSource(source: Source[Option[String]])  = source match{
    case host@HostSource(h,p) => HostSource[Stream](h,p).ok
    case file@FileSource(path) => FileSource[Stream](path).ok
    case d@DerivedSource(s)   => resolveSelect(s)() map ( s => DerivedSource(s))
  }
  
 
  private class ResolveEnv (env : List[Stream]) {

    // Basic Elements
    def resolve(expr: Expr[Option[String]]): ?[Expr[Stream]] = expr match {
      case c@Column(_, _) => resolveColumn(c)
      case AllColumns(s) => resolveAllColumns(s)
      case (f@Function(_, ps)) => resolveFunc(f)
      case ArithExpr(lhs, op, rhs) =>
        for (l <- resolve(lhs); r <- resolve(rhs)) yield ArithExpr(l, op, r)
      case Constant(tpe, value) => Constant[Stream](tpe, value).ok
      case (c@Case(_, _)) => resolveCase(c)
      case Input() => Input[Stream]().ok


    }

    def resolveNamed(n: Named[Option[String]]): ?[Named[Stream]] =
      resolve(n.expr) map (e => n.copy(expr = e))

    /*def resolveColumn(col: Column[Option[String]]  , streamName :String): ?[Column[Stream]] = {
      env find (_.name == streamName) map (s => col.copy(stream = s)) orFail ("Column references unknown")
    }*/

    def resolveColumn(col: Column[Option[String]])(implicit streamName: String = ""): ?[Column[Stream]] = {

      env find {
        s =>
          (col.stream, s.alias) match {
            case (Some(ref), None) => println(ref, s.name, ref == s.name); ref == s.name
            case (Some(ref), Some(alias)) => (ref == s.name) || (ref == alias)
            case (None, _) => true // assume that we take the first stream
            // if there are more than 1, not working
          }
      } map (s => col.copy(stream = s)) orFail ("Column references unknown")
    }

    def resolveAllColumns(streamRef: Option[String]) = streamRef match {
      case Some(ref) =>
        (env.find(s => ref == s.name || s.alias.map(_ == ref).getOrElse(false)) orFail ("Unknown stream '" + ref + "'")) map (AllColumns(_))
      case None => AllColumns(env.head).ok
    }

    def resolveCase(c: Case[Option[String]]) = for {
      predicates <- sequence(c.conditions map { case (x, _) => resolvePredicate(x)})
      results <- sequence(c.conditions map { case (_, x) => resolve(x)})
      elze <- sequenceO(c.elze map resolve)
    } yield Case(predicates zip results, elze)

    def resolveFunc(f: Function[Option[String]]) =
      sequence(f.params map resolve) map (ps => f.copy(params = ps))

    def resolvePredicate(p: Predicate[Option[String]]): ?[Predicate[Stream]] = p match {
      /*
        case class And[T](p1: Predicate[T], p2:Predicate[T]) extends  Predicate[T]
        case class Or[T](p1: Predicate[T], p2:Predicate[T]) extends  Predicate[T]
        case class Not[T](p: Predicate[T]) extends Predicate[T]
  
        sealed trait SimplePredicate[T] extends Predicate[T]
        case class Comaprison0[T](boolExpr: Expr[T]) extends SimplePredicate[T]
        case class Comparison1[T](expr: Expr[T], op: Operator1) extends SimplePredicate[T]
        case class Comparison2[T](lhs: Expr[T], op: Operator2, rhs: Expr[T]) extends SimplePredicate[T]
        case class Comparison3[T](t: Expr[T], op: Operator3, value1: Expr[T], value2: Expr[T]) extends SimplePredicate[T]
      */
      case simplePre: SimplePredicate[Option[String]] => resolveSimplePredicate(simplePre)
      case And(e1, e2) =>
        for {r1 <- resolvePredicate(e1); r2 <- resolvePredicate(e2)} yield And(r1, r2)
      case Or(e1, e2) =>
        for {r1 <- resolvePredicate(e1); r2 <- resolvePredicate(e2)} yield Or(r1, r2)
      case Not(p) => for {r <- resolvePredicate(p)} yield Not(r)
    }

    def resolveSimplePredicate(predicate: SimplePredicate[Option[String]]): ?[Ast.Predicate[Ast.Stream]] = predicate match {
      case p@Comparison0(b) => resolve(b) map (b => p.copy(boolExpr = b))
      case p@Comparison1(e1, op) =>
        resolve(e1) map (e => p.copy(expr = e))
      case p@Comparison2(e1, op, e2) =>
        for {l <- resolve(e1); r <- resolve(e2)} yield p.copy(lhs = l, rhs = r)
      case p@Comparison3(e1, op, e2, e3) =>
        for {r1 <- resolve(e1); r2 <- resolve(e2); r3 <- resolve(e3)} yield p.copy(t = r1, value1 = r2, value2 = r3)
    }


    //projection
    def resolveProj(proj: List[Named[Option[String]]]): ?[List[Named[Stream]]]
    = sequence(proj map resolveNamed)


    //StreamReference
    def resolveStreamRef(streamRefs: StreamReferences[Option[String]]) : ?[StreamReferences[Stream]]= streamRefs match {
      case c@ConcreteStream(stream,windowSpec, join) => for {
        ws <- resolveWindowSpec(windowSpec,stream)
        j <- sequenceO(join map resolveJoin)
      } yield c.copy(windowSpec = ws, join = j)
      

      case d@DerivedStream(name,windowSpec, select, join) => for{
        s <- resolveSelect(select)()
        ws <- resolveWindowSpec(windowSpec,Stream(name,None)) // TODO: this Stream is derived
        j <- sequenceO(join map resolveJoin)
      } yield d.copy(subSelect = s,join = j, windowSpec = ws)

    }


    //resolveWindowedStream
//    def resolveWindowedStream(windowedStream: WindowedStream[Option[String]]): ?[WindowedStream[Stream]] = {
//      val thisStream: Stream = windowedStream.stream
//      // resolveWindowedSpec
      /*def resolveWindowedSpec( windowedStream : WindowedStream[Option[String]]) : ?[Option[WindowSpec[Stream]]] =
        windowedStream.windowSpec.fold((None.ok: ?[Option[WindowSpec[Stream]]])) {
          spec => for {
            w <- resolveWindowing(spec.window)
            e <- resolveEvery(spec.every)
            p <- resolvePartition(spec.partition)

          } yield Some(spec.copy(window = w, every =  e , partition =  p))
    }*/
      def resolveWindowSpec(winSpec: Option[WindowSpec[Option[String]]], thisStream: Stream): ?[Option[WindowSpec[Stream]]] = {

        def resolvePolicyBased(based: PolicyBased[Option[String]]): ?[PolicyBased[Stream]] =
          sequenceO(based.onField map { f => resolveColumn(f)(thisStream.name)}) map (o => based.copy(onField = o))

        def resolveWindowing(window: Window[Option[String]]): ?[Window[Stream]] = {
          resolvePolicyBased(window.policyBased) map (p => window.copy(policyBased = p))
        }

        def resolveEvery(maybeEvery: Option[Every[Option[String]]]): ?[Option[Every[Stream]]] =
          sequenceO(maybeEvery map { e => resolvePolicyBased(e.policyBased) map (p => e.copy(policyBased = p))})

        def resolvePartition(maybePartition: Option[Partition[Option[String]]]): ?[Option[Partition[Stream]]] =
          sequenceO(maybePartition map { p => resolveColumn(p.field)(thisStream.name) map Partition.apply})


      sequenceO(winSpec map {
          spec => for {
            w <- resolveWindowing(spec.window)
            e <- resolveEvery(spec.every)
            p <- resolvePartition(spec.partition)
          } yield (spec.copy(window = w, every = e, partition = p))
        }
        )
      }

      // end  resolveWindowedSpec

      //.resolveWindowSpec(windowedStream.windowSpec) map (spec => windowedStream.copy(windowSpec = spec))

    //} // end resolveWindowedStream

    //resolveJoin
    def resolveJoin(join: Join[Option[String]]): ?[Join[Stream]] = {
      for {
        s <- resolveStreamRef(join.stream)
        j <- sequenceO(join.joinSpec map resolveJoinSpec)
      } yield join.copy(stream = s, joinSpec = j)
      
    }

    def resolveJoinSpec(spec: JoinSpec[Option[String]]): ?[JoinSpec[Stream]] = spec match {
      case NamedColumnJoin(col) => NamedColumnJoin[Stream](col).ok
      case QualifiedJoin(p) => resolvePredicate(p) map {pre => QualifiedJoin[Stream](pre)}
    }
    
    // where
    def resolveWhereOpt(where : Option[Where[Option[String]]]) = sequenceO( where map resolveWhere)
    def resolveWhere(where: Where[Option[String]]) = resolvePredicate(where.predicate) map Where.apply
    
    //groupBy
    def resolveGroupBy(groupBy: GroupBy[Option[String]]) = for {
      t <- sequence(groupBy.exprs map resolve)
      h <- resolveHavingOpt(groupBy.having)
    } yield groupBy.copy(exprs = t, having = h)
    def resolveGroupByOpt(groupBy: Option[GroupBy[Option[String]]]) = sequenceO(groupBy map resolveGroupBy)

    def resolveHaving(having: Having[Option[String]]) = resolvePredicate(having.predicate) map Having.apply
    def resolveHavingOpt(having: Option[Having[Option[String]]]) = sequenceO(having map resolveHaving)
  }
}




