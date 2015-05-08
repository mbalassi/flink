package org.apache.flink.streaming

package object fsql {
//  import scala.language.experimental.macros
//  import scala.language.implicitConversions

  
  
  // a.ok -> Ok(a)
  private[fsql] implicit class ResultOps[A](a: A)  {
    def ok = fsql.ok(a)
  }
  //  Option[A].orFail -> ?[A]
  private [fsql] implicit class ResultOptionOps[A](a: Option[A]){
    def orFail(s: => String) = a map fsql.ok getOrElse fail(s)
  }
  
  

  
  // create new ?[A] object
  private[fsql] def fail[A](s: String, column: Int = 0, line: Int = 0): ?[A] = {
    fsql.Failure(s, column, line)
  }
  private[fsql] def ok[A](a: A): ?[A] = 
    fsql.Ok(a)

  
  
  // accumulate
  private[fsql] def sequence[A](rs: List[?[A]]): ?[List[A]] =
    rs.foldRight(List[A]().ok) {
      (ra, ras) => for {
        as <- ras;
        a <- ra
      } yield a :: as
    }


  
  def sequenceX[A](rs: List[Option[A]]): Option[List[A]] =
    rs.foldRight(Option(List[A]())) {
      (ra, ras) => for {
        as <- ras;
        a <- ra
      } yield (a :: as)
    }
  
  
  private[fsql] def sequenceO[A](rs: Option[?[A]]): ?[Option[A]] =
    rs.foldRight(None.ok: ?[Option[A]]) {
      (ra, _) => for {
        a <- ra
      } yield Some(a)
    }

}


package fsql{
/**
 * * ? class : whether the result is successful or fail
 * *  monads style 
 */
  
  private[fsql] abstract sealed class ?[+A] { self =>
    def map[B](f: A => B): ?[B]
    def flatMap[B](f: A => ?[B]): ?[B]
    def foreach[U](f: A => U): Unit
    def fold[B](ifFail: Failure[A] => B, f: A=> B): B
    def filter(p: A => Boolean): ?[A]
    def getOrElse[B >: A](default: => B): B
  def withFilter(p: A => Boolean): WithFilter = new WithFilter(p)
    class WithFilter(p: A => Boolean){
      def map[B](f: A => B ): ?[B] = self filter p map f
      def flatMap[B](f: A => ?[B]): ?[B]  = self filter p flatMap f
      def foreach[U](f: A => U): Unit = self filter p foreach f
      def withFilter(q: A => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
    }
  
  }
  
  private[fsql] final case class Ok[+A](a: A) extends ?[A] {
    def map[B](f: (A) => B): ?[B] = Ok(f(a))

    def filter(p: (A) => Boolean): ?[A] = if(p(a)) this else fail("filter on ?[_] failed")

    def flatMap[B](f: (A) => ?[B]): ?[B] = f(a)

    def fold[B](ifFail: (Failure[A]) => B, f: (A) => B): B = f(a)

    def foreach[U](f: (A) => U): Unit = {f(a); ()}

    def getOrElse[B >: A](default: => B): B = a
  }
  
  private[fsql] final case class Failure[+A](message: String, column: Int, line: Int) extends ?[A]{
    def map[B](f: (A) => B): ?[B] = Failure(message, column, line)

    def filter(p: (A) => Boolean): ?[A] = this

    def flatMap[B](f: (A) => ?[B]): ?[B] = Failure(message, column, line)

    def fold[B](ifFail: (Failure[A]) => B, f: (A) => B): B = ifFail(this)

    def foreach[U](f: (A) => U): Unit = ()

    def getOrElse[B >: A](default: => B): B = default
  }
  
}