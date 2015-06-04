package org.apache.flink.streaming.experimental

/**
 * Created by kidio on 04/06/15.
 */
case class Person(name: String, age: Int)

object ToTupleTest extends App{
  val person = Person("duy", 12)
  
  def toTuple[Z: ({type ToTuple_[Z] = ToTuple[Z,T]})#ToTuple_, T](z: Z) =
    implicitly[ToTuple[Z,T]].toTuple(z)

  //println(toTuple(person))
  
}
