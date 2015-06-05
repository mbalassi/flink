package org.apache.flink.streaming.experimental

/**
 * Created by kidio on 04/06/15.
 */
object TestArrayMacro {

  case class Car (speed: Int, time: String)
  def main(args: Array[String]) {
    //def mapify2[Z: ArrMappable2,T](t: Z) = implicitly[ArrMappable2[Z,T]].toZuple(t)

    
    def toTuple[T: ArrMappable2](z: T) = implicitly[ArrMappable2[T]].toTuple(z)
    
    println(toTuple(Car(1,"2")).getClass)
  }

}
