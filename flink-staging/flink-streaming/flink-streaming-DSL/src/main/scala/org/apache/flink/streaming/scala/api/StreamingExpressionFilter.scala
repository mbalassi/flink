/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.scala.api

import org.apache.flink.streaming.api.scala._


import scala.Stream._
import scala.math._
import scala.language.postfixOps
import scala.util.Random

/**
 * Simple example for demonstrating the use of the Table API with Flink Streaming.
 */
object StreamingExpressionFilter {

  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long) extends Serializable

  case class Nested(myLong: Long) extends  Serializable

  class Pojo(var myString: String, var myInt: Int, var nested: Nested) extends  Serializable{
    def this() = {
      this("", 0, new Nested(1))
    }

    def this(myString: String, myInt: Int, myLong: Long) { this(myString, myInt, new Nested(myLong)) }

    override def toString = s"myString=$myString myInt=$myInt nested.myLong=${nested.myLong}"
  }


  def testPojoWithNestedCaseClass(): Unit = {
    /*
     * Test pojo with nested case class
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      new Pojo("one", 1, 1L),
      new Pojo("one", 1, 1L),
      new Pojo("two", 666, 2L) )

    val grouped = ds.groupBy("nested.myLong").reduce {
      (p1, p2) =>
        p1.myInt += p2.myInt
        p1
    }
    println(grouped)
    println(grouped.getType())
    env.execute()
  }
  
  
  def testCarStream: Unit = {

    /*val cars = genCarStream().toTable
      .filter('carId === 0)
      .select('carId, 'speed, 'distance + 1000 as 'distance, 'time % 5 as 'time)
      .as[CarEvent]
      
    cars.print()
      */

    val cars = genCarStream().getType()
    println(cars)



    StreamExecutionEnvironment.getExecutionEnvironment.execute("TopSpeedWindowing")


  }
  
  def main(args: Array[String]) {
    testPojoWithNestedCaseClass()
  }

  def genCarStream(): DataStream[CarEvent] = {

    def nextSpeed(carEvent : CarEvent) : CarEvent =
    {
      val next =
        if (Random.nextBoolean()) min(100, carEvent.speed + 5) else max(0, carEvent.speed - 5)
      CarEvent(carEvent.carId, next, carEvent.distance + next/3.6d,System.currentTimeMillis)
    }
    def carStream(speeds : Stream[CarEvent]) : Stream[CarEvent] =
    {
      Thread.sleep(1000)
      speeds.append(carStream(speeds.map(nextSpeed)))
    }
    carStream(range(0, numOfCars).map(CarEvent(_,50,0,System.currentTimeMillis())))
  }

  

  var numOfCars = 2
  var evictionSec = 10
  var triggerMeters = 50d

}




