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

package org.apache.flink.streaming.scala.examples.windowing

import java.util.concurrent.TimeUnit._

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.windowing.{Delta, Time}

import scala.Stream._
import scala.math._
import scala.language.postfixOps
import scala.util.Random

/**
 * An example of grouped stream windowing where different eviction and 
 * trigger policies can be used. A source fetches events from cars 
 * every 1 sec containing their id, their current speed (kmh),
 * overall elapsed distance (m) and a timestamp. The streaming
 * example triggers the top speed of each car every x meters elapsed 
 * for the last y seconds.
 */
object MyCar {

  case class CarEvent( speed: Int, distance: Double, time: Long) extends Serializable

  def main(args: Array[String]) {
    

    val cars = genCarStream()
      .window(Time.of(5, SECONDS))
      .every(Time.of(1,SECONDS))
      .sum("distance")
    
    cars.flatten print

    StreamExecutionEnvironment.getExecutionEnvironment.execute("TopSpeedWindowing")

  }

  def genCarStream(): DataStream[CarEvent] = {

    def nextSpeed(carEvent : CarEvent, delay : Long) : CarEvent =
    {
      Thread.sleep(delay * 1000)
      val next =
        if (Random.nextBoolean) min(100, carEvent.speed + 5) else max(0, carEvent.speed - 5)
      CarEvent( next, delay,System.currentTimeMillis )
    }
   
    val carEvent = CarEvent(50,0,System.currentTimeMillis())
    Seq(nextSpeed(carEvent, 0),nextSpeed(carEvent, 1), nextSpeed(carEvent,5),nextSpeed(carEvent, 0),nextSpeed(carEvent, 1), nextSpeed(carEvent,5)).toStream
  }



  var numOfCars = 1
  var evictionSec = 5
  var triggerMeters = 50d

}
