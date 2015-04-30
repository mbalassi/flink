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

import org.apache.flink.streaming.api.windowing.StreamWindow
import org.apache.flink.streaming.api.windowing.helper._


import java.util.concurrent.TimeUnit._

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.windowing.{Delta, Time}
import org.apache.flink.util.Collector

import scala.Stream._
import scala.math._
import scala.language.postfixOps
import scala.util.Random

object MyCar {

  case class CarEvent( speed: Int, time: Long) extends Serializable

  
  val ts: CarEvent => Long = (x: CarEvent) => x.time
  
  val mapFun : (Iterable[CarEvent],Collector[Double]) => Unit = {
    case x => {
      val count = x._1.toIterator.length.toDouble
      val sum = x._1.toIterator.map(_.speed).sum.toDouble
      x._2.collect(sum)
    }
  }

  def main(args: Array[String]) {

    val cars = genCarStream()
      .window(Time.of(20, ts, 1 ))
      //.every(Time.of(1,ts,2))//.every(Count.of(1))
      .mapWindow(mapFun)

    
    cars print

    StreamExecutionEnvironment.getExecutionEnvironment.execute("TopSpeedWindowing")

  }

  def genCarStream(): DataStream[CarEvent] = {

    Seq(CarEvent(1,1),CarEvent(2,9),CarEvent(4,9), CarEvent(8,10),CarEvent(20,19),CarEvent(80,20),CarEvent(1,20),CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream
    // Close End window: Seq(CarEvent(1,1),CarEvent(2,9),CarEvent(4,9), CarEvent(8,10),CarEvent(20,19),CarEvent(80,20),CarEvent(1,20),CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream

  }

  
  var numOfCars = 1
  var evictionSec = 5
  var triggerMeters = 50d
}
