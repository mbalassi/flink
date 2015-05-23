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

    val cars = genCarStream()//.reduce((e1,e2)=>new CarEvent(e1.speed+e2.speed,e1.time+e2.time))
      .map(c => (c.speed + c.time))
      .window(Time.of(10, ts, 0 ))
      //.every(Time.of(1, MICROSECONDS))
      //.mapWindow(mapFun).flatten
      //.reduceWindow((e1,e2)=>new CarEvent(e1.speed+e2.speed,e1.time)).flatten()
     /* .window(Count.of(3))
      .every(Time.of(10, ts ))
      .mapWindow(mapFun).flatten*/
    cars print

    StreamExecutionEnvironment.getExecutionEnvironment.execute("TopSpeedWindowing")

  }

  def genCarStream(): DataStream[CarEvent] = {
    //Seq(CarEvent(10,30),CarEvent(20,31),CarEvent(30,36),CarEvent(30,36),CarEvent(30,36),CarEvent(30,36)).toStream//, CarEvent(40,13),CarEvent(50,14),CarEvent(60,15),CarEvent(70,16)).toStream//,CarEvent(80,20),CarEvent(1,20)).toStream//,CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream

    //Examp: 1 Seq(CarEvent(10,10),CarEvent(20,11),CarEvent(30,12), CarEvent(40,13),CarEvent(50,14),CarEvent(60,15),CarEvent(70,16)).toStream//,CarEvent(80,20),CarEvent(1,20)).toStream//,CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream
    // Close End window: Seq(CarEvent(1,1),CarEvent(2,9),CarEvent(4,9), CarEvent(8,10),CarEvent(20,19),CarEvent(80,20),CarEvent(1,20),CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream
    Seq(CarEvent(1,6),CarEvent(4,11), CarEvent(20,13),CarEvent(80,14),CarEvent(100,18),CarEvent(1000,22),CarEvent(9,25),CarEvent(500,34),CarEvent(1,39),CarEvent(1,50)).toStream//,CarEvent(500,29),CarEvent(1000,39),CarEvent(2000,55)).toStream

  }

  
  var numOfCars = 1
  var evictionSec = 5
  var triggerMeters = 50d
}
