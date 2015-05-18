package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.helper.Count
import org.apache.flink.streaming.api.scala._

/**
 * Created by kidio on 26/01/15.
 */
object fSlidingExample {
  def main(args: Array[String]) {
    
    if (!parseParameters(args)) {
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /*
		 * SIMPLE-EXAMPLE: Use this to always keep the newest 10 elements in the
		 * buffer Resulting windows will have an overlap of 5 elements
		 */

    /*val dstream = env.fromCollection(generateCounting)
    .window(Count.of(10))
    .every(Count.of(5))
    .reduce(_ + "|" + _)*/

    /*
     * ADVANCED-EXAMPLE: Use this to have the last element of the last
     * window as first element of the next window while the window size is
     * always 5
     */
    
    val dstream = env.fromCollection(generateCounting)
                      .window(Count.of(5).withDelete(4))
                      .every(Count.of(4).startingAt(-1))
                      .reduce(_ + "|" + _)

    // emit result
    if (fileOutput) {
      dstream.writeAsText(outputPath, 1)
    }
    else {
      dstream.print
    }

    env.execute("Sliding Example")
  }

  private def generateCounting (): Stream[String] = {
    Stream.from(0,1).map("V" + _%10000)
  }


  private def parseParameters (args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        outputPath = args(0)
        System.out.println(outputPath)

      }else{
        System.err.println("Usage: SlidingExample <result path>")
        false
      }
    } else {
      System.out.println("Executing SlidingExample with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: SlidingExample <result path>")
    }
    true
  }

  private var fileOutput : Boolean = false
  private var outputPath : String =  null
}
