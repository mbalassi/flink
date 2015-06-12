package org.apache.flink.streaming.fsql

import java.io.FileWriter

private[fsql] class Timer(enabled: Boolean) {
  val fw = scala.tools.nsc.io.File("/tmp/FlinkCQL_test.txt")
  def apply[A](msg: => String, indent: Int, a: => A) =
    if (enabled) {
      val start = System.currentTimeMillis
      fw.appendAll((" " * indent) + msg)
      val aa = a
      fw.appendAll((" " * indent) + (System.currentTimeMillis - start) + "ms\n")
      aa
    } else a
}

private[fsql] object Timer {
  def apply(enabled: Boolean) = new Timer(enabled)
}
