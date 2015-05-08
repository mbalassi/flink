package org.apache.flink.fsql

private[fsql] class Timer(enabled: Boolean) {
  def apply[A](msg: => String, indent: Int, a: => A) =
    if (enabled) {
      val start = System.currentTimeMillis
      println((" " * indent) + msg)
      val aa = a
      println((" " * indent) + (System.currentTimeMillis - start) + "ms")
      aa
    } else a
}

private[fsql] object Timer {
  def apply(enabled: Boolean) = new Timer(enabled)
}
