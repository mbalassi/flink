package org.apache.flink.streaming.sql.table

import org.apache.flink.api.common.typeinfo.TypeInformation


sealed abstract class Operation {
  def outputFields: Seq[(String, TypeInformation[_])]
}

case class Root[T](input: T, outputFields: Seq[(String, TypeInformation[_])]) extends Operation
