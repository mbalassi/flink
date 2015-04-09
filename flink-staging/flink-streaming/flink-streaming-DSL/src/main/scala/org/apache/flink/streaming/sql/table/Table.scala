package org.apache.flink.streaming.sql.table


case class Table[A <: TableTranslator](
                                        private[flink] val operation: Operation,
                                        private[flink] val operationTranslator: A) {
  
  
  
  
}