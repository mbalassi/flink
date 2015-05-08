package org.apache.flink.streaming.fsql

import scala.collection.mutable


/**
 * An interface for looking up relations by name.
 */
class Catalog {

}


class SimpleCatalog extends  Catalog {
  val tables = new mutable.HashMap[String, String]() // should be logical plan

  
}