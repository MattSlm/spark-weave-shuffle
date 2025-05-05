package org.apache.spark.shuffle.weave.registry

import java.util.concurrent.ConcurrentHashMap

object WeaveRegistry {
  private val reducerAddresses = new ConcurrentHashMap[Int, (String, Int)]()

  def registerAddress(bin: Int, host: String, port: Int): Unit = {
    reducerAddresses.put(bin, (host, port))
  }

  def lookup(bin: Int): Option[(String, Int)] = {
    Option(reducerAddresses.get(bin))
  }

  def clear(): Unit = {
    reducerAddresses.clear()
  }
}
