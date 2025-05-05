package org.apache.spark.shuffle.weave.io

import org.apache.spark.shuffle.weave.codec.ProtobufBatchEncoder
import org.apache.spark.shuffle.weave.registry.WeaveRegistry
import org.sparkweave.schema.WeaveRecord

import java.io.OutputStream
import java.net.Socket
import scala.collection.mutable

class BaselineWriterTCP(
    numReducers: Int,
    keyToReducer: WeaveRecord => Int
) {

  private val buffers = Array.fill(numReducers)(mutable.Buffer.empty[WeaveRecord])

  def write(record: WeaveRecord): Unit = {
    val reducerId = keyToReducer(record) % numReducers
    buffers(reducerId) += record
  }

  def flush(): Unit = {
    buffers.zipWithIndex.foreach { case (records, reducerId) =>
      // 🔁 Wait and retry until reducer is registered
      var address: Option[(String, Int)] = None
      var attempts = 0
      while (address.isEmpty && attempts < 100) {
        address = WeaveRegistry.lookup(reducerId)
        if (address.isEmpty) {
          Thread.sleep(50)
          attempts += 1
        }
      }

      val (host, port) = address.getOrElse {
        throw new IllegalStateException(s"Reducer $reducerId not registered after waiting.")
      }

      val socket = new Socket(host, port)
      val out: OutputStream = socket.getOutputStream
      ProtobufBatchEncoder.writeBatch(records.toSeq, out)
      out.flush()
      out.close()
      socket.close()
    }
  }
}
