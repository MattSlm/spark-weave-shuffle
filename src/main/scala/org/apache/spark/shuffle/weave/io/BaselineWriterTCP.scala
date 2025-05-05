package org.apache.spark.shuffle.weave.io

import org.sparkweave.schema.WeaveRecord
import org.apache.spark.shuffle.weave.codec.ProtobufBatchEncoder
import org.apache.spark.shuffle.weave.registry.WeaveRegistry

import scala.collection.mutable
import java.io.OutputStream
import java.net.Socket

class BaselineWriterTCP(
    numReducers: Int,
    keyToReducer: WeaveRecord => Int
) {

  private val buffers = Array.fill(numReducers)(mutable.Buffer.empty[WeaveRecord])

  /** Buffer records by reducer ID */
  def write(record: WeaveRecord): Unit = {
    val reducerId = keyToReducer(record) % numReducers
    buffers(reducerId) += record
  }

  /** Connect to each reducer via TCP and send encoded batches */
  def flush(): Unit = {
    buffers.zipWithIndex.foreach { case (records, reducerId) =>
      WeaveRegistry.lookup(reducerId) match {
        case Some((host, port)) =>
          val socket = new Socket(host, port)
          val out: OutputStream = socket.getOutputStream
          ProtobufBatchEncoder.writeBatch(records.toSeq, out)
          out.flush()
          out.close()
          socket.close()
        case None =>
          throw new IllegalStateException(s"No reducer registered at $reducerId in WeaveRegistry")
      }
    }
  }
}
