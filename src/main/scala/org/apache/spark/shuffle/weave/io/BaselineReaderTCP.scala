package org.apache.spark.shuffle.weave.io

import org.sparkweave.schema.WeaveRecord
import org.apache.spark.shuffle.weave.codec.ProtobufBatchDecoder
import org.apache.spark.shuffle.weave.registry.WeaveRegistry

import java.io.InputStream
import java.net.{ServerSocket, Socket}
import scala.collection.mutable

class BaselineReaderTCP(
    reduceId: Int,
    expectedMappers: Int
) {

  def readAll(): Seq[WeaveRecord] = {
    // Start a server socket with an OS-assigned port
    val serverSocket = new ServerSocket(0)
    val port = serverSocket.getLocalPort

    // Register this reducer’s address in the central registry
    WeaveRegistry.register(reduceId, "localhost", port)
    println(s"[Reducer $reduceId] Listening on port $port for $expectedMappers mappers.")

    val allRecords = mutable.Buffer.empty[WeaveRecord]

    for (_ <- 0 until expectedMappers) {
      val socket: Socket = serverSocket.accept()
      val in: InputStream = socket.getInputStream
      val records = ProtobufBatchDecoder.readBatch(in)
      allRecords ++= records
      in.close()
      socket.close()
    }

    serverSocket.close()
    allRecords.toSeq
  }
}
