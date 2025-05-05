package org.apache.spark.shuffle.weave.io

import org.sparkweave.schema.WeaveRecord
import org.apache.spark.shuffle.weave.codec.ProtobufBatchDecoder
import org.apache.spark.shuffle.weave.registry.WeaveRegistry

import java.io.InputStream
import java.net.{InetAddress, ServerSocket, Socket}
import scala.collection.mutable

class BaselineReaderTCP(
    reduceId: Int,
    expectedMappers: Int
) {

  def readAll(): Seq[WeaveRecord] = {
    // Bind to any free port
    val serverSocket = new ServerSocket(0)
    val port = serverSocket.getLocalPort
    val host = InetAddress.getLocalHost.getHostAddress

    // ✅ Register reducer early
    WeaveRegistry.registerAddress(reduceId, host, port)
    println(s"[Reducer $reduceId] Registered at $host:$port and waiting for $expectedMappers mappers.")

    val allRecords = mutable.Buffer.empty[WeaveRecord]

    var received = 0
    while (received < expectedMappers) {
      try {
        val socket: Socket = serverSocket.accept()
        val in: InputStream = socket.getInputStream
        val records = ProtobufBatchDecoder.readBatch(in)
        allRecords ++= records
        in.close()
        socket.close()
        received += 1
        println(s"[Reducer $reduceId] Received from mapper $received/$expectedMappers.")
      } catch {
        case e: Exception =>
          println(s"[Reducer $reduceId] Error receiving from mapper: ${e.getMessage}")
      }
    }

    serverSocket.close()
    println(s"[Reducer $reduceId] Completed receiving all records.")
    allRecords.toSeq
  }
}
