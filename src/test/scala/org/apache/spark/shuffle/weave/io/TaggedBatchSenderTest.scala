package org.apache.spark.shuffle.weave.io

import org.scalatest.funsuite.AnyFunSuite

class TaggedBatchSenderTest extends AnyFunSuite {

  test("TaggedBatchSender flushes on batch size") {
    val flushed = scala.collection.mutable.Buffer.empty[(Int, Seq[(Boolean, String, String)])]

    val sender = new TaggedBatchSender[String, String](
      numBins = 2,
      batchSize = 3,
      send = (bin, batch) => {
        println(s"[Flush] bin=$bin => ${batch.mkString(", ")}")
        flushed.append((bin, batch))
      }
    )

    sender.addReal(0, "k1", "v1")
    sender.addReal(0, "k2", "v2")
    sender.addFake(0, "k3", "v3") // triggers flush

    assert(flushed.nonEmpty)
    assert(flushed.head._1 == 0)
    assert(flushed.head._2.exists(_._2 == "k3"))
  }

  test("TaggedBatchSenderParallel flushes real and fake separately") {
    val flushed = scala.collection.mutable.Buffer.empty[(Int, Boolean, Seq[(String, String)])]

    val sender = new TaggedBatchSenderParallel[String, String](
      numBins = 1,
      batchSize = 2,
      send = (bin, isFake, records) => {
        println(s"[Flush] bin=$bin isFake=$isFake => ${records.mkString(", ")}")
        flushed.append((bin, isFake, records))
      }
    )

    sender.addReal(0, "k1", "v1")
    sender.addReal(0, "k2", "v2") // triggers real flush
    sender.addFake(0, "k3", "v3")
    sender.addFake(0, "k4", "v4") // triggers fake flush

    assert(flushed.count(_._2 == false) == 1)
    assert(flushed.count(_._2 == true) == 1)
    assert(flushed.exists(_._3.exists(_._1 == "k3")))
  }
}  
