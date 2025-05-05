package org.apache.spark.shuffle.weave

import org.apache.spark.shuffle.weave.io.{BaselineReaderTCP, BaselineWriterTCP}
import org.apache.spark.shuffle.weave.codec.{ProtobufBatchEncoder, ProtobufBatchDecoder}
import org.apache.spark.shuffle.weave.registry.WeaveRegistry
import org.sparkweave.schema.WeaveRecord
import org.sparkweave.schema.WeaveRecord.{Key, Value}
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite
import org.sparkweave.schema.WeaveRecord

import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger

class BaselineShuffleTest extends AnyFunSuite {

  test("Sort by prefix (first digit of key) with thousands of records") {
    val numReducers = 5
    val expectedMappers = 1
    val reducerBuffers = Array.fill(numReducers)(mutable.Buffer.empty[WeaveRecord])

    // Reducers start TCP servers
    val readerThreads = (0 until numReducers).map { reducerId =>
      new Thread(() => {
        val reader = new BaselineReaderTCP(reducerId, expectedMappers)
        val records = reader.readAll()
        reducerBuffers(reducerId) ++= records
      })
    }
    readerThreads.foreach(_.start())
    Thread.sleep(500) // Give reducers time to start

    // Mapper logic
    val writer = new BaselineWriterTCP(
      numReducers,
      keyToReducer = r => r.getKeyStr.charAt(0).toInt % numReducers
    )

    val sortedKeys = (1000 until 2000).map(i => s"${i % 10}_key_$i")
    sortedKeys.foreach { k =>
      writer.write(
	WeaveRecord()
	  .withIsFake(false)
          .withKeyStr(k)	
          .withValStr("value")
	)
    }
    writer.flush()
    readerThreads.foreach(_.join())

    val allKeys = reducerBuffers.flatten.map(_.getKeyStr).toSet
    assert(allKeys == sortedKeys.toSet)
  }

  test("Word count simulation with hash-based keyToReducer and thousands of words") {
    val numReducers = 4
    val expectedMappers = 3
    val reducerResults = Array.fill(numReducers)(mutable.Buffer.empty[WeaveRecord])

    // Reducers
    val readerThreads = (0 until numReducers).map { reducerId =>
      new Thread(() => {
        val reader = new BaselineReaderTCP(reducerId, expectedMappers)
        val records = reader.readAll()
        reducerResults(reducerId) ++= records
      })
    }
    readerThreads.foreach(_.start())
    Thread.sleep(500)

    // Simulated mappers with thousands of words
    val wordsPool = Array("apple", "banana", "orange", "grape", "melon")
    val wordLists = List.fill(expectedMappers) {
      List.fill(2000)(wordsPool(scala.util.Random.nextInt(wordsPool.length)))
    }

    val writerThreads = wordLists.map { words =>
      new Thread(() => {
        val writer = new BaselineWriterTCP(
          numReducers,
          keyToReducer = r => math.abs(r.getKeyStr.hashCode) % numReducers
        )
        words.foreach { w =>
          writer.write(
	 	WeaveRecord()
  			.withIsFake(false)
  			.withKeyStr(w)
  			.withValStr("1")
			)}
        writer.flush()
      })
    }
    writerThreads.foreach(_.start())
    writerThreads.foreach(_.join())
    readerThreads.foreach(_.join())

    val allRecords = reducerResults.flatten
    val counts = allRecords.groupBy(_.getKeyStr).map { case (k, v) => k -> v.size }
    val totalWords = wordLists.flatten.size
    assert(counts.values.sum == totalWords)
  }
} 
