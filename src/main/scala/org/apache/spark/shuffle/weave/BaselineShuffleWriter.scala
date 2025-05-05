package org.apache.spark.shuffle.weave


import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.weave.io.BaselineWriterTCP
import org.apache.spark.shuffle.weave.config.WeaveShuffleConf
import org.sparkweave.schema.WeaveRecord
import org.apache.spark.scheduler.MapStatus

class BaselineShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    conf: WeaveShuffleConf
) extends ShuffleWriter[K, V] {

  private val numReducers: Int = handle.dependency.partitioner.numPartitions

  private val keyToReducer: WeaveRecord => Int = { record =>
    val key = record.key match {
      case WeaveRecord.Key.KeyStr(s)  => s
      case WeaveRecord.Key.KeyInt(i)  => i.toString
      case WeaveRecord.Key.KeyLong(l) => l.toString
      case WeaveRecord.Key.Empty      => "0"
    }
    math.abs(key.hashCode) % numReducers
  }

  private val writer = new BaselineWriterTCP(
    numReducers = numReducers,
    keyToReducer = keyToReducer
  )
  override def getPartitionLengths(): Array[Long] = Array.fill(numReducers)(0L)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    for ((k, v) <- records) {
      val record = WeaveRecord()
        .withIsFake(false)
        .withKeyStr(k.toString)
        .withValStr(v.toString)
      writer.write(record)
    }
    writer.flush()
  }
  

  override def stop(success: Boolean): Option[MapStatus] = None
  
}
