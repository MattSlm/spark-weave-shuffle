package org.apache.spark.shuffle.weave

import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.weave.io.BaselineReaderTCP
import org.apache.spark.shuffle.weave.config.WeaveShuffleConf
import org.sparkweave.schema.WeaveRecord

class BaselineShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startMapIndex: Int, 
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    conf: WeaveShuffleConf
) extends ShuffleReader[K, C] {

  private val numReducers = handle.dependency.partitioner.numPartitions
  private val reducerId = startPartition
  private val expectedMappers = conf.numWeavers

  override def read(): Iterator[Product2[K, C]] = {
    val reader = new BaselineReaderTCP(reducerId, expectedMappers)
    val records = reader.readAll()
    records.iterator.map { r =>
      val k = r.key match {
        case WeaveRecord.Key.KeyStr(s)  => s
        case WeaveRecord.Key.KeyInt(i)  => i.toString
        case WeaveRecord.Key.KeyLong(l) => l.toString
        case WeaveRecord.Key.Empty      => "?"
      }
      val v = r.value match {
        case WeaveRecord.Value.ValStr(s)  => s
        case WeaveRecord.Value.ValInt(i)  => i.toString
        case WeaveRecord.Value.ValLong(l) => l.toString
        case WeaveRecord.Value.Empty      => "?"
      }
      (k.asInstanceOf[K], v.asInstanceOf[C])
    }
  }
}
