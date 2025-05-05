package org.apache.spark.shuffle.weave

import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.weave.config.WeaveShuffleConf

class BaselineShuffleManager(conf: SparkConf) extends ShuffleManager {

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]
  ): ShuffleHandle = {
    new BaseShuffleHandle[K, V, C](shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext, 
      metrics: ShuffleWriteMetricsReporter
  ): ShuffleWriter[K, V] = {
    val baseHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    val weaveConf = WeaveShuffleConf.fromSparkConf(conf)
    new BaselineShuffleWriter[K, V](baseHandle, mapId, context, weaveConf)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int, 
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext, 
      metrics: ShuffleReadMetricsReporter
  ): ShuffleReader[K, C] = {
    val baseHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    val weaveConf = WeaveShuffleConf.fromSparkConf(conf)
    new BaselineShuffleReader[K, C](baseHandle, startMapIndex, endMapIndex, startPartition, endPartition, context, weaveConf)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = true

  override def stop(): Unit = {}
  
  override def shuffleBlockResolver: ShuffleBlockResolver =
  	throw new UnsupportedOperationException("Not implemented")



}
