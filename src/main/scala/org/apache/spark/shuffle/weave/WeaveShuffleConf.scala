package org.apache.spark.shuffle.weave.config

import org.apache.spark.SparkConf

case class WeaveShuffleConf(
  c: Int,
  alpha: Double,
  delta: Double,
  beta: Double,
  randomShuffleStrategy: String,
  histogramStrategy: String,
  balancedShuffleStrategy: String,
  fakePaddingStrategy: String,
  shuffleMode: String,
  globalSeed: Long,
  batchSize: Int,
  numWeavers: Int,
  bufferSize: Int,
  enableProfiling: Boolean
)

object WeaveShuffleConf {

  def fromSparkConf(conf: SparkConf): WeaveShuffleConf = {
    WeaveShuffleConf(
      c = conf.getInt("spark.weave.c", 1),
      alpha = conf.getDouble("spark.weave.alpha", 0.01),
      delta = conf.getDouble("spark.weave.delta", 0.05),
      beta = conf.getDouble("spark.weave.beta", 0.1),
      randomShuffleStrategy = conf.get("spark.weave.randomShuffle", "PRG"),
      histogramStrategy = conf.get("spark.weave.histogram", "InPlaceHash"),
      balancedShuffleStrategy = conf.get("spark.weave.balancedShuffle", "SimpleGreedyBinPacking"),
      fakePaddingStrategy = conf.get("spark.weave.fakePadding", "RepeatReal"),
      shuffleMode = conf.get("spark.weave.shuffleMode", "TaggedBatch"),
      globalSeed = conf.getLong("spark.weave.globalSeed", 1337L),
      batchSize = conf.getInt("spark.weave.batchSize", 100),
      numWeavers = conf.getInt("spark.weave.numWeavers", 64),
      bufferSize = conf.getInt("spark.weave.bufferSize", 4 * 1024 * 1024), // 4MB default
      enableProfiling = conf.getBoolean("spark.weave.enableProfiling", true)
    )
  }
}
