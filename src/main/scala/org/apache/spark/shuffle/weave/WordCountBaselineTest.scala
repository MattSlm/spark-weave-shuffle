package org.apache.spark.shuffle.weave

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.shuffle.weave.registry.WeaveRegistry

object WordCountBaselineTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCountBaselineTest")
      .setMaster("local[4]")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.weave.BaselineShuffleManager")

    val sc = new SparkContext(conf)

    // ✅ Wait for reducers before mappers begin
    val numReducers = 4
    WeaveRegistry.initBarrier(numReducers)

    val lines = sc.parallelize(Seq(
      "hello world",
      "hello spark",
      "hello weave",
      "baseline shuffle"
    ), numSlices = 4)

    val wordCounts = lines
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()

    println("✅ Word counts:")
    wordCounts.foreach { case (word, count) =>
      println(s"$word: $count")
    }

    sc.stop()
  }
}
