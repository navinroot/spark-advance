package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.sql.SparkSession

object MapPartitionAPI extends App {

  val spark = SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  /**
   * mapPartitions() can be used as an alternative to map() & foreach().
   * The main advantage being that, we can do initialization on Per-Partition basis instead of per-element
   * basis(as done by map() & foreach())
   *
   * in some cases mapPartition is faster than map
   */

  /**
   * use case - append the character count in each word
   */
  val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
  val TotalSum = rdd.mapPartitions(itr => {
    var sum = 0
    while (itr.hasNext) {
      sum += itr.next()
    }
    Iterator(sum)
  }).sum()

  println(TotalSum)
}
