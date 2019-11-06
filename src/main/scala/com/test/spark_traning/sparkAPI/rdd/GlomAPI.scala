package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.sql.SparkSession

object GlomAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext

  val num_partitions = 20000
  val rdd = sc.parallelize(0 until 1e6.toInt, num_partitions )

  val maxLengthUsingGlom= rdd.glom().map( _.length).max()

  val maxLengthUsingMapPartition = rdd.mapPartitions( itr => Iterator(itr.size)).max()

  println("maxLengthUsingGlom = " + maxLengthUsingGlom)

  println("maxLengthUsingMapPartition = " + maxLengthUsingMapPartition)





}
