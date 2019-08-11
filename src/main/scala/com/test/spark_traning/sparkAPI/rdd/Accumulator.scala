package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.sql.SparkSession

object Accumulator extends App with Serializable {

  val spark = SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val acc = sc.longAccumulator("dgsf")
  val someRDD = sc.parallelize(Array(1, 2, 3, 4)).map(x => acc.add(x)).count()

  println(acc.value)

}
