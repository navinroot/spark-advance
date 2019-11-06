package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.{SparkConf, SparkContext}

object TakeOrderedAPI extends App {

  val conf = new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("TakeOrdered API and TOP API")
  val sc = new SparkContext(conf)

  // get highest 2 element
  val decendingRdd = sc.parallelize(Seq(10, 4, 2, 12, 3)).top(2)
  println(decendingRdd.mkString(" "))

  //get lowest 2 element
  val accendingRdd = sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(2)
  println(accendingRdd.mkString(" "))

}
