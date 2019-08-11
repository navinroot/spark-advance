package com.test.spark_traning.sparkAPI

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object LagAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext
  import spark.implicits._

  // create new column lag by one with column - num ( order should not change)

  val df = sc.parallelize(Seq((4, 9.0), (3, 7.0), (2, 3.0), (1, 5.0)),2).toDF("id", "num")
    .withColumn("inc_num",lit("A"))
    .withColumn("lag_num", lag('num, 1, 0).over(Window.orderBy('inc_num)))
      .drop('inc_num)
  df.show()







}
