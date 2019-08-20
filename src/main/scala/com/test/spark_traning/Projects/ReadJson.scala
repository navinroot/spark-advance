package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession

object ReadJson extends App {

  val spark = SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  val jsonDf = spark.read.option("multiLine", true).json("/home/navin/Downloads/test.json")

  //  jsonDf.printSchema()

  // jsonDf.show()

  val element = jsonDf.withColumn("dateExplode", 'data)

  element.show()

}
