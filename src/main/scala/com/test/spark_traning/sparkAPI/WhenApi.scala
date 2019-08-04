package com.test.spark_traning.sparkAPI

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WhenApi extends App {


  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext
  import spark.implicits._


  val df = sc.parallelize(Seq((4, "blah", 2), (2, "", 3), (56, "foo", 3), (100, null, 5)))
    .toDF("A", "B", "C")

  val df1=df.withColumn("new_col", when('A=== 4, 4)
  .when('A === 2 or 'A=== 6, 2)
  .when('A===56 and 'A===123, 56)
  .otherwise(100))

  df1.show()

}
