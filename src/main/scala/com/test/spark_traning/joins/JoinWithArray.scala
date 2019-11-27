package com.test.spark_traning.joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinWithArray extends App {

  val spark = SparkSession.builder().appName("join using array contains").master("local[*]").getOrCreate()
  import spark.implicits._

  val left = Seq(1, 2, 3).toDF("col1")

  val right = Seq((Array(1, 2), "Yes"),(Array(3),"No")).toDF("col1_array", "col2")

  /**
   * Join using array_contains for optimization
   */
  val joinResult = left.join(right,array_contains('col1_array,'col1))

   joinResult.show()

  /**
   * join using explode Api
   *
   */
    val rightExplode= right.withColumn("col1_array", explode('col1_array))
      .withColumnRenamed("col1_array","col1")

  val joinUsingExplode = left.join(rightExplode,Seq("col1"))
  //joinUsingExplode.show()

  /**
   * now merge the col1 value by col2
    */

  joinUsingExplode.groupBy('col2).agg(sort_array(collect_list('col1),false)
    .as("col1_array")).show()


}
