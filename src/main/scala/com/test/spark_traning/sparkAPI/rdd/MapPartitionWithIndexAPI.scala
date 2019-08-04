package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.sql.SparkSession

object MapPartitionWithIndexAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext

  val inputData = sc.parallelize(Array("Skip this line XXXXX","Start here instead AAAA","Second line to work with BBB"))

  /**
    * strip first or first n lines of input data
    *
    */
  val strippedData= inputData.coalesce(1).mapPartitionsWithIndex( (index, itr) => {
    if(index ==0){
      itr.drop(1)
    }
    itr
  })
 // strippedData.collect().foreach(println)




}
