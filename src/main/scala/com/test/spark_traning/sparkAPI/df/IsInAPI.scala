package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession

object IsInAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()
  import spark.implicits._
  val sc= spark.sparkContext


  val df= spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/1.csv")

 // df.show()

  /**
    * +----------+--------+-------+
    * |   product|category|revenue|
    * +----------+--------+-------+
    * |      thin|celphone|   6000|
    * |    normal|  tablet|   1500|
    * |      mini|  tablet|   5500|
    * |ultra thin|celphone|   5000|
    * | very thin|celphone|   6000|
    * |       big|  tablet|   2500|
    * |  bendable|celphone|   3000|
    * |  foldable|celphone|   3000|
    * |       pro|  tablet|   4500|
    * |      pro2|  tablet|   6500|
    * +----------+--------+-------+
    */

  val revenueList = List(6000,5500,3000)

  // use case 1
  val filterDfUsingIsIn= df.filter('revenue.isin(revenueList:_*))

 //  filterDfUsingIsIn.show()

  // use case 2
  val filterdf = df.filter(('revenue.isin(6000,5000)))

 // filterdf.show()

  //use case 3

  val filterDfUsingIsInCollection = df.filter('revenue.isInCollection(revenueList))

  filterDfUsingIsInCollection.show()


  /**
    * get data from one column to list ( get all the revenue data in List)
    *
    */
  val revenueSeqUsingRdd = df.select('revenue).rdd.collect().map(x=>x(0)).toList
  val revenueSeqUsingDf = df.select('revenue).collect().map(x=>x(0)).toList
}
