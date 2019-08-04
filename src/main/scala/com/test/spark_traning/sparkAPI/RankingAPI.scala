package com.test.spark_traning.sparkAPI

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object RankingAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._


  val df= spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/1.csv")
    .repartition(4)

  /**
    * |   product|category|revenue|
    * +----------+--------+-------+
    * |      thin|celphone|   6000|
    * | very thin|celphone|   6000|
    * |ultra thin|celphone|   5000|
    * |  foldable|celphone|   3000|
    * |  bendable|celphone|   3000|
    * |       pro|  tablet|   4500|
    * |    normal|  tablet|   1500|
    * |      mini|  tablet|   5500|
    * |      pro2|  tablet|   6500|
    * |       big|  tablet|   2500|
    * +----------+--------+-------+
    */
  //df.show()

  /**
    * monotonically_increasing_id function index increasing id but not contiguous. if all the data are in same
    * partition then only it will index contiguous increasing Id
    *
    * +----------+--------+-------+-----------+
    * |      thin|celphone|   6000|          0|
    * | very thin|celphone|   6000|          1|
    * |ultra thin|celphone|   5000|          2|
    * |  foldable|celphone|   3000| 8589934592|
    * |  bendable|celphone|   3000| 8589934593|
    * |       pro|  tablet|   4500|17179869184|
    * |    normal|  tablet|   1500|17179869185|
    * |      mini|  tablet|   5500|25769803776|
    * |      pro2|  tablet|   6500|25769803777|
    * |       big|  tablet|   2500|25769803778|
    * +----------+--------+-------+-----------+
    */
  val df1= df.withColumn("index", monotonically_increasing_id())

  //df1.show()

  /**
    * index the dataframe with contiguous increasing id.
    *
    * +----------+--------+-------+---+
    * |   product|category|revenue| id|
    * +----------+--------+-------+---+
    * |      thin|celphone|   6000|  0|
    * | very thin|celphone|   6000|  1|
    * |ultra thin|celphone|   5000|  2|
    * |  foldable|celphone|   3000|  3|
    * |  bendable|celphone|   3000|  4|
    * |       pro|  tablet|   4500|  5|
    * |    normal|  tablet|   1500|  6|
    * |      mini|  tablet|   5500|  7|
    * |      pro2|  tablet|   6500|  8|
    * |       big|  tablet|   2500|  9|
    * +----------+--------+-------+---+
    *
    */

    val schema = df.schema.add("id",LongType)

    val df2= df.rdd.zipWithIndex().map(x => Row(x._1(0),x._1(1),x._1(2),x._2))

    val df3 = spark.createDataFrame(df2,schema).repartition(4)

  //  df3.show()

  /**
    * rank in decreasing order of revenue
    *
    * +----------+--------+-------+------------+
    * |   product|category|revenue|best revenue|
    * +----------+--------+-------+------------+
    * |      pro2|  tablet|   6500|           1|
    * |      thin|celphone|   6000|           2|
    * | very thin|celphone|   6000|           2|
    * |      mini|  tablet|   5500|           4|
    * |ultra thin|celphone|   5000|           5|
    * |       pro|  tablet|   4500|           6|
    * |  foldable|celphone|   3000|           7|
    * |  bendable|celphone|   3000|           7|
    * |       big|  tablet|   2500|           9|
    * |    normal|  tablet|   1500|          10|
    * +----------+--------+-------+------------+
    *
    */

  val df4= df3.select('product,'category,'revenue)
      .withColumn("best revenue", rank().over(Window.orderBy('revenue desc)))

 // df4.show()

  /**
    * dense rank in decreasing order of revenue
    *
    * +----------+--------+-------+------------+
    * |   product|category|revenue|best revenue|
    * +----------+--------+-------+------------+
    * |      pro2|  tablet|   6500|           1|
    * |      thin|celphone|   6000|           2|
    * | very thin|celphone|   6000|           2|
    * |      mini|  tablet|   5500|           3|
    * |ultra thin|celphone|   5000|           4|
    * |       pro|  tablet|   4500|           5|
    * |  foldable|celphone|   3000|           6|
    * |  bendable|celphone|   3000|           6|
    * |       big|  tablet|   2500|           7|
    * |    normal|  tablet|   1500|           8|
    * +----------+--------+-------+------------+
    */

  val df5= df3.select('product,'category,'revenue)
    .withColumn("best revenue", dense_rank().over(Window.orderBy('revenue desc)))

 // df5.show()

  /**
    * rowNumber in decreasing order of revenue
    *
    * +----------+--------+-------+------------+
    * |   product|category|revenue|best revenue|
    * +----------+--------+-------+------------+
    * |      pro2|  tablet|   6500|           1|
    * | very thin|celphone|   6000|           2|
    * |      thin|celphone|   6000|           3|
    * |      mini|  tablet|   5500|           4|
    * |ultra thin|celphone|   5000|           5|
    * |       pro|  tablet|   4500|           6|
    * |  bendable|celphone|   3000|           7|
    * |  foldable|celphone|   3000|           8|
    * |       big|  tablet|   2500|           9|
    * |    normal|  tablet|   1500|          10|
    * +----------+--------+-------+------------+
    */

  val df6= df3.select('product,'category,'revenue)
    .withColumn("best revenue", row_number().over(Window.orderBy('revenue desc)))

 // df6.show()

  /**
    * rank in decreasing order of revenue for each category
    *
    * +----------+--------+-------+-------------------------+
    * |   product|category|revenue|best revenue per category|
    * +----------+--------+-------+-------------------------+
    * | very thin|celphone|   6000|                        1|
    * |      thin|celphone|   6000|                        1|
    * |ultra thin|celphone|   5000|                        3|
    * |  bendable|celphone|   3000|                        4|
    * |  foldable|celphone|   3000|                        4|
    * |      pro2|  tablet|   6500|                        1|
    * |      mini|  tablet|   5500|                        2|
    * |       pro|  tablet|   4500|                        3|
    * |       big|  tablet|   2500|                        4|
    * |    normal|  tablet|   1500|                        5|
    * +----------+--------+-------+-------------------------+
    */

  val df7= df3.select('product,'category,'revenue)
    .withColumn("best revenue per category", rank().over(Window.partitionBy('category)
      .orderBy('revenue desc)))

 //  df7.show()

  /**
    * dense rank in decreasing order of revenue for each category
    *
    * +----------+--------+-------+-------------------------+
    * |   product|category|revenue|best revenue per category|
    * +----------+--------+-------+-------------------------+
    * | very thin|celphone|   6000|                        1|
    * |      thin|celphone|   6000|                        1|
    * |ultra thin|celphone|   5000|                        2|
    * |  bendable|celphone|   3000|                        3|
    * |  foldable|celphone|   3000|                        3|
    * |      pro2|  tablet|   6500|                        1|
    * |      mini|  tablet|   5500|                        2|
    * |       pro|  tablet|   4500|                        3|
    * |       big|  tablet|   2500|                        4|
    * |    normal|  tablet|   1500|                        5|
    * +----------+--------+-------+-------------------------+
    */

  val df8= df3.select('product,'category,'revenue)
    .withColumn("best revenue per category", dense_rank().over(Window.partitionBy('category)
      .orderBy('revenue desc)))

 //  df8.show()

  /**
    * rowNumber in decreasing order of revenue for each category
    *
    * +----------+--------+-------+-------------------------+
    * |   product|category|revenue|best revenue per category|
    * +----------+--------+-------+-------------------------+
    * | very thin|celphone|   6000|                        1|
    * |      thin|celphone|   6000|                        2|
    * |ultra thin|celphone|   5000|                        3|
    * |  bendable|celphone|   3000|                        4|
    * |  foldable|celphone|   3000|                        5|
    * |      pro2|  tablet|   6500|                        1|
    * |      mini|  tablet|   5500|                        2|
    * |       pro|  tablet|   4500|                        3|
    * |       big|  tablet|   2500|                        4|
    * |    normal|  tablet|   1500|                        5|
    * +----------+--------+-------+-------------------------+
    */

  val df9= df3.select('product,'category,'revenue)
    .withColumn("best revenue per category", row_number().over(Window.partitionBy('category)
      .orderBy('revenue desc)))

   // df9.show()


}
