package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.coalesce

object CoalesceAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._

  val dataDF= spark.read.option("delimiter",",")
    .option("inferSchema",true)
    .format("csv")
    .option("header",true)
    .load("/home/navin/workspace/git/spark-advance/src/main/resources/coalesce_data.csv")

  dataDF.show()
  /**
   * output:
   * +---+--------+--------+--------+
   * | id|logic_01|logic_02|logic_03|
   * +---+--------+--------+--------+
   * |  1|    null|       a|    null|
   * |  2|    null|       b|    null|
   * |  3|       c|    null|    null|
   * |  4|    null|    null|       d|
   * +---+--------+--------+--------+
   */

  /**
   *  coalesce all the logic* column to one column logic
   *
   *  coalesce function - Returns the first column's value that is not NULL,
   *  or NULL if all v's are NULL. This function can take any number of arguments.
   */

  val coalesceDf= dataDF.select('id,coalesce($"logic_01",$"logic_02",$"logic_03").as("logic"))

  coalesceDf.show()
  /**
   * output:
   * +---+-----+
   * | id|logic|
   * +---+-----+
   * |  1|    a|
   * |  2|    b|
   * |  3|    c|
   * |  4|    d|
   * +---+-----+
   *
   */
}
