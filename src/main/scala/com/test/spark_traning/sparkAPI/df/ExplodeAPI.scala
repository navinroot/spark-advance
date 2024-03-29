package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, explode, explode_outer}

object ExplodeAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._

  val df = Seq(
    (1, "Luke", Some(Array("baseball", "soccer"))),
    (2, "Lucy", None)
  ).toDF("id", "name", "likes")

  df. createOrReplaceTempView("df")
  /**
    *  explode API use :  we use to explode the Array or collection data inside column
    *  Note - it will ignore null value while explode ( in include null in explode use explode_outer function
    *  introduced in spark 2.2.0)
    *
    * +---+----+--------+
    * | id|name|   likes|
    * +---+----+--------+
    * |  1|Luke|baseball|
    * |  1|Luke|  soccer|
    * +---+----+--------+
    */


  val explodeDf= df.withColumn("likes", explode('likes))

  val explodeDfSql= spark.sql("select id,name,explode(likes) from df")

//  explodeDf.show()
//  explodeDfSql.show()




  /**
    *  another use case of explode can be use with shew data to remove skewness
    *
    * +---+----+--------+
    * | id|name|   likes|
    * +---+----+--------+
    * |  0|Luke|baseball|
    * |  1|Luke|baseball|
    * |  2|Luke|baseball|
    * |  0|Luke|  soccer|
    * |  1|Luke|  soccer|
    * |  2|Luke|  soccer|
    * +---+----+--------+
    *
    */

  val explodeDf1 = explodeDf
    .withColumn("id", explode(array($"id"-1,$"id", $"id"+1)))

 // explodeDf1.show()
  explodeDf.createOrReplaceTempView("explodeDf")
  val explodeDf1Sql = spark.sql("select explode(array(id-1,id,id+1)) as id,name,likes from explodeDf")
 // explodeDf1Sql.show()



  /**
    * explode_outer use case
    *
    * +---+----+--------+
    * | id|name|   likes|
    * +---+----+--------+
    * |  1|Luke|baseball|
    * |  1|Luke|  soccer|
    * |  2|Lucy|    null|
    * +---+----+--------+
    *
    */

  val explode_outerDf= df.withColumn("likes", explode_outer('likes))

 // explode_outerDf.show()

  val explode_outerDfSql = spark.sql("select id,name, explode_outer(likes) as likes from df")
  explode_outerDfSql.show()
}
