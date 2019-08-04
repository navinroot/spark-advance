package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object UserAnalytics extends App {

  val spark= SparkSession.builder().appName("User analytics").master("local[*]").getOrCreate()

  import spark.implicits._



  val schema =  StructType(
    List(
    StructField("id",IntegerType,false),
    StructField("age",IntegerType,true),
    StructField("gender",StringType,true),
    StructField("profession",StringType,true),
    StructField("zipcode",IntegerType,true)
    ))

  val userDf= spark.read.schema(schema).option("header", true)
    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/userData.csv")

  //userDf.show()

  /**
    * how many unique professions we have in whole data file
    *
    */

  val uniqueProfession= userDf.select('profession).distinct().count()

//  println("unique profession = "+uniqueProfession)

  /**
    * how many different users belong to unique professions
    *
    */

  val differentUserCountEachProfession = userDf.select('id,'profession).groupBy('profession).count().withColumnRenamed("count", "user count")

//  differentUserCountEachProfession.show()

  /**
    * how many users are male female
    *
    */
  val countByGender=userDf.select('gender).groupBy('gender).count()

 // countByGender.show()

  /**
    *
    *
    */

}
