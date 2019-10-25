package com.test.spark_traning.sparkAPI

import org.apache.spark.sql.SparkSession

/**
 *  use case:
 *  Let say you have thousands of columns containing data with null value
 *  Columns having datatype (String, integer, boolean, Double, Long.....)
 *
 * you have to replace
 * String with "not applicable"
 * integer, Long with 0
 * Double, Float with 0.0
 * and so on
 *
 */


object DealingWithNull extends App {

  val spark = SparkSession.builder().appName("join using array contains").master("local[*]").getOrCreate()
  import spark.implicits._

  val df  = spark.read.option("header",true).option("inferSchema",true)
    .csv("/home/navin/workspace/git/spark-advance/src/main/resources/dataWithNullValue.csv")

//  df.show()


  val schemaDatatype = df.dtypes
//  println(schemaDatatype.mkString(" "))
  // (name,StringType) (country,StringType) (zip_code,IntegerType)

  //find out all the distinct datatype
  val all_distinct_datatype =schemaDatatype.map(x => x._2).distinct
  println(all_distinct_datatype.mkString(" "))

  val stringColumn = schemaDatatype.filter(_._2 =="StringType").map(x => x._1)
  //println(stringColumn.mkString(" "))
  // name country

  val integerColumn = schemaDatatype.filter(_._2 =="IntegerType").map(x => x._1)

  val dfWithoutNull = df.na.fill("Not Applicable", stringColumn).na.fill(0,integerColumn)

  dfWithoutNull.show()

}
