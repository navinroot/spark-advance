package com.test.spark_traning.sparkAPI.df

import com.test.common.ResourcePath
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupByCount extends App {

  val conf = new SparkConf().setAppName("group by count").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  val empDf= spark.read.option("delimiter","|").option("header",true)
    .option("inferSchema",true).csv(ResourcePath.resourcePath+ResourcePath.pathSeperator+"Employee.csv")

  empDf.groupBy(empDf.col("Dept")).agg(count("*") as "count").where('count >=0).show()

  empDf.groupBy(empDf.col("Dept")).agg(count('Dept) as "count").where('count >=0).show()


  empDf.groupBy(empDf.col("Dept")).count().where('count >=0).show()

  empDf.groupBy(empDf.col("Dept")).agg(count(lit(1)) as "count").where('count >=0).show()

}
