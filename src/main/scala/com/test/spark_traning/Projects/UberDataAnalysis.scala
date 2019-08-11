package com.test.spark_traning.Projects

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Find the days on which each basement has more trips.
 *
 */

object UberDataAnalysis extends App {

  val conf = new SparkConf().setAppName("skew join using salting").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val schema = StructType(
    List(
      StructField("dispatching_base_number", StringType, false),
      StructField("date", TimestampType, true),
      StructField("active_vehicles", IntegerType, true),
      StructField("trips", IntegerType, true)
    )
  )

  val uberDf = spark.read.option("header", true)
    .option("timestampFormat", "MM/dd/yyyy")
    .schema(schema)
    .csv("/home/navin/workspace/git/spark-advance/src/main/resources/User_data.csv")
    .withColumn("week_day", date_format('date, "E"))

  // uberDf.show()
  // uberDf.printSchema()

  /**
   * schema:
   * root
   * |-- dispatching_base_number: string (nullable = true)
   * |-- date: timestamp (nullable = true)
   * |-- active_vehicles: integer (nullable = true)
   * |-- trips: integer (nullable = true)
   * |-- week_day: string (nullable = true)
   *
   */

  val result = uberDf.select('dispatching_base_number, 'week_day, 'trips)
    .groupBy('dispatching_base_number, 'week_day).agg(sum('trips).alias("total_trips"))
    .orderBy('total_trips desc)

  // result.show()


}
