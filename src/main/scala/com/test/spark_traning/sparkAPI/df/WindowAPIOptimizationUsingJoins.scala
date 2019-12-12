package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowAPIOptimizationUsingJoins extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._

  val dataDf = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")).toDF("fruit","price",
    "location")

  val w=Window.partitionBy('location)

  val dataStatsDfUsingWindowOperation = dataDf.select(dataDf.col("*"),max('price).over(w) as "max",
    avg('price).over(w) as "avg",sum('price).over(w) as "sum")

  /**
   * +-------+-----+--------+----+------+----+
   * |  fruit|price|location| max|   avg| sum|
   * +-------+-----+--------+----+------+----+
   * | Banana|  400|   China|4000|1775.0|7100|
   * |Carrots| 1200|   China|4000|1775.0|7100|
   * |  Beans| 1500|   China|4000|1775.0|7100|
   * | Orange| 4000|   China|4000|1775.0|7100|
   * | Banana| 1000|     USA|2000|1620.0|8100|
   * |Carrots| 1500|     USA|2000|1620.0|8100|
   * |  Beans| 1600|     USA|2000|1620.0|8100|
   * | Orange| 2000|     USA|2000|1620.0|8100|
   * | Orange| 2000|     USA|2000|1620.0|8100|
   * |  Beans| 2000|  Mexico|2000|2000.0|2000|
   * | Banana| 2000|  Canada|2000|2000.0|4000|
   * |Carrots| 2000|  Canada|2000|2000.0|4000|
   * +-------+-----+--------+----+------+----+
   */
  dataStatsDfUsingWindowOperation.show()

  /**
   * get the stats using join operation
   *
   */
  val dataStats = dataDf.select('location,'price).groupBy('location).agg(max('price) as "max",
    avg('price) as "avg", sum('price) as "sum")

  val dataStatsUsingJoinOperation = dataDf.join(dataStats,Seq("location"),"left")

  /**
   * +--------+-------+-----+----+------+----+
   * |location|  fruit|price| max|   avg| sum|
   * +--------+-------+-----+----+------+----+
   * |     USA| Banana| 1000|2000|1620.0|8100|
   * |     USA|Carrots| 1500|2000|1620.0|8100|
   * |     USA|  Beans| 1600|2000|1620.0|8100|
   * |     USA| Orange| 2000|2000|1620.0|8100|
   * |     USA| Orange| 2000|2000|1620.0|8100|
   * |   China| Banana|  400|4000|1775.0|7100|
   * |   China|Carrots| 1200|4000|1775.0|7100|
   * |   China|  Beans| 1500|4000|1775.0|7100|
   * |   China| Orange| 4000|4000|1775.0|7100|
   * |  Canada| Banana| 2000|2000|2000.0|4000|
   * |  Canada|Carrots| 2000|2000|2000.0|4000|
   * |  Mexico|  Beans| 2000|2000|2000.0|2000|
   * +--------+-------+-----+----+------+----+
   *
   */
 // dataStatsUsingJoinOperation.show()

}
