package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

/**
 * find max of total value for each hour
 * Approach 1 : using window function
 *
 * Approach 2: using dataset groupBykey function here it will leverage map side combine
 */

object WindowApiOptimizationUsingDataset extends App {


  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()
  import spark.implicits._

  val df = spark.sparkContext.parallelize(Seq(
    (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
    (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
    (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
    (3,"cat8",35.6))).toDF("Hour", "Category", "TotalValue")

  // Approach 1
  val w = Window.partitionBy('Hour).orderBy('TotalValue desc)

  val dfWithMax = df.withColumn("rn",row_number().over(w)).where('rn ===1).drop('rn)
  // dfWithMax.show()


  val dfWithMaxUsingGroupBy = df.groupBy('Hour).agg(max('TotalValue))
// dfWithMaxUsingGroupBy.show()


  //Approach 2 using Dataset mapside join approach
  /**
   * can leverage map side combine and don't require full shuffle so most of the time should exhibit
   * a better performance compared to window functions and joins
   */
  val dsWithMax = df.as[Record].groupByKey(_.Hour).reduceGroups((x,y) =>{
    if (x.TotalValue > y.TotalValue) x else y
  }).map(_._2).as[Record]

 // dsWithMax.show()


  /**
   * change datatype
   *
   */
  val df1=df.withColumn("HourInLong",'Hour.cast(LongType))
  df1.printSchema()

}

case class Record(Hour:Int,Category:String,TotalValue:Double)