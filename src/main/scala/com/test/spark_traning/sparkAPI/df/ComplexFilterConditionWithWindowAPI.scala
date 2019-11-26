package com.test.spark_traning.sparkAPI.df

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * 1. For every key check if there is a statusCode === UV.
 * 2. If there is no UV status code associated with that key ignore that key completely.
 * 3. Please Note: There should only ever be one UV for each key.
 * 4. If there is then search for the closest OA event that is after the UV timestamp.
 * 5. Please note: There could be multiple OA events after the UV timestamp. I want the one closest to the UV timestamp.
 *
 * 6. If the only OA event is in the past (i.e. before the UV I still want to keep that record because
 * an expected OA will come in but I want to still capture the row with a status code of OA but replace
 * the value will null
 */

object ComplexFilterConditionWithWindowAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._

  // Sample data:
  //   key `A`: requirement #3
  //   key `B`: requirement #2
  //   key `C`: requirement #4
  val df = Seq(
    ("A", "OA", Timestamp.valueOf("2019-05-20 00:00:00")),
    ("A", "E",  Timestamp.valueOf("2019-05-30 00:00:00")),
    ("A", "UV", Timestamp.valueOf("2019-06-22 00:00:00")),
    ("A", "OA", Timestamp.valueOf("2019-07-01 00:00:00")),
    ("A", "OA", Timestamp.valueOf("2019-07-03 00:00:00")),
    ("B", "C",  Timestamp.valueOf("2019-06-15 00:00:00")),
    ("B", "OA", Timestamp.valueOf("2019-06-25 00:00:00")),
    ("C", "D",  Timestamp.valueOf("2019-06-01 00:00:00")),
    ("C", "OA", Timestamp.valueOf("2019-06-30 00:00:00")),
    ("C", "UV", Timestamp.valueOf("2019-07-02 00:00:00"))
  ).toDF("key", "statusCode", "statusTimestamp")

  df.createOrReplaceTempView("df")
  /**
   * solution
   * 1. Filter the dataset for statusCode "UV" or "OA" only
   * 2. For each row, use Window functions to create a String of statusCode from the previous, current, and next 2 rows
   * 3. Use Regex pattern matching to identify the wanted rows
   */
  val w =Window.partitionBy('key).orderBy('statusTimestamp asc)


  val df1 = df.filter('statusCode.contains("OA") || 'statusCode.contains("UV"))
    .withColumn("prevCurrNext2", concat(
      coalesce(lag('statusCode,1,"").over(w)),
      lit("#"),
      coalesce('statusCode),
      lit("#"),
      coalesce(lead('statusCode,1,"").over(w)),
      lit("#"),
      coalesce(lead('statusCode,2,"").over(w))

    ))
  //  df1.show()

  /**
   * df1 output:
   * +---+----------+-------------------+-------------+
   * |key|statusCode|    statusTimestamp|prevCurrNext2|
   * +---+----------+-------------------+-------------+
   * |  B|        OA|2019-06-25 00:00:00|        #OA##|
   * |  C|        OA|2019-06-30 00:00:00|      #OA#UV#|
   * |  C|        UV|2019-07-02 00:00:00|      OA#UV##|
   * |  A|        OA|2019-05-20 00:00:00|    #OA#UV#OA|
   * |  A|        UV|2019-06-22 00:00:00|  OA#UV#OA#OA|
   * |  A|        OA|2019-07-01 00:00:00|    UV#OA#OA#|
   * |  A|        OA|2019-07-03 00:00:00|      OA#OA##|
   * +---+----------+-------------------+-------------+
   */

  val df1Sql =spark.sql(
    """select a.*,concat(coalesce(lag(a.statusCode, 1, "") over(partition by a.key order by a.statusTimestamp asc)),
      |"#",coalesce(a.statusCode),"#",
      |coalesce(lead(a.statusCode,1,"") over(partition by a.key order by a.statusTimestamp asc)),"#",
      |coalesce(lead(a.statusCode,2,"") over(partition by a.key order by a.statusTimestamp asc))) as prevCurrNext2
      |from( select * from df where
      |statusCode ="OA" or statusCode ="UV") a""".stripMargin)



  df1Sql.show()

}
