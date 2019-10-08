package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession

object BuildingProjectTimestampPractice extends App {

  val spark = SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val hvocDf = spark.read.option("header", true).option("inferSchema", true)
    .csv("/home/navin/workspace/dataset/OTHERS/Data Sets/Sensor data/HVAC.csv")
    .withColumn("DateTime", to_timestamp(concat('Date, 'Time), "MM/dd/yyHH:mm:ss"))


  val dateTimeOperationDF = hvocDf.withColumn("year", year('DateTime))
    .withColumn("month", month('DateTime))
    .withColumn("dayOfMonth", dayofmonth('DateTime))
    .withColumn("dayOfWeek", dayofweek('DateTime))
    .withColumn("hour", hour('DateTime))
    .withColumn("minute", minute('DateTime))
    .withColumn("second", second('DateTime))
    .withColumn("twoDaySub", date_sub('DateTime, 2))
    .withColumn("twoMonthSub", add_months('DateTime, -2))
    .withColumn("oneYearAdd", add_months('DateTime, 12))
    .withColumn("subOneYear", 'DateTime - expr("INTERVAL 1 YEARS"))
    .withColumn("subOneMonth", 'DateTime - expr("INTERVAL 1 MONTHS"))
    .withColumn("subOneDay", 'DateTime - expr("INTERVAL 1 DAYS"))
    .withColumn("addOneHours", 'DateTime + expr("INTERVAL 1 HOURS"))
    .withColumn("addOneMinutes", 'DateTime + expr("INTERVAL 1 MINUTES"))
    .withColumn("addOneSeconds", 'DateTime + expr("INTERVAL 1 SECONDS"))


  val filterByDateEquality = hvocDf.filter('DateTime === lit("2013-06-01 00:00:01"))

  filterByDateEquality.show()

  val filterByDateBetween = hvocDf.filter('DateTime.gt(lit("2013-06-01 00:00:01"))
    and 'DateTime.lt(lit("2013-06-03 00:00:01")))
  //  filterByDateBetween.show()

  // FILTER DATE BY MAX DATE TO LAST 7 DAYS DATE  (MAX DATE = LAST MODIFIED DATE)


  val lastModifiedDate = hvocDf.agg(max('DateTime) - expr("INTERVAL 7 DAYS")).as[java.sql.Timestamp].collect().head

  val filterByLast7DaysFromLastModifiedDate = hvocDf.filter('DateTime.geq(lit(lastModifiedDate)))
 // filterByLast7DaysFromLastModifiedDate.show()


  // FILTER LAST 7 days data

  val filerLast7DaysData = hvocDf.filter('DateTime.geq(current_date() - expr("INTERVAL 7 DAYS")))

  filerLast7DaysData.show()

  //show last 1 hour data

  val filterLast1HourData = hvocDf
    .select('DateTime)
    .withColumn("last 1 hour", (current_timestamp()
      - expr("INTERVAL 1 HOURS")))

  // filterLast1HourData.show(false)

  /**
   * get week day of provided DateTime
   * FORMAT - "E" for week day as Sun Mon Tue
   * "EEEEE" for week day Wednesday Friday Monday
   * "u" for week day in numeric format 1 2 3 4 5 6 7  (  same as dayOfWeek function )
   * "MM/dd/yyyy" for conversion from string to date format
   */
  val getWeekDayOfDate = hvocDf.select('DateTime)
    .withColumn("weekDay", date_format('DateTime, "E"))
  // getWeekDayOfDate.show()




}
