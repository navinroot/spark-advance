package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession

object OlympicsProject extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext
  import spark.implicits._


  val olymSummerDf = spark.read.option("header",true).option("inferSchema",true)
    .csv("/home/navin/workspace/dataset/OTHERS/OLYMPICS/OLYMPICS SUMMER.csv").withColumnRenamed("Country","Code")

 // olymSummerDf.show()

  val countryDetailsDf= spark.read.option("header",true).option("inferSchema",true)
    .csv("/home/navin/workspace/dataset/OTHERS/OLYMPICS/COUNTRY POPULATION AND GDP.csv")

//  countryDetailsDf.show()

  val totalMedalWonByEachCountyInSwimming = olymSummerDf.filter('Discipline === "Swimming").groupBy('Code)
    .count().join(countryDetailsDf, Seq("Code"))
      .select('Country,'count).withColumnRenamed("count","MedalCount")

  //question 1 solution - (find the total medal won by each country)
 // totalMedalWonByEachCountyInSwimming.coalesce(1).write.option("header",true).csv("/home/navin/workspace/dataset/OTHERS/OLYMPICS/Q1Answer")

  val totalMedalWonByIndiaYearWise = olymSummerDf.filter('Code === "IND").select('Year, 'Medal)
    .groupBy('Year, 'Medal).count()
    .withColumnRenamed("count","Medal won by india").orderBy('Year desc)

  //question 2 ( Find the number of medals that India won year wise. )
  totalMedalWonByIndiaYearWise.show()

}
