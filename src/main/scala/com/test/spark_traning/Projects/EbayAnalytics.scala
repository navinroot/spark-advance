package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EbayAnalytics extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext
  import spark.implicits._


  val ebaySchema = StructType(
    List(
      StructField("auctionid",StringType,false),
      StructField("bid",StringType,true),
      StructField("bidtime",FloatType,true),
      StructField("bidder",StringType,true),
      StructField("bidderrate",IntegerType,true),
      StructField("openbid",FloatType,true),
      StructField("price",FloatType,true),
      StructField("item",StringType,true),
      StructField("daystolive",IntegerType,true)
    )
  )

  val auctionDf= spark.read.schema(ebaySchema).option("header",false)
    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/ebay.csv")

 // auctionDf.show()

  /**
   * how many auction were held total
    *
    */
  val auctionNum= auctionDf.select('auctionid).distinct()

 // println("total number of auction = "+ auctionNum.count())

  /**
    * How many bids were made per item?
    *
    */

  val bidPerItem= auctionDf.select('item,'auctionid).groupBy('auctionid,'item).count()

//  bidPerItem.show()

  /**
    *  What's the min number of bids per item? what's the average? what's the max?
    */

  val bidStates = auctionDf.groupBy('auctionid, 'item).count().agg(min('count), max('count), avg('count))

  bidStates.show()

  /**
    *  Get the auctions with closing price > 100
    *
    */
  val auctionWithPrice = auctionDf.filter('price >100)

//  auctionWithPrice.show()

  /**
    * read sfpd.csv file with header
    *
    */

  val sfpdDf= spark.read.option("header", true).option("inferSchema", true)
    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/sfpd.csv")

//  sfpdDf.show()

//  sfpdDf.printSchema()

  /**
    * What are the top 10 Resolutions ?
    */

  val top10Resolution = sfpdDf.select('Resolution).groupBy('Resolution).count()
                              .orderBy('count.desc).limit(10)

 // top10Resolution.show()


  /**
    * What are the top 10 most incident Categories?
    */
  val top10Category = sfpdDf.select('Category).groupBy('Category).count()
                            .orderBy('count desc).limit(10)

  //  top10Category.show()

}
