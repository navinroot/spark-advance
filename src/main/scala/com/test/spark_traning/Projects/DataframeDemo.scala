package com.test.spark_traning.Projects

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, count, datediff, max}

/**
 * Hello world!
 *
 */
object DataframeDemo extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext
  import spark.implicits._


  val postsDf = spark.read.option("header",true).option("inferSchema",true).option("delimiter","~")
        .csv("/home/navin/workspace/spark-in-action-VM-box/spark traning/first-edition/ch05/italianPosts.csv")


  val postsIdBody= postsDf.select('id,'body)

  val postsId = postsIdBody.drop('body)

  val italianoBodyCount = postsIdBody.filter('body contains "Italiano").count()

  val noAnswer = postsDf.filter(('postTypeId ===1) and ('acceptedAnswerId isNull))

  val renameOwnerUserId= postsDf.withColumnRenamed("ownerUserId","owner")

 // renameOwnerUserId.printSchema()


  val lessThan35Ratio = postsDf.filter('postTypeId ===1)
    .withColumn("ratio",'viewCount/'score)
    .where('ratio < 35 )

//  println(lessThan35Ratio.show())


  /**
    * lets find out the question which was active for largest amount of time
    *
    */
  val sol1 = postsDf.filter('postTypeId === 1)
    .withColumn("activeTime", datediff('lastActivityDate,'creationDate))
    .agg(max('activeTime))

 // println(sol1.first())


  /**
    * lets find out average score, maximum score and total number of question
    *
    */

  val sol2= postsDf.filter('postTypeId === 1).agg(max("score").alias("max score"),
    avg("score").alias("average score"), count("score").alias("count"))

  //  println(sol2.show())


  /**
    * Window function
   * find ownerUserId, acceptedAnswerId, score, max score per user
    *
    *
    */

  val sol3= postsDf.filter('postTypeId === 1)
    .select('ownerUserId, 'acceptedAnswerId , 'score, max("score")
    .over(Window.partitionBy("ownerUserId")).alias("maxPerUser"))

 // println(sol3.show())



  /**
    *find number of posts per author, associated tag and post id
    *
    *
    */

  val sol4 = postsDf.groupBy('ownerUserId,'tags,'postTypeId).count().orderBy('ownerUserId desc)

 // println(sol4.show())


  /**
    *  find the last activity date and the maximum post score per user
    *
    *
    */

  val sol5 = postsDf.groupBy('ownerUserId).agg(max('lastActivityDate).alias("last activity date"),
    max('score).alias("max score"))

  // println(sol5.show(false))


  /**
   * find the last activity date and the maximum post score per user where max score is greater than 5
    *
    *
    */

  val sol6 = postsDf.groupBy('ownerUserId).agg(max('lastActivityDate).alias("last activity date"),
    max('score).alias("max score")).where($"max score" > 5)

 //  println(sol6.show(false))

  /**
    *  rollup function use case
    *
    */

  val sol7 = postsDf.rollup('ownerUserId,'tags,'postTypeId).count().orderBy('ownerUserId)
 // println(sol7.show())

  /**
    *  cube function use case
    *
    */

  val sol8 = postsDf.cube('ownerUserId,'tags,'postTypeId).count().orderBy('ownerUserId)
  // println(sol8.show())


  /**
    *           Join postDf and votesDf by id and postId
    *
    *
    */

  val votesDf = spark.read.option("header",true).option("inferSchema",true).option("delimiter","~")
        .csv("/home/navin/workspace/spark-in-action-VM-box/spark traning/first-edition/ch05/italianVotes.csv")

//  votesDf.printSchema()

  val join1 = postsDf.join(votesDf, postsDf.col("id") === votesDf.col("postId"), "inner")
      .drop(votesDf.col("id")).drop(votesDf.col("postId"))

  println(join1.show())


}
