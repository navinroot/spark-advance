package com.test.spark_traning.Projects

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.math._

object SkewJoinUsingSalting extends App {

  val conf = new SparkConf().setAppName("skew join using salting").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext

  val numPartition = 16
  // create the large skewed rdd
  val skewLargeRdd = sc.parallelize(0 to numPartition, numPartition)
    .flatMap(x => List(0 until (exp(x.toDouble)).toInt)).flatMap(x => x)

  // skewLargeRdd.glom().map(x => x.length).collect.foreach(println)
  /**
   * output :
   * 1
   * 2
   * 7
   * 20
   * 54
   * 148
   * 403
   * 1096
   * 2980
   * 8103
   * 22026
   * 59874
   * 162754
   * 442413
   * 1202604
   * 12155127
   */

  // put it in (key, value) form
  val skewedLarggeRdd = skewLargeRdd.mapPartitionsWithIndex((ind, itr) => {
    itr.map(x => (ind, x))
  })

  // skewedLarggeRdd.take(20).foreach(println)

  /**
   * output :
   * (0,0)
   * (1,0)
   * (1,1)
   * (2,0)
   * (2,1)
   * (2,2)
   * (2,3)
   * (2,4)
   */

  val smallRdd = sc.parallelize(0 to numPartition, numPartition).map(x => (x, x))

  // smallRdd.collect.foreach(println)
  /**
   * (0,0)
   * (1,1)
   * (2,2)
   * (3,3)
   * (4,4)
   * (5,5)
   * (6,6)
   * (7,7)
   * (8,8)
   * (9,9)
   */

  /**
   * normal join without removing skewness
   * total time taken - 37 seconds
   */
  val t0 = System.nanoTime()
  val resultOfJoin = skewedLarggeRdd.leftOuterJoin(smallRdd)
  println(resultOfJoin.count())
  val t1 = System.nanoTime()
  println("skew join without salting. Elapsed time: " + (t1 - t0) / 1e9 + "second")


  /**
   * Join with salting on skewed data
   *
   */
  val N = 30 //  parameter to control level of data replication
  val smallRddTransferred = smallRdd.cartesian(sc.parallelize(0 to N)).map(x => ((x._1._1, x._2), x._1._2))
    .repartition(numPartition).cache()
  // smallRddTransferred.collect.foreach(println)

  /**
   * output:
   * ((0,4),0)
   * ((0,20),0)
   * ((0,36),0)
   * ((0,52),0)
   * ((0,68),0)
   * ((0,84),0)
   * ((0,100),0)
   * ((1,12),1)
   * ((1,28),1)
   * ((1,44),1)
   */

  val skewedLargeRddTransformed = skewedLarggeRdd.map(x => {
    val rand = new scala.util.Random()
    ((x._1, rand.nextInt(30)), x._2)
  }).repartition(numPartition) // # add a random int to format  new key

  // skewedLargeRddTransformed.glom().map(x => x.length).collect.foreach(println)


  val t2 = System.nanoTime()
  val skewJoinResultUsingSalting = skewedLargeRddTransformed.leftOuterJoin(smallRddTransferred)
  println(skewJoinResultUsingSalting.count())
  val t3 = System.nanoTime()
  println("skew Joining with salting. Elapsed time: " + (t3 - t2) / 1e9 + " second")


  /**
   * NOTE - with salting the joining takes 1.5 second. ( it was taking 37 second without salting)
   *
   * big improvement. YO
   */

}
