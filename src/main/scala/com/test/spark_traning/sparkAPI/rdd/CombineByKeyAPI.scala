package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.sql.SparkSession

object CombineByKeyAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext

  /**
    * we have a RDD of studentName, subjectName and marks and we want to get the percentage of all students.
    */

  val studentRDD = sc.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
    ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
    ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
    ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
    ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
    ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
    ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
    ("Juan", "Biology", 60)), 3).map(x=> (x._1,(x._2,x._3)))


  val initiate = (tuple2: (String, Int)) => (tuple2._2.toDouble, 1)
  val combiner= (acc:(Double,Int), value:(String,Int)) => ( acc._1+value._2,acc._2+1)
  val merger= (v1:(Double,Int),v2:(Double,Int))=> (v1._1+v2._1,v1._2+v2._2)

  val findPercentage = studentRDD.combineByKey(initiate, combiner, merger).map(x => (x._1, x._2._1 / x._2._2))

  findPercentage.collect().foreach(println)


}
