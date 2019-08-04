package com.test.spark_traning.sparkAPI.rdd

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object AggregateByKeyAPI extends Serializable with App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  val sc= spark.sparkContext

  val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
  val data = sc.parallelize(keysWithValuesList)

  /**
    * let’s collect unique values per key.
    * Think of this as an alternative of calling someRDD.groupByKey().distinct()
    *
    * result :
    * (foo,CompactBuffer(A, A, A, A, B))
    * (bar,CompactBuffer(C, D, D))
    */

  val kv= data.map(x => x.split("=")).map(x=> (x(0),x(1)))


  val initialSet = mutable.HashSet.empty[String]
  val insidePartitionMerge = (v1: mutable.HashSet[String], v2: String) => v1 += v2
  val acrossPartitionMerge = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1++=p2

  val groupBy= kv.aggregateByKey(initialSet)(insidePartitionMerge,acrossPartitionMerge)

  groupBy.collect().foreach(println)

}
