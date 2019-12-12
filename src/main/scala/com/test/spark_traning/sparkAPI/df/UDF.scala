package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDF extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._

  val dataDf = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")).toDF("fruit","price",
  "location")

  val toLowerCase = (x:String,y:Int) => x.toLowerCase.take(y)

  val toLowerCaseUDF = spark.udf.register("toLowerCaseUDF",toLowerCase)

  val toLowerCaseUDF2 = udf((s:String,y:Int) => s.toLowerCase.take(y))

  val dataWithUDF = dataDf.withColumn("sub_fruit",toLowerCaseUDF('fruit,lit(2)))


  dataWithUDF.show()








}
