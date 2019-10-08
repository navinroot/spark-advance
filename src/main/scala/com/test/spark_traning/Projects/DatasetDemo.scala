package com.test.spark_traning.Projects

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, max, rank}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.SizeEstimator

object DatasetDemo extends App{

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()
  import spark.implicits._
  val sc= spark.sparkContext

  case class Person(name: String, age: Int, personid : Int)

  case class Profile(name: String, personid  : Int , profileDescription: String)

  val df7 = spark.createDataFrame(
    Person("Bindu",20,  2)
      :: Person("Raphel",25, 5)
      :: Person("Ram",40, 9):: Nil).as[Person]


  val df6 = spark.createDataFrame(
    Profile("Spark",2,  "SparkSQLMaster")
      :: Profile("Spark",5, "SparkGuru")
      :: Profile("Spark",9, "DevHunter"):: Nil
  ).as[Profile]

  val pivotdf = df6.groupBy($"name").pivot("personId").count()

  //pivotdf.show()

  val ds= spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9),2)

 // println(ds.getNumPartitions)

  /**
    * remove first row of the ds1
    *
    */
  val ds1=ds.mapPartitionsWithIndex( (index,iterator) =>{
    if(index == 0){
       iterator.drop(1)
    }else{
      iterator
    }
  }
  )

//  println(ds1.count())

  //---------------------

  /**
    * get second element of df dataset
    *
    * @param c1
    * @param c2
    */
  case class Record(c1: String, c2: String)
  val data = List(Record("a", "b"), Record("b", "c"), Record("c", "d"))
  val df8 = data.toDF("c1", "c2").as[Record]
  import spark.implicits._

  val df1 = df8.rdd.zipWithIndex().filter(x => x._2 == 2).map(x => (x._1.c1, x._1.c2)).toDF().as[Record]

  // df1.collect().foreach(println)

  //------------------- get sample data using pairRdd


  val seq = Seq(
    (2147481832,23355149,1),(2147481832,973010692,1),(2147481832,2134870842,1),(2147481832,541023347,1),
    (2147481832,1682206630,1),(2147481832,1138211459,1),(2147481832,852202566,1),(2147481832,201375938,1),
    (2147481832,486538879,1),(2147481832,919187908,1),(214748183,919187908,1),(214748183,91187908,1)
  )

  val rddPair= spark.sparkContext.parallelize(seq,5).map(x=>(x._1,x))

  val fraction = rddPair.map(_._1).distinct().map(x=> (x,0.5)).collectAsMap()

  val sampleRdd= rddPair.sampleByKeyExact(false,fraction)

//  println(rddPair.count())
//
//  println(sampleRdd.count())

  //------------------------------------------- reduceBykey operation

val df9= spark.read
  .option("header",true)
  .option("escape", "\"")
  .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/1.csv")


 // println(df9.show())

 // df9.write.option("quoteAll",true).option("header",true)
  //    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/scala/com/test/output")

  //---------------------------------------
  val matches = spark.sparkContext.parallelize(Seq(
    Row(1, "John Wayne", "John Doe"),
    Row(2, "Ive Fish", "San Simon")))

  val players = spark.sparkContext.parallelize(Seq(
    Row("John Wayne", 1986),
    Row("Ive Fish", 1990),
    Row("San Simon", 1974),
    Row("John Doe", 1995)
  ))

  val matchesDf = spark.createDataFrame(matches, StructType(Seq(
    StructField("matchId", IntegerType, nullable = false),
    StructField("player1", StringType, nullable = false),
    StructField("player2", StringType, nullable = false)))
  )

  val playersDf = spark.createDataFrame(players, StructType(Seq(
    StructField("player", StringType, nullable = false),
    StructField("birthYear", IntegerType, nullable = false)
  )))

  val joinResult= matchesDf.join(playersDf,playersDf.col("player")===matchesDf.col("player1"))
      .drop('player).withColumnRenamed("birthYear","birth_p1")
      .join(playersDf,playersDf.col("player")<=>matchesDf.col("player2")).drop('player)
      .withColumnRenamed("birthYear","birth_p2")
    .withColumn("diffInBirthYear", abs('birth_p1 - 'birth_p2))


 // println(joinResult.show())


  //------------------------------------------

  val df= spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("/home/navin/workspace/intelliz-workspace/nestedArticture/src/main/resources/1.csv")

//  product,category,revenue
  val df2= df.select('product,'category,'revenue,
    rank().over(Window.partitionBy('category).orderBy('revenue desc)).alias("best_selling"))
    .where('best_selling <=2)


  //  df2.show()

val df3= df.select('product,'category,'revenue,
  max('revenue).over(Window.partitionBy('category)).alias("max_revenue"))
    .withColumn("diff",abs('revenue - 'max_revenue)).orderBy('category desc)


 // df3.show()
  //----------------------------------------
  // get particular index of a column

  val index= df.rdd.zipWithIndex().filter(x=> x._2 == 5).map(x => x._1)
  //  index.foreach(println)


  //---------------------------find size of dataframe
  val size=SizeEstimator.estimate(df)
 // println(size)

//------------------------------------------------------------------------------



  spark.stop()
}
