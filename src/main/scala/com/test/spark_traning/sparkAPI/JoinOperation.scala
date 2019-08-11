package com.test.spark_traning.sparkAPI

import org.apache.spark.sql.SparkSession

object JoinOperation extends App {
  val r1 = Seq(Row(1, "A1"), Row(2, "A2"), Row(3, "A3"), Row(4, "A4")).toDS()

  private[this] implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")
  val r2 = Seq(Row(3, "A3"), Row(4, "A4"), Row(4, "A4_1"), Row(5, "A5"), Row(6, "A6")).toDS()
  val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

  println(" r1 Data")
  r1.show()

  println("r2 Data")
  r2.show()

  case class Row(id: Int, value: String)

  joinTypes foreach { joinType =>
    println(s"${joinType.toUpperCase()} JOIN")
    r1.join(right = r2, usingColumns = Seq("id"), joinType = joinType).orderBy("id").show()
  }
}