package com.test.spark_traning.sparkAPI.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, expr, sum}

object PivotAPI extends App {

  val spark= SparkSession.builder().appName("chapter 3 program").master("local[*]").getOrCreate()

  import spark.implicits._

  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  /**
    * using toDf convert Seq/List to dataframe directly with schemaInference
    *
    * root
    * |-- Product: string (nullable = true)
    * |-- Amount: integer (nullable = false)
    * |-- Country: string (nullable = true)
    */
  val df = data.toDF("Product","Amount","Country")

 // df.printSchema()


 // df.show()

  /**
    * get the total amount exported to each country of each product,
    * and sum of Amount
    */

  val totalMountExported= df.groupBy('Country,'product).agg(sum('Amount))

//  totalMountExported.show()
  /**
    * output:
    * +-------+-------+-----------+
    * |Country|product|sum(Amount)|
    * +-------+-------+-----------+
    * | Mexico|  Beans|       2000|
    * |  China|  Beans|       1500|
    * |    USA|Carrots|       1500|
    * |  China| Banana|        400|
    * |  China| Orange|       4000|
    * | Canada|Carrots|       2000|
    * | Canada| Banana|       2000|
    * |  China|Carrots|       1200|
    * |    USA| Orange|       4000|
    * |    USA|  Beans|       1600|
    * |    USA| Banana|       1000|
    * +-------+-------+-----------+
    */

  /**
    * get the total amount exported to each country of each product,
    * will do group by Product, pivot by Country, and sum of Amount
    */
  val totalAmountPivotExported = df.groupBy('Country).pivot('product)
    .agg(sum('Amount))

  totalAmountPivotExported.show()
  /**
    * output :
    * +-------+------+-----+-------+------+
    * |Country|Banana|Beans|Carrots|Orange|
    * +-------+------+-----+-------+------+
    * |  China|   400| 1500|   1200|  4000|
    * |    USA|  1000| 1600|   1500|  4000|
    * | Mexico|  null| 2000|   null|  null|
    * | Canada|  2000| null|   2000|  null|
    * +-------+------+-----+-------+------+
    */

  /**
    * get the total amount exported to each country of each product,
    * will do group by Product, pivot by Country, and sum  and average of Amount
    */
  val totalAndAverageAMountPivotExported= df.groupBy('Country).pivot('product)
    .agg(sum('Amount),avg('Amount))

 // totalAndAverageAMountPivotExported.show()
  /**
    * output :
    * +-------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+-----------------------------------+-----------------------------------+----------------------------------+----------------------------------+
    * |Country|Banana_sum(CAST(Amount AS BIGINT))|Banana_avg(CAST(Amount AS BIGINT))|Beans_sum(CAST(Amount AS BIGINT))|Beans_avg(CAST(Amount AS BIGINT))|Carrots_sum(CAST(Amount AS BIGINT))|Carrots_avg(CAST(Amount AS BIGINT))|Orange_sum(CAST(Amount AS BIGINT))|Orange_avg(CAST(Amount AS BIGINT))|
    * +-------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+-----------------------------------+-----------------------------------+----------------------------------+----------------------------------+
    * |  China|                               400|                             400.0|                             1500|                           1500.0|                               1200|                             1200.0|                              4000|                            4000.0|
    * |    USA|                              1000|                            1000.0|                             1600|                           1600.0|                               1500|                             1500.0|                              4000|                            2000.0|
    * | Mexico|                              null|                              null|                             2000|                           2000.0|                               null|                               null|                              null|                              null|
    * | Canada|                              2000|                            2000.0|                             null|                             null|                               2000|                             2000.0|                              null|                              null|
    * +-------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+-----------------------------------+-----------------------------------+----------------------------------+----------------------------------+
    */


  /**
    * unpivot a pivoted table
    *
    * : Unpivot is a reverse operation, by rotating column values into rows values.
    * Spark doesn’t have unpivot function hence will use the stack function.
    * Below code converts column countries to row.
    * Banana|Beans|Carrots|Orange|
    */

  val unpivotDf = totalAmountPivotExported.select('Country,
      expr("stack(4,'Banana',Banana,'Beans',Beans,'Carrots',Carrots,'Orange',Orange) " +
        "as (Product,Total)")).where('Total.isNotNull)

  unpivotDf.show()
  /**
    * output:
    * +-------+-------+-----+
    * |Country|Product|Total|
    * +-------+-------+-----+
    * |  China| Banana|  400|
    * |  China|  Beans| 1500|
    * |  China|Carrots| 1200|
    * |  China| Orange| 4000|
    * |    USA| Banana| 1000|
    * |    USA|  Beans| 1600|
    * |    USA|Carrots| 1500|
    * |    USA| Orange| 4000|
    * | Mexico|  Beans| 2000|
    * | Canada| Banana| 2000|
    * | Canada|Carrots| 2000|
    * +-------+-------+-----+
    *
    */

}
