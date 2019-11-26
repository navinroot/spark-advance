package com.test.spark_traning.joins

/**
 * https://medium.com/@achilleus/https-medium-com-joins-in-apache-spark-part-1-dabbf3475690
 *
 * Currently, Spark offers
 * 1)Inner-Join - Inner join basically removes all the things that are not common in both the tables.
 *      It returns back all the data that has a match on the join condition from both sides of the table.
 *
 * 2) Left-Join - In a left join, all the rows from the left table are returned irrespective of whether there
 *      is a match in the right side table. If a matching id is found in the right table is found, it is returned
 *      or else a null is appended.
 *
 * 3)Right-Join - This is similar to Left join. In Right join, all the rows from the Right table are returned
 *      irrespective of whether there is a match in the left side table
 *
 * 4)Outer-Join -
 *
 * 5)Cross-Join -
 *
 * 6)Left-Semi-Join - A left semi join is the same as filtering the left table for only rows with keys
 *       present in the right table. it doesn't contain any data of right table like left join
 *
 *
 * 7)Left-Anti-Semi-Join - The left anti join also only returns data from the left table,
 *       but instead only returns records that are not present in the right table
 *
 *
 *
 *
 */



object AllAboutJoins {

}
