package com.test.scala_traning.collection

import scala.collection.mutable

object ListOperation extends App{

  /**
   * All the operation of immutable List
    *update is not possible in List
    *
    */

  val s = List(1, 2, 4, 3)

  // get first element of List
  println(s.head)
  var s1=List(7,9,8,9)

  // concat two List

  val s2= s ::: s1   // or s++s1

  println(s2)

  // find max and min in set
  println("max value" + s2.max+",\nmin value "+ s2.min)

  // finding common value between two sets
  val commonValueBetweenSets = s.intersect(s2)
  println("common value between s and s2 = "+commonValueBetweenSets)


  // add value to a list
    val s3 = s.::(3)

  println(s3)

  /**
    * ListBuffer is mutable part of array so all the CRUD operation we can do with this
    *
    *
    */
  val a = mutable.ListBuffer(1, 2, 3, 4)
  val b = mutable.ListBuffer(5, 6, 7, 8, 9)

  // add new element to ListBuffer
  a += 1
  print("printing ListBuffer after adding new element =")
  a.foreach(print)

  //delete an element from ListBuffer
  println()
  a-= (1,2)
  print("printing all the ListBuffer after deleting element (1,2) = ")
  a.foreach(print)

  // sort all the elements of array
  println()
  print("printing the sorted ListBuffer = ")
  a.sorted(Ordering.Int.reverse).foreach(print)



}
