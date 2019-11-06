package com.test.scala_traning.collection

import scala.collection.mutable.ArrayBuffer

object ArrayOperation extends App {

  /**
    * in Array deletion is not possible because Array is immutable
    *
    *
    */

  var array = Array.apply(1,3,2,4,3,6)

  array.foreach(print)

  //update an element in array
  array(2)=10

  println("")
  array.foreach(print)

  println("")
  array.foreach(print)

  /**
    * ArrayBuffer is mutable part of array so all the CRUD operation we can do with this
    *
    *
    */
  val a = ArrayBuffer(1,2,3,4)
  val b= ArrayBuffer(5,6,7,8,9)

  // add new element to arrayBuffer
  a += 1
  print("printing ArrayBuffer after adding new element =")
  a.foreach(print)

  //delete an element from arrayBuffer
  println()
  a-= (1,2)
  print("printing all the ArrayBuffer after deleting element (1,2) = ")
  a.foreach(print)

  // sort all the elements of array
  println()
  print("printing the sorted in reverse order ArrayBuffer = ")
  a.sorted(Ordering.Int.reverse).foreach(print)


}
