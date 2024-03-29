package com.test.scala_traning.collection

import scala.collection.immutable.SortedMap
import scala.collection.{immutable, mutable}

object MapOperation extends App {

  val m1 = immutable.Map("Megha" -> 3, "Ruchi" -> 2, "Becky" -> 4)
  var m = immutable.Map("Ayushi" -> 0, "Megha" -> 4)
  /**
   * immutable map contains only unique keys
   *
   */

  val m2 = m ++ m1
  //  m1 += ("navin" -> 4)  [ NOT POSSIBLE BECAUSE OF VAL M1]
  m += ("navin" -> 3)
  m -= ("navin")
  m.getOrElse("navin","Navin Not found")


  // iterate on map using lambda
  m2.foreach {
    case (key, value) => println("key =" + key + ", value = " + value)
  }

  // iterate on map using normal for loop
  for( (k,v) <- m2){
    println("key =" + k + ", value = " + v)
  }

  // check if a key is present
  println(m.contains("navin"))
  m("navin")

  println(m2.toSet.mkString(" "))

  /**
   * mutable map contains only unique keys
   *
   */
  println("mutable map started")
  val mm = mutable.Map("Ayushi" -> 0, "Megha" -> 4)

  // add new element to same mutable map
  mm.put("Megha", 3)
  mm.put("Navin", 1)
  mm += ("navin+=" -> 3)
  println(mm.mkString(" "))

  // add or update same element to same mutable map
  mm.update("Navin", 2)
  println(mm.mkString(" "))

  // get element if existing else update/add it and return get/updated element.
  mm.getOrElseUpdate("Navin1", 4)
  println(mm.mkString(" "))

  // remove existing element from map
  mm.remove("Navin1")
  mm -= "navin+="
  println(mm.mkString(" "))

  // clear all the element for mutable map
  mm.clear()




  /**
   * synchronized mutable thread safe map
   *
   */

  val sychronizedMap = new mutable.HashMap[String, Int]() with mutable.SynchronizedMap[String, Int]
  sychronizedMap.put("navin", 1)
  println(sychronizedMap.mkString(" "))

}
