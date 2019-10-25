package com.test.scala_traning

import scala.collection._

object SetOperation extends App{

  /**
   * immutable set contains unique value always
    * it doesn't sort elements
    *
    */

  val s = immutable.Set(1, 2, 4, 3)
  println("s[2] = "+ s(2))

  // get first element of set
  println(s.head)

  var s1=Set(7,9,8,9)

  // concat two sets

  val s2= s ++ s1

  println(s2)

  // find max and min in set
  println("max value" + s2.max+",\nmin value "+ s2.min)

  // finding common value between two sets
   val commonValueBetweenSets = s.intersect(s2)
  println("common value between s and s2 = "+commonValueBetweenSets)


  // add new element to set
   s1 += 3
  // remove element from set
  s1 -= (3,4)
  println(s1)

  // find if set contains an element
  println(s1.contains(3))

  // concat other collection
   var s3 = s1 ++ Array(10,11,12)
   s3 = s1 ++ List(10,11,12)
  println(s3)


  // set difference(elements existing in one set, but not in another)
  val diff= s.diff(s1)
  println(diff)

  /**
    * ------------------------------------------------------------------------
    * SORTED SET
    * it maintains sorted order in with +/- value both
    */

  val sortedSet = immutable.SortedSet(2, 3, 9, 8, -1, 1, -3)
  println(sortedSet)

  /**
    * --------------------------------------------------------------------------
    * Bit set
    *  it only accepts positive value ( with negative value it throws exception )
    *  it also maintains sorted order.
    */

  val bitSet = immutable.BitSet(2, 3, 9, 8, 1)
  println(bitSet)

  // --------------------------------- mutable set-------------------------------
  val ms = mutable.Set(1, 2, 4, 3)
  // add an element
  ms.add(5)
  ms += 6
  // remove an element
  ms.remove(4)
  ms -= 1
  println("mutable set operation")

  println(ms.mkString(" "))

  // --------------------------------- mutable sorted set ( same for Bit set)-------------------------------
  val mss = mutable.SortedSet(1, 2, 4, 3)
  // add an element
  mss.add(5)
  mss += 6
  // remove an element
  mss.remove(4)
  mss -= 1
  println("mutable sorted/bit set operation")

  println(mss.mkString(" "))
}
