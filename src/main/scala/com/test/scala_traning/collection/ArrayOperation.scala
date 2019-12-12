package com.test.scala_traning.collection

import scala.collection.mutable.ArrayBuffer

object ArrayOperation extends App {

  /**
    * Both Array and ArrayBuffer are mutable, which means that you can modify elements at particular indexes: a(i) = e
   *
   * ArrayBuffer is resizable, Array isn't. If you append an element to an ArrayBuffer, it gets larger. If you try to
   * append an element to an Array, you get a new array. Therefore to use Arrays efficiently, you must know its size
   * beforehand.
   *
   * Arrays are implemented on JVM level and are the only non-erased generic type. This means that they are the most
   * efficient way to store sequences of objects – no extra memory overhead, and some operations are implemented as
   * single JVM opcodes.
   *
   * ArrayBuffer is implemented by having an Array internally, and allocating a new one if needed. Appending is
   * usually fast, unless it hits a limit and resizes the array – but it does it in such a way, that the overall
   * effect is negligible, so don't worry. Prepending is implemented as moving all elements to the right and
   * setting the new one as the 0th element and it's therefore slow. Appending n elements in a loop is
   * efficient (O(n)), prepending them is not (O(n²)).
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
