package com.test.scala_traning

object MapOperation extends App{

  /**
    * map contains only unique keys
    *
    */

  var m=Map("Ayushi"->0,"Megha"->4)
  var m1=Map("Megha"->3,"Ruchi"->2,"Becky"->4)

  m += ("navin"-> 3)
  m-= ("navin")

  val m2= m++m1

  m2.foreach{
    case (key,value) => println("key =" +key+", value = "+value)
  }

  // check if a key is present
  println(m.contains("navin"))
}
