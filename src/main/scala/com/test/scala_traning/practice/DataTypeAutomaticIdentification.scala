package com.test.scala_traning.practice

object DataTypeAutomaticIdentification {

  def identifyDatatypeUsingDef(any:Any):Unit={
    any match {
      case i:Int => println("Integer")
      case str:String => println("String")
      case f:Float => println("Float")
      case d:Double => println("Double")
      case seq: Seq[Int] => println("Sequence of String")
      case _ => println("can't indentify")

    }
  }

  val identifyDatatypeUsingVal: (Any) => Unit = {
      case i:Integer => println("Integer")
      case str:String => println("String")
      case f:Float => println("Float")
      case d:Double => println("Double")
      case list: List[Int] => println("List of int")
      case seq: Seq[Int] => println("Sequence of int")
      case _ => println("can't indentify")
  }


  def main(args: Array[String]): Unit = {
    identifyDatatypeUsingDef("gfhfjgh5678")
    identifyDatatypeUsingVal(List(1,2,3))




  }





}
