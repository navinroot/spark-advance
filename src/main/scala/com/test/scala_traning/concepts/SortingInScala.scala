package com.test.scala_traning.concepts

object Test {

  def compareStrings(firstString: String, secondString: String, thirdString: String): String = {
    var list = scala.collection.mutable.ListBuffer(firstString,secondString,thirdString)
    list =  list.sorted(Ordering.String)

    println(list.mkString(""))

    ""
  }

  def compareEmployee(firstString: Employee, secondString: Employee, thirdString: Employee){
    var list = scala.collection.mutable.ListBuffer(firstString,secondString,thirdString)
    list =  list.sortWith((x,y)=> (x.id<y.id && x.name<y.name))

    println(list.mkString(""))
  }


  def main(args: Array[String]): Unit = {
    val e1 = Employee(1,"navin")
    val e2 = Employee(3,"navin3")
    val e3 = Employee(1,"navin2")
    compareStrings("one","two","three")
    compareEmployee(e1,e2,e3)
  }
}

case class Employee(id:Int,name:String)
