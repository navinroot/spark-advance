package com.test.scala_traning.concepts

object LazyEvaluation extends App{

  /**
    * lazy evaluation is only allowed with val  ( not with var)
    * lazy value only gets evaluated when we call lazy val in some action.
    *
    *
    */


  val list= List(1,2,3,4,5,6)
  lazy val square= list.map(x => x*x)

  println(square.mkString(" "))

}
