package com.test.scala_traning.concepts

object StringInterpolation extends App {

  /**
    * s String Interpolator
    */

  val name= "Navin"
   println(s" Hello $name, How are you")


  /**
    * f String Interpolator
    *  problem with s string interpolation is that it
    */

  val floatVal= 77.000
  println(s"floatVal is $floatVal using s string interpolation")
  println(f"floatVal is $floatVal%.3f using f string interpolation")

  /**
    * raw String Interpolator
    *
    * raw is like s, except that it doesn't escape literals within a string.
    */

  println(raw"$name says value is $floatVal\nOkay, bye")

}
