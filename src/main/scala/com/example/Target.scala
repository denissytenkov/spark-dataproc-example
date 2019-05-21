package com.example

import org.apache.spark.SparkContext


object Target {
  def main(args: Array[String]): Unit = {
    var sc = SparkContext.getOrCreate()
    val count = sc. parallelize(1 to 100000).filter { _ =>
      val x = math.random
      val y = math.random
      x*x + y*y < 1
    }.count()
    println(s"Hit the target ${count}")
  }

}
