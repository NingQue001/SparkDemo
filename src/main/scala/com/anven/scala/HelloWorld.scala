package com.anven.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Scala:
 * 1. 函数式编程
 * 2. 高阶函数，函数是第一等公民
 * 3. closures, 闭包
 *
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list: RDD[Int] = sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    list.foreach(println)

  }
}
