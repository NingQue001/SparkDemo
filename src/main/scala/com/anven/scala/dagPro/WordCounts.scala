package com.anven.scala.dagPro

import org.apache.spark.{SparkConf, SparkContext}

/**
 * DAG生成stages示例
 */
object WordCounts {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCounts").setMaster("local")
    val sc = new SparkContext(conf)

    // DAG 生成stages过程
    // 1. sc.textFile 生成第一个 MapPartitionsRDD
    val lines = sc.textFile("file:///usr/local/data/spark/word.txt", 1)
    // 2. flatMap 生成第二个 MapPartitionsRDD
    val words = lines.flatMap(line => line.split(" "))
    // 3. map 生成第三个 MapPartitionsRDD
    val pairs = words.map(word => (word, 1))
    // 4. reduceByKey: local阶段生成第四个 MapPartitionsRDD，reduceByKeyShuffle生成 ShuffledRDD
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(s => println(s))
  }

}
