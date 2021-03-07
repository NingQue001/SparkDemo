package com.anven.scala.pro

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SkewDataPro {
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

    // 对倾斜数据加盐
    val addSaltPairs = pairs.map(x => {
      val prefix = Random.nextInt(10)
      (prefix + "_" + x._1, x._2)
    })
    // 对加盐后的数据进行局部聚合
    val localAggRdd = addSaltPairs.reduceByKey(_ + _)

    // 去盐
    val removeSaltRdd = localAggRdd.map(x => {
      val originKey = x._1.split("_")(1)
      (originKey, x._2)
    })

    // 对去盐后的数据进行全局聚合
    val globalAggRdd = removeSaltRdd.reduceByKey(_ + _)

    globalAggRdd.foreach(println(_))
  }
}
