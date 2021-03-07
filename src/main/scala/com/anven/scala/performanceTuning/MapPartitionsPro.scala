package com.anven.scala.performanceTuning

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 优化之使用 mapPartitions 替代 map
 * a）map对每个元素操作，如果需要频繁创建对象，则性能较查，如果JDBC时创建数据库连接
 * b）mapPartitions对分区的迭代器操作
 */
object MapPartitionsPro {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionsPro").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(1 to 9, 3)
    // 使用 mapPartitions
    val mpData = data.mapPartitions(iter => {
      var res = List[(Int, Int)]()
      while (iter.hasNext) {
        var cur = iter.next()
        res .::= (cur, cur * 2)
      }
      res.iterator
    })

    mpData.collect().foreach(println(_))
    sc.stop()
  }
}
