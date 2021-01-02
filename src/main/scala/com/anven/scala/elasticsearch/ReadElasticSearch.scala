package com.anven.scala.elasticsearch

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object ReadElasticSearch {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    val sc = new SparkContext(sparkConf)

    val esRdd = sc.esRDD("trip/trip")

    val rdd1 = esRdd.map(t => {
      (t._1, t._2.get("departure"))
    }).collect().foreach(println)

    val rdd2 = esRdd.map(t => {
      (t._1, t._2.get("arrival"))
    }).collect().foreach(println)
  }
}
