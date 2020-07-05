package com.anven.scala.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object WriteElasticSearch {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    val sc = new SparkContext(sparkConf)

//    val numbers = Map("one" -> 1, "tow" -> 2, "three" -> 3)
//    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//
//    sc.makeRDD(
//      Seq(numbers, airports)
//    ).saveToEs("spark/doc")

    // use EsSpark to insert record to Es
    case class Trip(departure: String, arrival: String)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "NY")

    val rdd = sc.makeRDD(
      Seq(upcomingTrip, lastWeekTrip)
    )
    EsSpark.saveToEs(rdd, "spark/doc")
  }
}
