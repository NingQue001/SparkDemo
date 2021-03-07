package com.anven.scala.streamingdemo

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * 创建DStream接收Kafka的数据流
 */
object DirectStreamFromKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "garbage",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => {
      println("kafka: " + record.key())
      (record.key(), record.value())
    })
  }
}
