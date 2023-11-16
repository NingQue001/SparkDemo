package com.anven.scala.sqlDemo

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL: UDF
 */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HelloSparkSQL")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read.json("file:///usr/local/data/spark/people.json")
    df.createOrReplaceTempView("people")

    // 自定义函数
    // flink sql是否能搞？
    spark.udf.register("prefixName", (name: String) => {
      "UdfName: " + name
    })
    spark.udf.register("null2Zero", (age: Integer) => {
      age + 0
    })


    spark.sql("select prefixName(name), null2Zero(age) from people").show()

    spark.close()
  }
}
