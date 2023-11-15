package com.anven.scala.sqlDemo

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL第一课：
 * 类型转换
 */
object HelloSparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HelloSparkSQL")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    // RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(Seq(("zhansan", 16), ("LiSi", 18)))
    val df = rdd.toDF("name", "age")
    df.printSchema()
    df.show()

    spark.close()
  }
}
