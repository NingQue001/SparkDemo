package com.anven.scala.sqlDemo

import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * SparkSQL: UDF
 */
object JDBCDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HelloSparkSQL")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val url = "jdbc:mysql://localhost:3306/test"
    val props = new Properties()
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    props.put("user", "root")
    props.put("password", "bigdata40k")
//    props.put("fetchSize", "100")

    val df = spark.read.json("file:///usr/local/data/spark/people.json")
    df.write.mode("append").jdbc(url, "people", props)

    spark.read.jdbc(url, "people", props).show()

    spark.close()
  }
}
