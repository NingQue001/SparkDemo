package com.anven.scala.sqlDemo

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object SparkSQLExample {
  // 样例类
  case class People(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    // sparkSession 相当与Spark Shell里的 spark 变量
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .master("local")
      .getOrCreate()

    // 引入隐式转换类
    import sparkSession.implicits._

    val df: Dataset[Row] = sparkSession.read.json("file:///usr/local/data/spark/people.json")
//    df.printSchema() // 打印Schema
//    df.show()
//    df.select($"name", $"age" + 10).show()
    df.createOrReplaceTempView("people") // 创建临时视图
    val sqlDF = sparkSession.sql("SELECT name, age FROM people")
    sqlDF.show()

    // parquet 列式存储
    // 读取parquet生成 dataset
    val people: Dataset[People] = sparkSession.read.parquet("file:///usr/local/data/spark/people.parquet").as[People]
    // people.show()
//    people.createOrReplaceTempView("people2")
    people.createGlobalTempView("people2")
    // GlobalView使用时必须带前缀 global_temp！！！
    val countDF = sparkSession.sql("select count(*) from global_temp.people2")
    countDF.show()
  }
}
