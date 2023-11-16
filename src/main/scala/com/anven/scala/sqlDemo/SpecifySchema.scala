package com.anven.scala.sqlDemo

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * 自定义DF Schema
 */
object SpecifySchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SpecifySchema")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val peopleRS = Seq("Michael, 29", "Andy, 30", "Justin, 19").toDS()

    // 构造Schema：StructField和StructType
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)



    spark.close()
  }
}
