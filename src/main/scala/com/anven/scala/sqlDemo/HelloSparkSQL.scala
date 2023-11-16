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

    //1. RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(Seq(("zhansan", 16), ("LiSi", 18)))
    val df = rdd.toDF("name", "age")
//    df.printSchema()
//    df.show()
    val rdd1 = df.rdd

    //2. DataSet <=> DataFrame
    val ds = df.as[User]
//    ds.show()
    val df2 = ds.toDF()
//    df2.show()

    //2. RDD <=> DataSet
    val rdd2ds = rdd.map {
      case (name, age) => {
        User(name, age)
      }
    }.toDS()
//    rdd2ds.show()
    val ds2Rdd = rdd2ds.rdd

    spark.close()
  }

  case class User(name: String, age: Integer)
}
