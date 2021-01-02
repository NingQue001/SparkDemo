package com.anven.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val lines = sc.textFile("hdfs://node01:9000/user/spark/input/users.txt")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(" ")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      Row(id, name, age)
    })

    val structType: StructType = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, structType)
    bdf.registerTempTable("t_user")
    val result:DataFrame = sqlContext.sql("select * from t_user order by age asc")
    result.show()

    sc.stop()

  }
}
