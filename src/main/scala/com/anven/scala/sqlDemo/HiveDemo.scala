package com.anven.scala.sqlDemo

import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * SparkSQL连接hive：
 * 1、拷贝配置文件到resources：hdfs-site.xml, core-site.xml 和 hdfs-site.xml
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HelloSparkSQL")
      .master("local")
      // 下面的语句并不需要增加
//      .config("spark.sql.warehouse.dir", "hdfs://hdp02:8020/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

//    spark.sql("show tables;").show();
    spark.sql("select * from people").show();

    spark.close()
  }
}
