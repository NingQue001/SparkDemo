package com.anven.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object HiveDemo {
  def main(args: Array[String]): Unit = {
    //创建支持hive的SparkSession
    val spark = SparkSession.builder().appName("HiveDemo").master("local").
      config("hive.metastore.uris", "thrift://node01:9083").enableHiveSupport().getOrCreate();

//    spark.sql("CREATE TABLE IF NOT EXISTS emp(id Int,name String,age Int,status String) " +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

//    spark.sql("LOAD DATA LOCAL INPATH '/usr/local/tmp/emps.csv' INTO table emp")
    spark.sql("select * from emp").show()
    spark.sql("select * from employee_sample").show()
  }
}
