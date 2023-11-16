package com.anven.scala.sqlDemo

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * 尚硅谷大数据Spark教程从入门到精通
 * https://www.bilibili.com/video/BV11A411L7CK?p=181&vd_source=836c8db8ca01f16e194502ab966468a7
 *
 * SparkSQL 项目实战之数据准备（https://blog.csdn.net/yangshengwei230612/article/details/117450747）
 */
object Spark06_SparkSQL_Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HelloSparkSQL")
      .master("local")
      // 下面的语句并不需要增加
      //      .config("spark.sql.warehouse.dir", "hdfs://hdp02:8020/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 准备数据
    //多行写sql的方式,在hive中创建表
//    spark.sql(
//      """
//        |CREATE TABLE `user_visit_action`(
//        |  `date` string,
//        |  `user_id` bigint,
//        |  `session_id` string,
//        |  `page_id` bigint,
//        |  `action_time` string,
//        |  `search_keyword` string,
//        |  `click_category_id` bigint,
//        |  `click_product_id` bigint,
//        |  `order_category_ids` string,
//        |  `order_product_ids` string,
//        |  `pay_category_ids` string,
//        |  `pay_product_ids` string,
//        |  `city_id` bigint)
//        |row format delimited fields terminated by '\t'
//            """.stripMargin)

    //加载本地数据
    spark.sql(
      """
        |load data local inpath 'file:///Users/anven/IdeaProjects/SparkDemo/src/main/resources/user_visit_action.txt' into table user_visit_action
            """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'file:///Users/anven/IdeaProjects/SparkDemo/src/main/resources/product_info.txt' into table product_info
            """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'file:///Users/anven/IdeaProjects/SparkDemo/src/main/resources/city_info.txt' into table city_info
            """.stripMargin)

    spark.sql("""select * from city_info""").show


    spark.close()
  }
}
