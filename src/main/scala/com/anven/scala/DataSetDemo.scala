package com.anven.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataSetDemo").setMaster("local");
    val sc = new SparkContext(conf) //Spark 上下文
    val sqlContext = new SQLContext(sc) //创建SQL上下文
    import sqlContext.implicits._ //神仙操作：导入隐含类

    val products = sc.parallelize(Product(1, "food", "dumpling", 3) ::
      Product(2, "grocery", "toilet paper", 5) :: Nil) //如果不导入sqlContext的隐含类，则toDF会报错

    val productDF = products.toDF()
    productDF.registerTempTable("product")
    sqlContext.sql("select * from product").show()
    // 写入HDFS 文件格式是parquet
//    productDF.write.parquet("/usr/local/tmp/product.parquet")

    //read from parquet file
    val productParquetDF = sqlContext.read.parquet("/usr/local/tmp/product.parquet")
//    productParquetDF.registerTempTable("prodectParquet")
    productParquetDF.createOrReplaceTempView("prodectParquet")
    sqlContext.sql("select sum(price) from prodectParquet").show()
  }

  //Product类需要放在mian函数外面，要不然
  // ".toDF"会报错：toDF is not a member of org.apache.spark.rdd.RDD[Product]
  //Why？
  case class Product(id: Int, item: String, name: String, price: Int)
}
