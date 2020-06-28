package com.anven.scala.bulkload

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Description: spark 通过内置算子写数据到 HBase：使用 saveAsNewAPIHadoopDataset()
 * 通过读取HDFS的文件，使用TableOutputFormat来作为格式化的类
 */
object WriteHBaseWithNewHadoopAPI {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
                        .setAppName(s"${this.getClass.getSimpleName}")
                        .setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val input = sparkContext.textFile("/usr/local/tmp/news_profile_data.txt")

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "node01:2181,node02:2181,node03:2181")
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin

    val jobConf = new JobConf(hbaseConf, this.getClass)
    // 设置表名
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "news")

    // 如果表不存在则创建表
    if (!admin.tableExists(TableName.valueOf("news"))) {
      val desc = new HTableDescriptor(TableName.valueOf("news"))
      val hcd = new HColumnDescriptor("cf1")
      desc.addFamily(hcd)
      admin.createTable(desc)
    }

    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    // 使用TableOutputFormat来做输出类的格式化
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val data = input.map(jsonStr => {
      println(s"#======content: " + jsonStr)
      // 处理数据的逻辑
      val jsonObject = JSON.parseObject(jsonStr)
      val newsId = jsonObject.get("id").toString.trim
      val title = jsonObject.get("title").toString.trim
      val put = new Put(Bytes.toBytes(newsId))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("title"), Bytes.toBytes(title))
      (new ImmutableBytesWritable, put)
    })

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sparkContext.stop()

  }
}
