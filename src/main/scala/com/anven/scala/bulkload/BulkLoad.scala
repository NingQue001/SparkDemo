package com.anven.scala.bulkload

import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * BulkLoad：批量导入，减少HBase服务器的压力
 */
object BulkLoad {
  val zookeeperQuorum = "node01:2181,node02:2181,node03:2181";
  val datasourcePath = "/usr/local/tmp/news_profile_data.txt";
  val hdfsRootPath = "hdfs://node01:9000/usr/mac";
  val hFilePath = "hdfs://node01:9000/usr/mac/hbase/bulkload/hfile";
  val tableName = "news";
  val familyName = "cf1";
  val qualifiername = "title";

  def main(args: Array[String]): Unit = {
//    HRegion

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(s"${this.getClass.getSimpleName}")
    val sparkContext = new SparkContext(sparkConf)
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsRootPath)
    val fileSystem = FileSystem.get(hadoopConf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    // 创建HBase的Admin，通过Admin操作HBase
    val admin = hbaseConn.getAdmin

    // 1. 准备程序运行的环境
    // 如何 Hbase 表不存在，就创建一个新表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      // 表
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      // 列族
      val hcd = new HColumnDescriptor(familyName)
      desc.addFamily(hcd)
      admin.createTable(desc)
    }

    // 如果存放 HFile 文件的路径已经存在，就删除掉
    if (fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
    }

    // 2. 清洗需要存放到 HFile 中的数据，rowkey 一定要排序， 否则会报错
    val data = sparkContext.textFile(datasourcePath)
      .map(jsonStr => {
        val jsonObject = JSON.parseObject(jsonStr)
        val rowKey = jsonObject.get("id").toString.trim
        val title = jsonObject.get("title").toString.trim
        (rowKey, title)
      })
      // sortByKey 会Shuffle
      .sortByKey()
      .map(tuple => {
        val kv = new KeyValue(Bytes.toBytes(tuple._1)
          , Bytes.toBytes(familyName)
          , Bytes.toBytes(qualifiername)
          , Bytes.toBytes(tuple._2))
        (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), kv)
      })

    // 3. Save HFile on HDFS
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)

    // 保存文件到HDFS
    data.saveAsNewAPIHadoopFile(
      hFilePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    // 4. Bulk load HFiles to HBase
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)

    hbaseConn.close()
    fileSystem.close()
    sparkContext.stop()
  }
}
