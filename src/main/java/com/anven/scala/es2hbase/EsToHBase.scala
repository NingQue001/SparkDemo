package com.anven.scala.es2hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object EsToHBase {
  val zookeeperQuorum = "node01:2181,node02:2181,node03:2181";
  val hdfsRootPath = "hdfs://node01:9000/usr/mac";
  val hFilePath1 = "hdfs://node01:9000/usr/mac/hbase/bulkload/hfile/departure";
  val hFilePath2 = "hdfs://node01:9000/usr/mac/hbase/bulkload/hfile/arrival";
  val tableName = "news";
  val familyName = "trip";
  val qualifiername = "title";

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    val sc = new SparkContext(sparkConf)

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsRootPath)
    val fileSystem = FileSystem.get(hadoopConf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin

    // 1. 准备程序运行的环境
    // 如果 Hbase 表不存在，就创建一个新表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor(familyName)
      desc.addFamily(hcd)
      admin.createTable(desc)
    }

    // 如果存放 HFile 文件的路径已经存在，就删除掉
    if (fileSystem.exists(new Path(hFilePath1))) {
      fileSystem.delete(new Path(hFilePath1), true)
    }
    if (fileSystem.exists(new Path(hFilePath2))) {
      fileSystem.delete(new Path(hFilePath2), true)
    }

    val esRdd1 = sc.esRDD("trip/trip")
      .map(t => {
        (t._1, t._2.get("departure").toString)
      })
      .sortByKey()
      .map(tuple => {
        val kv = new KeyValue(Bytes.toBytes(tuple._1)
          , Bytes.toBytes(familyName) // family name
          , Bytes.toBytes("departure")  // qualifier name
          , Bytes.toBytes(tuple._2))
        (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), kv)
      })

    val esRdd2 = sc.esRDD("trip/trip")
      .map(t => {
        (t._1, t._2.get("arrival").toString)
      })
      .sortByKey()
      .map(tuple => {
        val kv = new KeyValue(Bytes.toBytes(tuple._1)
          , Bytes.toBytes(familyName) // family name
          , Bytes.toBytes("arrival") // qualifier name
          , Bytes.toBytes(tuple._2))
        (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), kv)
      })

    // 3. Save HFile on HDFS
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)

    esRdd1.saveAsNewAPIHadoopFile(
      hFilePath1,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    esRdd2.saveAsNewAPIHadoopFile(
      hFilePath2,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    // 4. Bulk load HFiles to HBase
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path(hFilePath1), admin, table, regionLocator)
    bulkLoader.doBulkLoad(new Path(hFilePath2), admin, table, regionLocator)

    hbaseConn.close()
    fileSystem.close()
    sc.stop()
  }
}
