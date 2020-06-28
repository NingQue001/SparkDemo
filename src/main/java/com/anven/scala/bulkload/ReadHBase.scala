package com.anven.scala.bulkload

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
// 隐式转换： Java集合类型在Scala操作时没有foreach方法，因此需要转换为Scala的集合类型
import scala.collection.JavaConversions._

object ReadHBase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local")
                        .setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[ImmutableBytesWritable]))

    val sparkContext = new SparkContext(sparkConf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "node01:2181,node02:2181,node03:2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "news")

    val hbaseRDD = sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.take(10).foreach(tuple => {
      val result = tuple._2
      printResult(result)
    })
  }

  def printResult(result: Result): Unit = {
    val cells = result.listCells

    for (cell <- cells) {
      printCell(cell)
    }
  }

  def printCell(cell: Cell): Unit = {
    val str =
      s"rowkey: ${Bytes.toString(CellUtil.cloneRow(cell))}, family:${Bytes.toString(CellUtil.cloneFamily(cell))}, " +
        s"qualifier:${Bytes.toString(CellUtil.cloneQualifier(cell))}, value:${Bytes.toString(CellUtil.cloneValue(cell))}, " +
        s"timestamp:${cell.getTimestamp}"
    println(str)
  }
}