package com.anven.scala.sqlDemo

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField, StructType}

/**
 * SparkSQL: UDAF
 */
object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HelloSparkSQL")
      .master("local")
      .getOrCreate()


    spark.sql("select * from json.`file:///usr/local/data/spark/people.json`").show()

    val df = spark.read.json("file:///usr/local/data/spark/people.json")
    df.createOrReplaceTempView("people")

    // 自定义函数
    // flink sql是否能搞？
    spark.udf.register("prefixName", (name: String) => {
      "UdfName: " + name
    })
    spark.udf.register("null2Zero", (age: Integer) => {
      age + 0
    })

    /**
     * 过时方法类：UserDefinedAggregateFunction，使用以下类替代：
     * org.apache.spark.sql.expressions.Aggregator
     */
//    spark.udf.register("myAvg", new MyAvgUDAF())
    spark.udf.register("myAvg2", functions.udaf(new MyAvgUDAF2()))


//    spark.sql("select prefixName(name), null2Zero(age) from people").show()
//    spark.sql("select myAvg(age) from people").show()
//    spark.sql("select age from people").show()
//    spark.sql("select myAvg2(age) from people").show()

    spark.close()
  }

  class MyAvgUDAF extends UserDefinedAggregateFunction{
    // 输入数据的结构
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区数据的结构
    override def bufferSchema: StructType =
    {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    // 函数计算数据结果的数据类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = 0L
//      buffer(1) = 0L

      buffer.update(0, 0L)
      buffer.update(0, 0L)
    }

    // 根据输入值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(input.get(0) != null) {
        val buffer0 = if (buffer.get(0) == null) 0L else buffer.getLong(0)
        val buffer1 = if (buffer.get(1) == null) 0L else buffer.getLong(1)
        val input0 = if (input.get(0) == null) 0L else input.getLong(0)
        buffer.update(0, buffer0 + input0)
        buffer.update(1, buffer1 + 1)
      }
    }

    // 合并缓冲区数据
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val buffer10 = if (buffer1.get(0) == null) 0L else buffer1.getLong(0)
      val buffer11 = if (buffer1.get(1) == null) 0L else buffer1.getLong(1)
      val buffer20 = if (buffer2.get(0) == null) 0L else buffer2.getLong(0)
      val buffer21 = if (buffer2.get(1) == null) 0L else buffer2.getLong(1)
      println(buffer10, buffer11, buffer20, buffer21)
      buffer1.update(0, buffer10 + buffer20)
      buffer1.update(1, buffer11 + buffer21)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

  /**
   * 自定义聚合函数类
   * org.apache.spark.sql.expressions.Aggregator
   */
  case class Buff(var total: Long, var count: Long)
  class MyAvgUDAF2 extends Aggregator[Long, Buff, Long] {
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
