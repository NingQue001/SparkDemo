package com.anven.scala.businessAction

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDD_Movie_Users_Analyzer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RDD_Movie_Users_Analyzer")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("Debug")

    var dataPath = "file:///usr/local/data/spark/" //数据存放的目录；
    //把用到的数据加载进来转换为RDD，此时使用sc.textFile并不会读取文件，而是标记了有这个操作，在遇到Action级别算子时才会正真去读取文件。
    // users.dat UserID::Gender::Age::Occupation::Zip-code
    // ratings.dat UserID::MovieID:Rating::Timestamp
    // movies.dat MovieID::Title:Genres
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")

    // 获取排名前十的电影
    /*具体数据处理的业务逻辑 */
    println("所有电影中平均得分最高（口碑最好）的电影:")
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2)))
    val movies = moviesRDD.map(_.split("::")).map(x => (x(0), x(1)))

    val movieAndRating = ratings.map(x => (x._2, (x._3.toDouble, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val avgRating = movieAndRating.map(x => (x._1, x._2._1 / x._2._2))
    avgRating
      .join(movies)
      .map(x => (x._2._1, x._2._2))
      .sortByKey(false)
      .take(15)
      .foreach(x => println(x._2 + " 评分为：" + x._1))
    // 关闭SparkSession
    spark.stop()
  }
}
