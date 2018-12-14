package com.nuvola_tech.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window

//import org.apache.spark.sql.SparkSession.implicits._

case class UserActivityData(user: String, ts: Long, session: String)

object SessionUDWFDemo {
  def main(args: Array[String]): Unit = {
    val st: Long = System.currentTimeMillis()
    val one_minute: Int = 60 * 1000


    val d: Array[UserActivityData] = Array[UserActivityData](
      UserActivityData("user1", st, "f237e656-1e53-4a24-9ad5-2b4576a4125d"),
      UserActivityData("user2", st + 5 * one_minute, null),
      UserActivityData("user1", st + 10 * one_minute, null),
      UserActivityData("user1", st + 15 * one_minute, null),
      UserActivityData("user2", st + 15 * one_minute, null),
      UserActivityData("user1", st + 140 * one_minute, null),
      UserActivityData("user1", st + 160 * one_minute, null))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL user-defined Datasets aggregation example")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val sqlContext = spark.sqlContext


    val df = sqlContext.createDataFrame(sc.parallelize(d))

    val specs = Window.partitionBy($"user").orderBy($"ts".asc)
    val res = df.withColumn("newsession", SessionUDWF.calculateSession($"ts", $"session") over specs)

    df.show(20)

    res.show(20, false)
    res.explain()
  }

}
