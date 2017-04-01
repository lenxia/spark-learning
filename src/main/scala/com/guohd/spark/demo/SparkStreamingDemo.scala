package com.guohd.spark.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sun.management.Sensor

/**
  * Author : guohongdou
  * Created: 2017/3/11
  * Updated: 2017/3/11
  * Version: 0.0.0
  * Contact: 2219708253@qq.com
  * spark streaming 流式计算：
  * DStream(离散流):对多个rdd的抽象
  */
object SparkStreamingDemo {

  /**
    * 监听本地9999端口，启动客户端nc -lk 9999
    */
  def demo01(): Unit = {
    /*
        创建一个每隔1second中处理一个batch sparkStream流。
     */
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    // 1、监听tcp9999端口，从9999端口获取流式数据源
    val lines = ssc.socketTextStream("localhost", 9999)

    val pairs = lines.flatMap(line => line.split(" ")).map(word => (word, 1))

    // 单词计数
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

  /**
    * 监听文件目录
    */
  def demo02(): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("textStreaming").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5)) // 每隔5s处理一个batch，job的运行有spark-streaming控制
    val lineDStream = ssc.textFileStream("/Users/guohongdou/Projects/spark-learning/dataStream/") // 监听目录
    val pairs = lineDStream.flatMap(line => line.split(" "))
    val count = pairs.map(word => (word, 1)).reduceByKey(_ + _)
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    demo02()
    demo01()
  }

}
