package com.guohd.spark.thread

import java.io.{File, PrintWriter}
import java.util.concurrent.{ExecutorService, Executors}

import scala.util.Random

/**
  * Author : guohongdou
  * Created: 2017/3/11
  * Updated: 2017/3/11
  * Version: 0.0.0
  * Contact: 2219708253@qq.com
  * scala 多线程
  */
class ThreadDemo(threadName: String, fileName: String) extends Runnable {

  override def run(): Unit = {

    val writer = new PrintWriter(new File("/Users/guohongdou/Projects/spark-learning/dataStream/" + fileName))

    val dataSource: String = "hadoop java spark shell"
    val words = dataSource.split(" ")
    for(i <- 0 to words.length){
      writer.write(words(Random.nextInt(words.length))+" ")
      writer.flush()
    }
    writer.close()

    println(threadName+" 写入数据成功....")
    Thread.sleep(5000)
  }
}

object ThreadPoolDemo {
  def main(args: Array[String]): Unit = {
    // 创建线程池
    val threadPool: ExecutorService = Executors.newFixedThreadPool(10)
    try {
      // 提交20个线程
      for (i <- 1 to 20) {
        threadPool.execute(new ThreadDemo("thread " + i, i+ ".txt"))
      }
    } finally {
      threadPool.shutdown()
    }

  }


}
