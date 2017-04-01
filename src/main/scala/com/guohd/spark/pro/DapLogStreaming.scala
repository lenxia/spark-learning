package com.guohd.spark.pro

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Author : guohongdou
  * Created: 2017/3/12
  * Updated: 2017/3/12
  * Version: 0.0.0
  * Contact: 2219708253@qq.com
  * 通过spark-streaming，spark-sql
  * 以60秒为间隔，统计60秒内的pv,ip数,uv
  * 最终结果包括：
  * 时间点：pv：ips：uv
  * 数据日志格式：
  * 2015-11-11T14:59:59|~|xxx|~|202.109.201.181|~|xxx|~|xxx|~|xxx|~|B5C96DCA0003DB546E7
  * 2015-11-11T14:59:59|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|B1611D0E00003857808
  * 2015-11-11T14:59:59|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|1555BD0100016F2E76F
  * 2015-11-11T15:00:00|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|C0EA13670E0B942E70E
  * 2015-11-11T15:00:00|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|C0EA13670E0B942E70E
  * 2015-11-11T15:00:01|~|xxx|~|125.119.144.252|~|xxx|~|xxx|~|xxx|~|4E3512790001039FDB9
  *
  * 每条日志包含7个字段，分隔符为|~|，其中，第3列为ip，第7列为cookieid。
  * 假设原始日志已经由Flume流到Kafka中。
  */
case class DapLog(day: String, ip: String, cookieID: String)

// 单例SQLContext
object SQLContextSingleton {
  // 不需要序列化
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

object DapLogStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("DapLogStreaming")
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      zkQuorum = "host1:2181,host2:2181",
      groupId = "DapLogStreaming",
      topics = Map[String, Int]("daplog" -> 0, "daplog" -> 1),
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER
    ).map(x => x._2.split("\\|~\\|", -1)) // 日志以|~|为分隔符

    kafkaStream.foreachRDD((rdd: RDD[Array[String]], time: Time) => {
      /*rdd.foreachPartition(part =>{


      })*/
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: DapLog,提取日志中相应的字段
      val logDataFrame = rdd.map(w => DapLog(w(0).substring(0, 10),w(2),w(6))).toDF()
      //注册为tempTable
      logDataFrame.registerTempTable("daplog")
      //查询该批次的pv,ip数,uv
      val logCountsDataFrame =
      sqlContext.sql("select " +
        "date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as time," +
        "count(1) as pv,count(distinct ip) as ips," +
        "count(distinct cookieid) as uv " +
        "from daplog")
      //打印查询结果
      logCountsDataFrame.show()
    })

    // 开始计算
    ssc.start()
    ssc.awaitTermination()
  }
}
