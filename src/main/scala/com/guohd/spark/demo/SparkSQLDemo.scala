package com.guohd.spark.demo

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author : guohongdou
  * Created: 2017/3/10
  * Updated: 2017/3/10
  * Version: 0.0.0
  * Contact: 2219708253@qq.com
  * spark sql初探：
  * 简介：SparkSQL引入了一种新的RDD——SchemaRDD，SchemaRDD由行对象（Row）以及描述行对象中每列数据类型的Schema组成；
  * SchemaRDD很象传统数据库中的表。SchemaRDD可以通过RDD、Parquet文件、JSON文件、或者通过使用hiveql查询Hive数据来建立；
  * SchemaRDD除了可以和RDD一样操作外，还可以通过registerTempTable注册成临时表，然后通过SQL语句进行操作。
  * 总结：spark sql大致分为两类：
  * 一类是使用Case Class定义RDD，另一类是使用applySchema定义RDD
  *
  * 文件格式如下（）
  * name|age |salary
  * ----------------
  * Jary|23|500
  * Mary|24|1000f
  * Bob|22|1500
  * Lenxia|26|10000
  * Jack|25|5000
  * HaHa|30|100000
  * LiLi|20|1000
  */
//scheme
case class Person(name: String, age: Int, salary: Double)

object SparkSQLDemo {

  /**
    * 采用case class指定rdd，最大列数是有限制的(32列)
    */
  def demo01(): Unit = {
    val conf = new SparkConf().setAppName("demo01").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    //RDD隐式转换成DataFrame
    import sqlCtx.implicits._
    val user = sc.textFile("/Users/guohongdou/Projects/spark-learning/data/user").map(line => line.split("\\|"))
      .map(p => Person(p(0), p(1).toInt, p(2).toDouble)).toDF()

    // 注册表
    user.registerTempTable("tbl_user")

    /*
        需求1：
        +--------+---+--------+
        |    name|age|  salary|
        +--------+---+--------+
        |  "Mary"| 24|  1000.0|
        |   "Bob"| 22|  1500.0|
        |"Lenxia"| 26| 10000.0|
        |  "Jack"| 25|  5000.0|
        |  "HaHa"| 30|100000.0|
        |  "LiLi"| 20|  1000.0|
        +--------+---+--------+
     */
    //    sqlCtx.sql("select *  " +
    //      "from tbl_user " +
    //      "")
    //      .toDF().show()

    /*
      需求2：按照薪水分组降序排列
     */

    sqlCtx.sql("select name ,age,salary as s  " +
      "from tbl_user group by salary,name,age" +
      "")
      .toDF().sort($"s".desc) show()

  }

  /**
    * 当样本类不能提前确定（例如，记录的结构是经过编码的字符串，或者一个文本集合将会被解析，不同的字段投影给不同的用户），
    * 一个schemaRDD可以通过三步来创建。
    * 1、从原来的RDD创建一个行的RDD
    * 2、创建由一个structType表示的schema匹配第一步创建的RDD的行结构
    * 3、通过SQLContext提供的applySchema方法应用这个schema到行的RDD
    */
  def demo02(): Unit = {
    val conf = new SparkConf().setAppName("demo01").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    val user = sc.textFile("/Users/guohongdou/Projects/spark-learning" + "/data/user").map(line => line.split("\\|"))

    // 这里着重说明一下！！！
    // 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换,貌似1.6以后的版本不用
    //    import sqlCtx.implicits._

    // 1、指定schemeString
    val schemaString = "name\tage\tsalary"

    // 2、通过schemeString创建scheme
    val schema = StructType(schemaString.split("\\t").map(fieldName => StructField(fieldName, StringType, true)))

    /*val scheme2 = StructType(
      Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
        StructField("salary",DoubleType,true)
      )
    )*/

    // 3、创建RowRdd
    val rowRDD = user.map(p => Row(p(0), p(1), p(2).trim))

    // 4、将scheme应用到rowRDD上
    val userSchemaRDD = sqlCtx.createDataFrame(rowRDD, schema)

    // 5、注册表
    userSchemaRDD.registerTempTable("tbl_user")

    /*
        构建用户自定义函数
     */
    sqlCtx.udf.register("getLen",(x:String) => x.length)


    // 6、使用查询语句
    val results = sqlCtx.sql("select getlen(name) as len,* from tbl_user")


    results.show()
    // 获取表第一列(name)信息
//    results.map(record => record(0)).collect().foreach(println)

  }

  /**
    * spark sql join(三种形式，对应mr join)
    *
    */
  def demo03(): Unit = {
    // 1、初始化spark环境及作业信息
    val conf = new SparkConf().setAppName("demo_join").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

     // 注册第一张表
    // 2、数据处理：具体包括创建数据源，数据结构化，数据分析
    /*
      模拟用户访问日志，日志用逗号隔开,第一列是姓名，第二列是id
     */
    val userAccessLog = Array(
      "zhangsan1,23435",
      "zhangsan2,11123422",
      "zhangsan3,2341234",
      "zhangsan4,1805208126370281",
      "zhangsan,1820150000003959",
      "lisi,1817430219307158"
    )
    val userAccessLogRDD = sc.parallelize(userAccessLog, 5)

    // 2.1、将普通的RDD，转换为元素为Row的RDD
    val rowRDD = userAccessLogRDD.map(logMsg => logMsg.split(",")) // 单词切割
      .map(log => Row(log(0), log(1).toLong)) // 数据格式化

    // 2.2 构造表scheme
    val scheme = StructType(
      Array(
        StructField("name",StringType,true), // true表示该字段可以为null
        StructField("uid",LongType,false)
      )
    )

    // 2.3 将scheme于rowRDD结合构成一张表。
    val userAccessLogRowDF = sqlCtx.createDataFrame(rowRDD,scheme)

    // 2.4 注册第一张用户日志表user_log
    userAccessLogRowDF.registerTempTable("user_log") // 第一张表

    // 注册第二张用户浏览网站表page，格式如下：
    /**
      * {"name":"zhangsan1","page":"百度"}
      * {"name":"lisi","page":"google"}
      * {"name":"wangwu","page":"搜狗"}
      */

    val pageAccessDF = sqlCtx.read.json("/Users/guohongdou/Projects/spark-learning/data/page_access.json")
//    pageAccessDF.show()
//    pageAccessDF.select("name","page").show()
    pageAccessDF.registerTempTable("page_access")
//    sqlCtx.sql("select * from page_access").show()

    // join，
    val results = sqlCtx.sql("select " +
      "a.name,a.uid,b.page from " +
      "user_log a " +
      "left join " +
      "page_access b " +
      "on a.name=b.name")
//    results.show()
    // 结果保存
//    results.persist(StorageLevel.DISK_ONLY)
//    results.write.saveAsTable("")

  }

  // 读取hive表
  def demo04():Unit={
    val conf = new SparkConf().setAppName("demo01").setMaster("local")
    conf.set("","")
    val sc = new SparkContext(conf)
    /**
      * 需要将hive-site.xml加入到类路径下,否则默认读取/user/hive/warehouse
      */
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hiveContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    hiveContext.sql("FROM src SELECT key, value").collect().foreach(println)

  }
  def main(args: Array[String]): Unit = {
    demo02()
  }
}
