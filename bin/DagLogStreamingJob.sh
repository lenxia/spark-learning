#!/usr/bin/env bash
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#   Created 2017/3/12
#   Updated 2017/3/12
#   Author  guohongdou
#   Contact guohd@izhonghong.com,2219708253@qq.com
#   Version 0.0.0
#   Desc    <p>spark提交作业</p>
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@

usage(){
echo "Usage:"
}

export SPARK_HOME=""
# 第三方jar,比如kafka，hive，多个jar包用逗号隔开
extra_jars=""

${SPARK_HOME}/bin/spark-submit \
--class com.guohd.spark.pro.DapLogStreaming \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 6 \
--jars ${extra_jars}
