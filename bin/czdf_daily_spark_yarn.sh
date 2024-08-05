#!/bin/bash

workpath=$1
cd $workpath
source $workpath/venv_pyetl/bin/activate

# 设置开始时间和结束时间
current_date=$(date +%Y-%m-%d)
previous_date=$(date -d "$current_date -1 day" +%Y-%m-%d)

# 将开始时间和结束时间转换为时间戳
start_time=$(date -d "$previous_date" +%s)
end_time=$(date -d "$previous_date" +%s)
echo $start_time
echo $end_time


# 执行python脚本
export PYSPARK_DRIVER_PYTHON=`which python`
export PYSPARK_PYTHON=./venv_pyetl/bin/python
while [ "$start_time" -le "$end_time" ]
do
  current_date=$(date -d @$start_time +"%Y-%m-%d")
  echo $current_date
  /usr/bin/spark-submit \
     --archives hdfs:///user/root/cloud/venv_pyetl.tar#venv_pyetl \
     --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv_pyetl/bin/python \
     --conf spark.executorEnv.PYSPARK_PYTHON=./venv_pyetl/bin/python \
     --master yarn \
     --packages ru.yandex.clickhouse:clickhouse-jdbc:0.3.2 \
     --deploy-mode client \
     --num-executors 16 \
     --executor-cores 2\
     --driver-memory 4G \
     --executor-memory 8G \
     --py-files hdfs:///user/qjy20472/cloud/pyetl/core.zip \
     --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
     core/etl/czdf_spark_gc_all_v2.py --env=prod --date=${current_date} --data_source=czdf --data_type=gc --mode=history \
     > $workpath/log/czdf_v2_${current_date}.log

  sleep 1

  # 增加一天到开始时间
  start_time=$(($start_time + 86400))
done
