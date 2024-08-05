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

export PYSPARK_DRIVER_PYTHON=`which python`
export PYSPARK_PYTHON=./venv_pyetl/bin/python
# 循环遍历日期范围内的每一天
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
     core/etl/sdec_spark_gc_all.py --env=prod --date=${current_date} --data_source=snat --data_type=gc --platname=大通 \
     > $workpath/log/all_spark_sdec_${current_date}.log

  sleep 1

  # 增加一天到开始时间
  start_time=$(($start_time + 86400))
done

#  > $workpath/log/e_spark_etl_${current_date}.log
