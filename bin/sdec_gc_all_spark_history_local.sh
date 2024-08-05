#!/bin/bash

workpath=$1
cd $workpath
source $workpath/venv_pyetl/bin/activate

# 设置开始时间和结束时间
start_date=$2
end_date=$3

# 将开始时间和结束时间转换为时间戳
start_time=$(date -d "$start_date" +%s)
end_time=$(date -d "$end_date" +%s)
echo $start_time
echo $end_time

# 循环遍历日期范围内的每一天
while [ "$start_time" -le "$end_time" ]
do
  current_date=$(date -d @$start_time +"%Y-%m-%d")
  echo $current_date
  $workpath/venv_pyetl/bin/spark-submit \
     --master local[4] \
     --conf "spark.pyspark.driver.python=$workpath/venv_pyetl/bin/python" \
     --conf "spark.pyspark.python=$workpath/venv_pyetl/bin/python" \
     --packages ru.yandex.clickhouse:clickhouse-jdbc:0.3.2 \
     --driver-memory 8G \
     --executor-memory 16G \
     --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
     core/etl/sdec_spark_gc_all.py --env=prod --date=${current_date} --data_source=snat --data_type=gc --platname=大通
     # > $workpath/log/all_spark_sdec_${current_date}.log

  sleep 1

  # 增加一天到开始时间
  start_time=$(($start_time + 86400))
done

#  > $workpath/log/e_spark_etl_${current_date}.log
