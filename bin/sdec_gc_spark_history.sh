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
  /usr/bin/spark-submit \
     --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
     --conf spark.pyspark.driver.python=/data1/pythonProjects/pyetl/venv_pyetl/bin/python \
     --conf spark.pyspark.python=/data1/pythonProjects/pyetl/venv_pyetl/bin/python \
     --driver-memory 4G \
     --executor-memory 8G \
     core/etl/sdec_spark_gc.py --env=prod --date=${current_date} --data_source=snat --hos_series=E --data_type=gc --platname=大通 \
     > $workpath/log/e_spark_sdec_${current_date}.log

  sleep 1

  /usr/bin/spark-submit \
     --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
     --driver-memory 4G \
     --executor-memory 8G \
     core/etl/sdec_spark_gc.py --env=prod --date=${current_date} --data_source=snat --hos_series=H --data_type=gc --platname=大通 \
     > $workpath/log/h_spark_sdec_${current_date}.log

  sleep 1

  /usr/bin/spark-submit \
     --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
     --driver-memory 4G \
     --executor-memory 8G \
     core/etl/sdec_spark_gc.py --env=prod --date=${current_date} --data_source=snat --hos_series=R --data_type=gc --platname=大通 \
     > $workpath/log/r_spark_sdec_${current_date}.log

  sleep 1

  # 增加一天到开始时间
  start_time=$(($start_time + 86400))
done

#  > $workpath/log/e_spark_etl_${current_date}.log