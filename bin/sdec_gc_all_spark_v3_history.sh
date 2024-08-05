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
     --master local[8] \
     --conf "spark.pyspark.driver.python=$workpath/venv_pyetl/bin/python" \
     --conf "spark.pyspark.python=$workpath/venv_pyetl/bin/python" \
     --driver-memory 8G \
     --executor-memory 16G \
     --jars $(echo $workpath/core/etl/jars/*.jar | tr ' ' ',') \
     core/etl/sdec_spark_gc_v3.py --env=prod --date=${current_date} --data_source=sdec --data_type=gc --mode=history \
     # > $workpath/log/all_spark_sdec_v3_${current_date}.log

  sleep 1

  # 增加一天到开始时间
  start_time=$(($start_time + 86400))
done

#  > $workpath/log/e_spark_etl_${current_date}.log
