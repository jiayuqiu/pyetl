#!/bin/bash

workpath=$1
cd $workpath
source $workpath/venv_pyetl/bin/activate

# 设置开始时间和结束时间
start_date=$2
end_date=$3
start_time=$(date -d "$start_date" +%s)
end_time=$(date -d "$end_date" +%s)

# 执行python脚本
while [ "$start_time" -le "$end_time" ]
do
    current_date=$(date -d @$start_time +"%Y-%m-%d")
    next_date=$(date -d "$current_date +1 day" +%Y-%m-%d)
    echo "$current_date, $next_date"
    python main.py \
        -s "${current_date} 00:00:00" -e "${next_date} 00:00:00" \
        --platname="大通" \
        -E prod \
        --data_source="czdf" \
        --app="etl" > $workpath/log/czdf_${current_date}.log
    
    # 增加一天到开始时间
    start_time=$(($start_time + 86400))
done
