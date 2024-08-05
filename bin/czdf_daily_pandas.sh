#!/bin/bash

workpath=$1
cd $workpath
source $workpath/venv_pyetl/bin/activate

# 设置开始时间和结束时间
current_date=$(date +%Y-%m-%d)
previous_date=$(date -d "$current_date -1 day" +%Y-%m-%d)

# 执行python脚本
python main.py \
    -s "${previous_date} 00:00:00" -e "${current_date} 00:00:00" \
    --platname="大通" \
    -E prod \
    --data_source="czdf" \
    --app="etl" > $workpath/log/czdf_${previous_date}.log