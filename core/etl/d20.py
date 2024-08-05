#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: macs
@contact: sohu.321@qq.com
@version: 1.0.0
@license: Apache Licence
@file: d20.py
@time: 2023/12/1 下午9:50
@desc: d20代码，获取原始数据，用于减少clickhouse的存储问题
"""


import os
import sys
cur_file_path = os.path.abspath(__file__)
pydmp_path = os.path.dirname(os.path.dirname(os.path.dirname(cur_file_path)))
print(f"pyetl path = {pydmp_path}")
sys.path.append(pydmp_path)

from core.conf import ck_properties_master

from pyspark.sql import SparkSession


def spark_main(date: str):
    spark = SparkSession \
        .builder \
        .appName(f"d20_load_data_{date}") \
        .getOrCreate()

    d20_data_sql = f"""
        select if(startsWith(ein, '0'), substring(ein, 2, LENGTH(ein)), ein)    as ein1
            , deviceID                                                          as vin
            , formatDateTime(uploadTime1, '%Y-%m-%d %H:%M:%S')                  as uploadTime1_
            , toUnixTimestamp(uploadTime1)                                      as clt_timestamp
            , arrayStringConcat(arrayMap(x -> toString(x), params.valueM), ',') as valueM
            , arrayStringConcat(params.codeM, ',')                              as codeM
            , toString(uploadDate)
        from
            sdecdmp.D20Data2M_all
        where
            CreateDate = '{date}'
        -- limit 1000000
    """
    d20_data_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", d20_data_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load()

    (d20_data_sdf.write.
     mode("overwrite").
     option("sep", "@").
     csv(f"/data/pythonProjects/pyetl/data/d20_{date}", header=True))

    print(f"{date} done!")
    spark.stop()


if __name__ == '__main__':
    date = sys.argv[1:][0]
    spark_main(date)
