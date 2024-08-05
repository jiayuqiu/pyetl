# -*- encoding: utf-8 -*-
'''
@File    :   czdf_spark_gc_all_all.py
@Time    :   2023/11/23 12:52:53
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   czdf etl spark任务, 适配融合表etl_data
             czdf数据传输协议与sdec一致, 融合代码都引用自sdec_spark_gc_all.py
'''

# here put the import lib
import pandas as pd
import numpy as np

import re
import os
import time
import traceback
import logging
import warnings
import datetime
import copy
import pandas as pd
import numpy as np
import random
import getopt

import sys
cur_file_path = os.path.abspath(__file__)
pydmp_path = os.path.dirname(os.path.dirname(os.path.dirname(cur_file_path)))
print(f"pyetl path = {pydmp_path}")
sys.path.append(pydmp_path)

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit

from core.conf import ck_properties_master, mysql_properties_prod
from core.conf import ck_engine_dict
from core.tools.dt import date_range, timestamp2unix, unix2datetime
from core.etl.sdec_spark_gc_all import get_params, etl_map_to_dict, convert_to_float, pid_fusion_map_by_dict


D_ETL_SCHEMA = StructType([
    StructField("ein", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("platname", StringType(), True),
    StructField("hos_series", StringType(), True),
    StructField("hos_mat_id", StringType(), True),
    StructField("lng", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("clt_timestamp", IntegerType(), True),
    StructField("clt_time", TimestampType(), True),
    StructField("clt_date", DateType(), True),
    StructField("atmospheric_pressure", FloatType(), True),  # 6
    StructField("atmospheric_temperature", FloatType(), True),  # 7
    StructField("speed", FloatType(), True),  # 8
    StructField("accpp", FloatType(), True),  # 9
    StructField("fuel_level", FloatType(), True),  # 10
    StructField("catalyst_level", FloatType(), True),  # 11
    StructField("engine_speed", FloatType(), True),  # 12
    StructField("fuel_rate", FloatType(), True),  # 13
    StructField("coolant_temperature", FloatType(), True),  # 14
    StructField("actual_engine_tq_per", FloatType(), True),  # 15
    StructField("friction_tq_per", FloatType(), True),  # 16
    StructField("reference_engine_tq", FloatType(), True),  # 17
    StructField("engine_state", StringType(), True),  # 18
    StructField("pto", StringType(), True),  # 19
    StructField("engine_intake_air_flow", FloatType(), True),  # 20
    StructField("engine_exhaust_air_flow", FloatType(), True),  # 21
    StructField("intake_manifold_pressure", FloatType(), True),  # 22
    StructField("down_stream_air_cooler_temperature", FloatType(), True),  # 23
    StructField("egr_cooler_temperature", FloatType(), True),  # 24
    StructField("egr_vlv_actuator_position", FloatType(), True),  # 25
    StructField("throttle_position", FloatType(), True),  # 26
    StructField("t4_temperature", FloatType(), True),  # 27
    StructField("t5_temperature", FloatType(), True),  # 28
    StructField("t6_temperature", FloatType(), True),  # 29
    StructField("scr_front_nox", FloatType(), True),  # 30
    StructField("catalyst_flow_rate", FloatType(), True),  # 31
    StructField("catalyst_concentration", FloatType(), True),  # 32
    StructField("dpf_pressure_diff", FloatType(), True),  # 33
    StructField("dpf_state", StringType(), True),  # 34
    StructField("dpf_soot_mass", FloatType(), True),  # 35
    StructField("battery_voltage", FloatType(), True),  # 36
    StructField("total_run_time", FloatType(), True),  # 37
    StructField("total_distance", FloatType(), True),  # 38
    StructField("total_fuel", FloatType(), True),  # 39
    StructField("PFltRgn_lSnceRgn", FloatType(), True),  # 40
    StructField("InjCrv_qPoI3Set", FloatType(), True),  # 41
    StructField("InjCtl_qCurr", FloatType(), True),  # 42
    StructField("Driving_regeneration", StringType(), True),  # 43
    StructField("rgn_interval", FloatType(), True),  # 44
    StructField("Parking_regeneration", StringType(), True),  # 45
    StructField("PFltRgn_stDem", StringType(), True),  # 46
])


def czdf_etl_map_loader(conf: dict) -> str:
    """
    生成读取etl_map_all的sql
    :param conf: 任务参数
    :return:
    """
    sql = f"""
           SELECT 
               target_name, 
               target_unit, 
               signal_code as signal_name, 
               signal_unit, 
               data_source, 
               data_type as signal_type, 
               extend_feature
           FROM 
               dmp_mysql.etl_map_all
           WHERE
               data_source = 'czdf' 
               and signal_name is not null
           """
    print(sql)
    return sql


def czdf_data_loader(conf: dict) -> str:
    """
    读取ck原始数据

    :param conf: 任务配置
    :param eins: 当日在线的车辆列表
    :return: 原始数据获取sql
    """
    czdf_sql = f"""
    select 
        if(startsWith(ein, '0'), substring(ein, 2, LENGTH(ein)), ein)       as ein1
        , vin
        , formatDateTime(clt_time, '%Y-%m-%d %H:%M:%S')                     as uploadTime1_
        , toString(parse_value)                                             as valueM
        , toString(code)                                                    as codeM
        , lng                                                               as longitude
        , lat                                                               as latitude
        , clt_date                                                          as uploadDate
    from sdecdmp.signal_data_nosql_all sma
    where
        sma.clt_date = '{conf['date']}'
        and data_source = 'czdf'
        and valueM is not null and codeM is not null
        and ein1 <> ''
    """
    return czdf_sql


def czdf_fusion_gc_by_dict(row, etl_map_dict: dict, pid_map_dict: dict, device_all_dict: dict):
    """对一行数据进行融合

    Args:
        row (spark.Row): spark dataframe的一行
        etl_map_dict (dict): etl配置
        pid_map_dict (dict): pid etl配置
        device_all_dict (dict): device_all数据，用于匹配hos_series与hos_mat_id

    Returns:
        dict: 融合后的一行数据
    """
    try:
        ein = row['ein']
        vin = row['vin']
        upload_time = row['uploadTime1_']
        longitude = row['longitude']
        latitude = row['latitude']
        upload_date = row['uploadDate']
        

        value_m_list = [convert_to_float(val.strip('val')) for val in re.findall(r"'([^']+)'", row['valueM'])]
        code_m_list = re.findall(r"'([^']+)'", row['codeM'])

        # 记录pid与数值信息
        pid_val_dict = {}
        for val, pid in zip(value_m_list, code_m_list):
            # print(f"pid floating... target unit = {pid_map_dict[pid]['target_unit']}")
            if pid in pid_map_dict.keys():
                if not pid_map_dict[pid]['target_unit'] is None:
                    val = convert_to_float(val)
                pid_val_dict[pid] = val
        # print(pid_val_dict)

        fusion_pid_val_dict = pid_fusion_map_by_dict(etl_map_dict, pid_val_dict)
        
        # 添加固定字段
        fusion_pid_val_dict['ein'] = ein
        fusion_pid_val_dict['vin'] = vin
        fusion_pid_val_dict['clt_date'] = datetime.datetime.strptime(conf['date'], '%Y-%m-%d')
        fusion_pid_val_dict['lng'] = float(longitude)
        fusion_pid_val_dict['lat'] = float(latitude)
        fusion_pid_val_dict['clt_timestamp'] = timestamp2unix(upload_time)
        # fusion_pid_val_dict['clt_time'] = unix2datetime(timestamp2unix(upload_time)).strftime("%Y-%m-%d %H:%M:%S")
        fusion_pid_val_dict['clt_time'] = datetime.datetime.strptime(upload_time, '%Y-%m-%d %H:%M:%S')
        fusion_pid_val_dict['data_type'] = row['data_type']
        fusion_pid_val_dict['data_source'] = row['data_source']
        fusion_pid_val_dict['platname'] = conf['platname']

        # 添加device_all中的series与mat_id信息
        if ein in device_all_dict:
            fusion_pid_val_dict['hos_series'] = device_all_dict[ein]['hos_series']
            fusion_pid_val_dict['hos_mat_id'] = device_all_dict[ein]['hos_mat_id']
        else:
            fusion_pid_val_dict['hos_series'] = None
            fusion_pid_val_dict['hos_mat_id'] = None

        # if np.random.uniform(0, 100) < 2:
        #     print(f"ein -> {ein}, time -> {fusion_pid_val_dict['clt_time']}")
        return fusion_pid_val_dict
    except:
        traceback.print_exc()
        return None


def spark_main(conf: dict):
    """
    spark任务主函数，不区分车型系列，对SDECData2M_all全量融合

    :param conf: 任务配置
    :return:
    """
    spark = SparkSession \
        .builder \
        .appName(f"etl_fusion_history_{conf['date']}_{conf['data_source']}_all_series") \
        .getOrCreate()

    # 1. load etl_map_all fusion 配置信息
    etl_map_sql = czdf_etl_map_loader(conf)
    etl_map_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", etl_map_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load()  # 数据量较小，可直接放入内存持久化
    etl_map_df = etl_map_sdf.toPandas()
    etl_map_dict, pid_map_dict = etl_map_to_dict(etl_map_df)

    # 获取全量数据
    czdf_df_sql = czdf_data_loader(conf)
    print(czdf_df_sql)

    # 生成device_all数据字典
    device_all_df = pd.read_sql(
        sql=f"SELECT hos_id as ein, hos_mat_id, hos_series, type as tbox_type FROM ck_mysql.device_all",
        con=ck_engine_dict['slaves'][2]
    )

    device_all_dict = {}
    for _, row in device_all_df.iterrows():
        device_all_dict[row['ein']] = {
            'hos_mat_id': row['hos_mat_id'],
            'hos_series': row['hos_series'],
            'tbox_type': row['tbox_type'],
        }

    # 2. 读取原始数据
    czdf_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", czdf_df_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load()
    
    # 字段名调整
    czdf_sdf = czdf_sdf.withColumnRenamed("ein1", "ein")
    czdf_sdf = czdf_sdf.withColumn('data_type', lit(conf['data_type']))
    czdf_sdf = czdf_sdf.withColumn('data_source', lit(conf['data_source']))

    # 3. 对sdf每一行进行解析融合
    partition_num = 8  # 此处分区数量与executors的数量成正比，最好是1~2倍的关系
    data_rdd = czdf_sdf.rdd.repartition(partition_num)
    rdd = data_rdd.map(lambda row: czdf_fusion_gc_by_dict(row, etl_map_dict, pid_map_dict, device_all_dict)).filter(lambda r: r != None)
    print(f">>>>>>>>>>>>>>> {time.time()} 开始转换为dataframe >>>>>>>>>>>>>>>>>>>> ")
    fusion_sdf = spark.createDataFrame(rdd, D_ETL_SCHEMA)
    
    # 5. 输出到ck中
    url = f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp"
    properties = {
        "driver": ck_properties_master['driver'],
        "socket_timeout": "3000000",
        "rewriteBatchedStatements": "true",
        "batchsize": "10000",
        "numPartitions": f"{partition_num}",
        "user": ck_properties_master['user'],
        "password": ck_properties_master['password'],
        "ignore": "true"
    }
    fusion_sdf.write.jdbc(
        url=url,
        table='etl_data_all',
        mode='append',
        properties=properties
    )
    # fusion_sdf.show()
    spark.stop()
    

def drop_ck_data(clt_date,):
    drop_sql = f"""
    alter table etl_data drop partition ('czdf', 'gc', '{clt_date}')
    """
    print(drop_sql)
    
    # delete slaves
    for con in ck_engine_dict['slaves']:
        print(con)
        con.execute(
            drop_sql
        )
        print(f"delete ck {con} done.")


if __name__ == '__main__':
    t1 = time.time()
    print(sys.argv[1:])
    conf = get_params(sys.argv[1:])
    print(conf)
    # 删除已有数据的分区
    drop_ck_data(conf['date'])
    time.sleep(5)  # 等待30秒，ck删除数据存在延迟
    spark_main(conf)
    t2 = time.time()
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!! use time: {t2 - t1} seconds. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
