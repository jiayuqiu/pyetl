#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 6/9/23 3:22 PM
# @Author  : qjy20472
# @Site    : 
# @File    : sdec_spark_gc.py
# @Software: IntelliJ IDEA
# @Description:  用spark读sdec历史数据融合脚本

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

from core.conf import ck_properties_master, mysql_properties_dev, mysql_properties_prod
from core.conf import ck_engine_dict
from core.tools.dt import date_range, timestamp2unix, unix2datetime
from core.etl.unit import convert_unit_precision


sdec_ck_properties = {
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    "socket_timeout": "3000000",
    "rewriteBatchedStatements": "true",
    "batchsize": "2000",
    "numPartitions": "4",
    "user": "default",
    "password": "5NsWWQeJ",
    "host": "10.129.165.72",
    "database": 'sdecdmp',
    "port": 8123
}


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


def get_params(agrs: list) -> dict:
    """
    获取脚本需要的参数

    :param agrs: sys.args
    :return:
    """
    date = ''
    platname = None
    data_source = None
    delivery = None
    hos_series = None
    hos_mat_id = None
    app = None
    env = 'dev'
    config_path = None
    sheet_name = None
    mode = 'manual'
    data_type = 'gc'

    p = ['help', 'date=', 'platname=', 'env=', 'data_source=', 'data_type=', 
         'delivery=', 'hos_series=', 'app=', 'hos_mat_id=', 'config_path=', 'sheet_name=']
    opts, args = getopt.getopt(agrs[1:], '-h', p)
    print(opts, args)
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '--help'):
            print("[*] Help info")
            exit()

        if opt_name in ('--date'):
            date = opt_value
            print("[*] date is ", date)
        
        if opt_name in ('--platname'):
            platname = opt_value
            print("[*] platname is ", platname)

        if opt_name in ('--env'):
            env = opt_value
            print("[*] env is ", env)

        if opt_name in ('--data_source', ):
            data_source = opt_value
            print("[*] data_source is ", data_source)

        if opt_name in ('--delivery', ):
            delivery = opt_value
            print("[*] delivery is ", delivery)

        if opt_name in ('--hos_series', ):
            hos_series = opt_value
            print("[*] hos_series is ", hos_series)

        if opt_name in ('--hos_mat_id', ):
            hos_mat_id = opt_value
            print("[*] hos_mat_id is ", hos_mat_id)
            
        if opt_name in ('--data_type', ):
            data_type = opt_value
            print("[*] data_type is ", data_type)

        if opt_name in ('--app', ):
            app = opt_value
            print("[*] app is ", app)

        if opt_name in ('--config_path', ):
            """
            若app = import_config, 才会生效
            """
            config_path = opt_value
            print("[*] config_path is ", config_path)

        if opt_name in ('--sheet_name', ):
            """
            若app = import_config, 才会生效
            """
            sheet_name = opt_value
            print("[*] sheet_name is ", sheet_name)
    
    conf = {
        'date': date,
        'platname': platname,
        'data_source': data_source,
        'data_type': data_type,
        'delivery': delivery,
        'hos_series': hos_series,
        'hos_mat_id': hos_mat_id,
        'app': app,
        'env': env,
        'mode': mode,
        'config_path': config_path,
        'sheet_name': sheet_name
    }
    return conf


def snat_online_loader(conf: dict) -> list:
    """
    snat数据源，找出当日有数据的车辆

    Args:
        conf: 任务配置

    Returns:
        get online eins in The Date
    """
    sql = f"""
    select B.ein1 as ein,
           B.uploadDate,
           B.cnt,
           C.hos_id,
           C.hos_series
    from (select *
          from (select if(startsWith(sma.ein, '0'), substring(sma.ein, 2, LENGTH(sma.ein)), sma.ein) as ein1
                     , any(uploadDate)                                                               as uploadDate
                     , count(1)                                                                      as cnt
                from sdecdmp.SDECData2M_all sma
                where sma.uploadDate = '{conf['date']}'
                group by ein1) A
          where A.cnt > 0
            and A.ein1 <> '') B
             left join (
        select hos_id, hos_series
        from ck_mysql.device_all
        where hos_series = '{conf['hos_series'] + '系列'}'
        ) C on B.ein1 = C.hos_id
    where C.hos_series is not null
    """
    df = pd.read_sql(
        sql=sql,
        con=ck_engine_dict['master']
    )
    eins = list(df['ein'])
    return eins


def snat_etl_map_loader(conf: dict) -> str:
    """
    生成读取etl_map_all的sql
    :param conf: 任务参数
    :return:
    """
    sql = f"""
           SELECT 
               target_name, target_unit, signal_code, signal_unit, data_source, hos_series, data_type, hos_mat_id,
               extend_feature, platname
           FROM 
               dmp_mysql.etl_map_all
           WHERE
               data_source = 'snat' 
               and hos_series = '{conf['hos_series']}' 
               and data_type = '{conf['data_type']}'
               and signal_code is not null
           """
    print(sql)
    return sql


def snat_data_loader(conf: dict, eins: list) -> str:
    """
    读取ck原始数据

    :param conf: 任务配置
    :param eins: 当日在线的车辆列表
    :return: 原始数据获取sql
    """
    # eins = snat_online_loader(date, series)
    eins_str = "'" + "', '".join(eins) + "'"

    snat_sql = f"""
    select if(startsWith(ein, '0'), substring(ein, 2, LENGTH(ein)), ein)    as ein1
        , deviceID                                                          as vin
        , formatDateTime(uploadTime1, '%Y-%m-%d %H:%M:%S')                  as uploadTime1_
        , arrayStringConcat(arrayMap(x -> toString(x), params.valueM), ',') as valueM
        , arrayStringConcat(params.codeM, ',')                              as codeM
        , longitude
        , latitude
        , uploadDate
    from sdecdmp.SDECData2M_all sma
    where
        sma.uploadDate = '{conf['date']}'
        and params.valueM is not null and params.codeM is not null
        and ein <> ''
        and ein1 in ({eins_str})
    """
    return snat_sql


def pid_fusion_row(etl_row, pid_val_dict):
    """单个pid融合

    Args:
        etl_row (pd.Series): 单个配置
        pid_val_dict (dict): pid数值字典
    """
    # 1. 按;分割pids
    signal_codes = etl_row['signal_code'].split(';')
    if etl_row['signal_unit'] is None:
        # 若无单位信息，则无需做单位转换
        units = [None] * len(signal_codes)
    else:
        units = etl_row['signal_unit'].split(';')
    
    # 2. 单位转换
    for sc, unit in zip(signal_codes, units):
        if unit is None:
            unit_perision = 0
        else:
            unit_perision = convert_unit_precision(unit, etl_row['target_unit'])

        # 找到pid_val_dict中对应sc的数值，进行单位转换；转换完成记录待return
        if sc in pid_val_dict:
            # 若有对应pid，则进行单位转换并记录
            if unit_perision == 0:
                # string value
                return str(pid_val_dict[sc])
            else:
                # decimal value
                try:
                    # 转换成浮点型
                    return float(pid_val_dict[sc]) * unit_perision
                except:
                    # 若转换失败则赋值空
                    return None
        else:
            # 不存在，则进入下一个sc进行判断
            pass
    return None


def pid_fusion_map(etl_map_df, pid_val_dict):
    """融合

    Args:
        etl_map_df (pd.DataFrame): etl 配置表
        pid (dict): pid数值字典
    """
    fusion_pid_val_dict = {}
    for _, etl_row in etl_map_df.iterrows():
        target_name = etl_row['target_name']
        if target_name in ['lng', 'lat']:
            # 经纬度等特定数据，无需转换，在fusion外进行输入
            pass
        else:
            # 非特定字段，需要转换
            fusion_val = pid_fusion_row(etl_row, pid_val_dict)
            fusion_pid_val_dict[target_name] = fusion_val
    return fusion_pid_val_dict


def sdec_fusion_gc(row, etl_map_df, device_all_dict):
    """对一行数据进行融合

    Args:
        row (spark.Row): spark dataframe的一行
        etl_map_df (pd.DataFrame): etl配置
        device_all_dict (dict): device_all数据，用于匹配hos_series与hos_mat_id

    Returns:
        dict: 融合后的一行数据
    """
    try:
        ein = row['ein']
        vin = row['vin']
        upload_time = row['uploadTime1_']
        value_m_list = row['valueM'].split(',')
        code_m_list = row['codeM'].split(',')
        longitude = row['longitude']
        latitude = row['latitude']
        upload_date = row['uploadDate']

        # 记录pid与数值信息
        pid_val_dict = {}
        for val, pid in zip(value_m_list, code_m_list):
            val = float(val)
            pid_val_dict[pid] = val

        # 添加固定字段
        fusion_pid_val_dict = pid_fusion_map(etl_map_df, pid_val_dict)
        fusion_pid_val_dict['ein'] = ein
        fusion_pid_val_dict['vin'] = vin
        fusion_pid_val_dict['clt_date'] = upload_date
        fusion_pid_val_dict['lng'] = float(longitude)
        fusion_pid_val_dict['lat'] = float(latitude)
        fusion_pid_val_dict['clt_timestamp'] = timestamp2unix(upload_time)
        fusion_pid_val_dict['clt_time'] = unix2datetime(timestamp2unix(upload_time))
        fusion_pid_val_dict['data_type'] = row['data_type']
        fusion_pid_val_dict['data_source'] = row['data_source']
        fusion_pid_val_dict['platname'] = row['platname']
        
        # 添加device_all中的series与mat_id信息
        if ein in device_all_dict:
            fusion_pid_val_dict['hos_series'] = device_all_dict[ein]['hos_series']
            fusion_pid_val_dict['hos_mat_id'] = device_all_dict[ein]['hos_mat_id']
        else:
            fusion_pid_val_dict['hos_series'] = None
            fusion_pid_val_dict['hos_mat_id'] = None
        
        # if random.uniform(0, 100) < 1:
        #     print(f"ein -> {ein}, time -> {fusion_pid_val_dict['clt_time']}")
        return fusion_pid_val_dict
    except:
        traceback.print_exc()
        return None


# def rdd_to_clickhouse(records, properties):
#     # 建立连接
#     conn = clickhouse_driver.connect(
#         host=properties['host'],
#         port=properties['port'],
#         user=properties['user'],
#         password=properties['password'],
#         database='sdecdmp'
#     )
#     # 创建一个游标
#     cur = conn.cursor()
    
#     for record in records:
#         cols = []
#         vals = []
#         for key in records.keys():
#             cols.append(key)
#             vals.append(record[key])
        
#         cols_str = ','.join(cols)
#         vals_str = 
#         cur.execute(
#             f""""""
#         )


sdec_fusion_gc_udf = udf(
    sdec_fusion_gc, D_ETL_SCHEMA
)


def spark_main(conf: dict):
    """
    spark任务主函数

    :param conf: 任务配置
    :return:
    """
    spark = SparkSession \
        .builder \
        .appName(f"etl_fusion_history_{conf['date']}_{conf['data_source']}_{conf['hos_series']}") \
        .getOrCreate()

    # 1. load etl_map_all fusion 配置信息
    etl_map_sql = snat_etl_map_loader(conf)
    etl_map_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{sdec_ck_properties['host']}:{sdec_ck_properties['port']}/sdecdmp") \
        .option("query", etl_map_sql) \
        .option("user", sdec_ck_properties["user"]) \
        .option("password", sdec_ck_properties["password"]) \
        .option("driver", sdec_ck_properties["driver"]) \
        .load().persist()  # 数据量较小，可直接放入内存持久化
    etl_map_df = etl_map_sdf.toPandas()

    online_eins = snat_online_loader(conf)
    print(f"oneline eins shape: {len(online_eins)}")
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    snat_df_sql = snat_data_loader(conf, online_eins[:])
    print(snat_df_sql)

    # 生成device_all数据字典
    device_all_df = pd.read_sql(
        sql=f"SELECT hos_id as ein, hos_mat_id, hos_series, type as tbox_type FROM ck_mysql.device_all",
        con=ck_engine_dict['master']
    )

    device_all_dict = {}
    for _, row in device_all_df.iterrows():
        device_all_dict[row['ein']] = {
            'hos_mat_id': row['hos_mat_id'],
            'hos_series': row['hos_series'],
            'tbox_type': row['tbox_type'],
        }

    # 2. 读取原始数据
    snat_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{sdec_ck_properties['host']}:{sdec_ck_properties['port']}/sdecdmp") \
        .option("query", snat_df_sql) \
        .option("user", sdec_ck_properties["user"]) \
        .option("password", sdec_ck_properties["password"]) \
        .option("driver", sdec_ck_properties["driver"]) \
        .load()
    
    # 字段名调整
    snat_sdf = snat_sdf.withColumnRenamed("ein1", "ein")
    snat_sdf = snat_sdf.withColumn('data_type', lit(conf['data_type']))
    snat_sdf = snat_sdf.withColumn('data_source', lit(conf['data_source']))
    snat_sdf = snat_sdf.withColumn('platname', lit(conf['platname']))
    
    # 3. 与device_all合并，添加hos_mat_id, hos_series
    # 数据量较大，join会导致任务失败，调整为将device_all作为dict df传入sdec_fusion_gc
    # snat_sdf = snat_sdf.join(device_all_sdf, on=['ein'], how='left')

    # 3. 对sdf每一行进行解析融合
    partition_num = 32  # 此处分区数量与executors的数量成正比，最好是1~2倍的关系
    data_rdd = snat_sdf.rdd.repartition(partition_num)
    rdd = data_rdd.map(lambda row: sdec_fusion_gc(row, etl_map_df, device_all_dict)).filter(lambda r: r != None)
    print(f">>>>>>>>>>>>>>> {time.time()} 开始转换为dataframe >>>>>>>>>>>>>>>>>>>> ")
    fusion_sdf = spark.createDataFrame(rdd, D_ETL_SCHEMA)
    # fusion_sdf.show()
    
    # TODO: 5. 输出到ck中
    url = f"jdbc:clickhouse://{sdec_ck_properties['host']}:{sdec_ck_properties['port']}/sdecdmp"
    properties = {
        "driver": "ru.yandex.clickhouse.ClickHouseDriver",
        "socket_timeout": "3000000",
        "rewriteBatchedStatements": "true",
        "batchsize": "10000",
        "numPartitions": f"{partition_num}",
        "user": "default",
        "password": "5NsWWQeJ",
        "ignore": "true"
    }
    fusion_sdf.write.jdbc(
        url=url,
        table='etl_data_all',
        mode='append',
        properties=properties
    )
    spark.stop()
    

def delete_ck_data(clt_date, data_source, hos_series):
    delete_sql = f"""
    ALTER TABLE 
        sdecdmp.etl_data DELETE 
    WHERE 
        clt_date = '{clt_date}' and data_source = '{data_source}'
        and (hos_series = '{hos_series}' or hos_series = '{hos_series}系列')
    """
    # delete master
    ck_engine_dict['master'].execute(
        delete_sql
    )
    print(delete_sql)
    print(f"delete ck {ck_engine_dict['master']} done.")
    
    # delete slaves
    for con in ck_engine_dict['slaves']:
        print(con)
        con.execute(
            delete_sql
        )
        print(f"delete ck {con} done.")


if __name__ == '__main__':
    """
    run cmd:
    
    $ spark-submit --master local[4] --jars  $(echo /home/qjy20472/pythonProjects/pyetl/core/etl/jars/*.jar | tr ' ' ',') \
      core/etl/sdec_gc_spark.py
    """
    t1 = time.time()
    # conf = {'start_time': '2023-06-01', 'end_time': '2023-05-02', 'hos_series': 'E', 'data_source': 'snat',
    #         'data_type': 'gc', 'platname': '大通'}
    # conf['date'] = '2023-06-01'
    
    # conf = {'start_time': '2023-04-01', 'end_time': '2023-04-01', 'hos_series': 'H', 'data_source': 'snat',
    #         'data_type': 'gc', 'platname': '大通'}
    # conf['date'] = '2023-04-01'
    
    conf = get_params(sys.argv[1:])
    print(conf)
    delete_ck_data(conf['date'], conf['data_source'], conf['hos_series'])
    time.sleep(30)  # 等待30秒，ck删除数据存在延迟
    spark_main(conf)
    t2 = time.time()
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!! use time: {t2 - t1} seconds. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
