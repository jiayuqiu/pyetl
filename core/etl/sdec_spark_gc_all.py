#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 6/9/23 3:22 PM
# @Author  : qjy20472
# @Site    : 
# @File    : sdec_spark_gc.py
# @Software: IntelliJ IDEA
# @Description:  用spark读sdec历史数据融合脚本
# @Version : 1.1，与EOL系统同步，增加目前应用已经涉及的测点

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

from core.conf import ck_properties_master, ck_properties_slave_4, mysql_properties_dev, mysql_properties_prod
from core.conf import ck_engine_dict, mysql_engine_prod
from core.tools.dt import date_range, timestamp2unix, unix2datetime
from core.etl.unit import convert_unit_precision


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
    StructField("clt_time", StringType(), True),
    StructField("clt_date", StringType(), True),
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
    hos_series = 'all'
    hos_mat_id = None
    app = None
    env = 'dev'
    config_path = None
    sheet_name = None
    mode = 'manual'
    data_type = 'gc'

    p = ['help', 'date=', 'platname=', 'env=', 'data_source=', 'data_type=', 
         'delivery=', 'app=', 'hos_mat_id=', 'config_path=', 'sheet_name=']
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


def snat_etl_map_loader(conf: dict) -> pd.DataFrame:
    """
    生成读取etl_map_all的sql
    :param conf: 任务参数
    :return:
    """
    sql_v2 = f"""
        SELECT 
           target_name, target_unit, signal_name, signal_unit, signal_type, data_source
        FROM 
           dmp.etl_map_v2
        WHERE
           data_source = 'sdec' 
           and signal_name is not null
        """

    sql_v1 = f"""
           SELECT 
               target_name
           FROM 
               dmp.etl_map_all
           WHERE
               data_source = 'snat' 
               and signal_code is not null
           """
    map_v1_df = pd.read_sql(
        sql=sql_v1, con=mysql_engine_prod
    )
    map_v2_df = pd.read_sql(
        sql=sql_v2, con=mysql_engine_prod
    )
    print(map_v2_df.shape)
    map_v2_df = map_v2_df.loc[map_v2_df['target_name'].isin(map_v1_df['target_name'])]
    print(map_v2_df.shape)
    print(map_v2_df)
    return map_v2_df


def snat_data_loader(conf: dict) -> str:
    """
    读取ck原始数据

    :param conf: 任务配置
    :param eins: 当日在线的车辆列表
    :return: 原始数据获取sql
    """
    # eins = snat_online_loader(date, series)
    # eins_str = "'" + "', '".join(eins) + "'"

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
    limit 100000
    """
    return snat_sql


def etl_map_to_dict(df: pd.DataFrame) -> tuple:
    """etl配置表 datafraome 转 dict, 用于增加融合效率

    Args:
        df (pd.DataFrame): etl配置表 datafraome

    Returns:
        dict: 配置字典，格式:
              {'<target_name>': {'<pids>': {'<pid1>': ... }, 'target_unit': 'kg'}}
    """
    etl_map_dict = {}
    pid_map_dict = {}
    for target_name, group in df.groupby('target_name'):
        etl_map_dict[target_name] = {'pids': {}, 'target_unit': group['target_unit'].values[0]}
        for _, row in group.iterrows():
            if target_name in ['lng', 'lat']:
                # 经纬度等特定数据，无需转换，在fusion外进行输入
                pass
            else:
                # 非特定字段，需要转换
                etl_map_dict[target_name]['pids'][row['signal_name']] = {
                    'signal_type': row['signal_type'],
                    'signal_unit': row['signal_unit']
                }
                pid_map_dict[row['signal_name']] = {
                    'signal_type': row['signal_type'],
                    'signal_unit': row['signal_unit'],
                    'target_unit': group['target_unit'].values[0]
                }
    return etl_map_dict, pid_map_dict


def convert_to_float(value, default_val=-999.):
    """
    The string value is converted to float value.
    If the conversion succeeds, float is returned.
    If the conversion fails, it indicates that the data is non-numeric.
    If it is empty, None is returned, else return initial value.

    :param value:
    :return:
    """
    try:
        result = float(value)
        return result
    except ValueError:
        return default_val


def col_fusion_dict(etl_col_dict, pid_val_dict):
    """单列融合
    
    做出一个case when的功能，同一个target_name的字段可能存在多个pid存在
    此时，取第一次遇到非None数值的pid将数值进行填充

    Args:
        etl_col_dict (pd.DataFrame): 单个配置
        pid_val_dict (dict): pid数值字典
    """
    # 单位转换
    # for sc, unit in zip(signal_codes, units):
    for pid in etl_col_dict['pids'].keys():
        sc = pid
        unit = etl_col_dict['pids'][pid]['signal_unit']
        
        # 找到pid_val_dict中对应sc的数值，进行单位转换；转换完成记录待return
        if sc in pid_val_dict:
            # 若有对应pid，则进行单位转换并记录
            if unit is None:
                unit_perision = 0
            else:
                unit_perision = convert_unit_precision(unit, etl_col_dict['target_unit'])

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
    

def pid_fusion_map_by_dict(etl_map_dict, pid_val_dict):
    """融合

    Args:
        etl_map_dict (dict): etl 配置表
        pid_val_dict (dict): pid数值字典
    """
    fusion_pid_val_dict = {}
    for target_name in etl_map_dict.keys():
        if target_name in ['lng', 'lat']:
            # 经纬度等特定数据，无需转换，在fusion外进行输入
            pass
        else:
            # 非特定字段，需要转换
            # if target_name in ['t4_temperature', 't5_temperature']:
            #     pass
            # else:
            #     continue
            # print(target_name)
            fusion_val = col_fusion_dict(etl_map_dict[target_name], pid_val_dict)
            # print(f"{target_name}: {fusion_val}")

            fusion_pid_val_dict[target_name] = fusion_val
    return fusion_pid_val_dict


def sdec_fusion_gc_by_dict(row, etl_map_dict: dict, pid_map_dict: dict, device_all_dict: dict, mode: str):
    """对一行数据进行融合

    Args:
        row (spark.Row): spark dataframe的一行
        etl_map_dict (dict): etl配置
        pid_map_dict (dict): pid etl配置
        device_all_dict (dict): device_all数据，用于匹配hos_series与hos_mat_id
        mode (str): etl模式, stream模式 - codeM与valueM已经是list；history模式 - 从clickhouse拿到的历史数据
                                                                 是以,为连接的字符串，需要分割成List才能处理

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
        
        if mode == 'history':
            value_m_list = row['valueM'].split(',')
            code_m_list = row['codeM'].split(',')
        elif mode == 'stream':
            value_m_list = row['valueM']
            code_m_list = row['codeM']
        else:
            print(f"mode = {mode}，etl结果直接返回None!!!!!!!!")
            return None
        # print(value_m_list)
        # print(code_m_list)

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
        # print(fusion_pid_val_dict)
        
        # 添加固定字段
        fusion_pid_val_dict['ein'] = ein
        fusion_pid_val_dict['vin'] = vin
        # fusion_pid_val_dict['clt_date'] = upload_date
        fusion_pid_val_dict['clt_date'] = upload_date.strftime("%Y-%m-%d")
        fusion_pid_val_dict['lng'] = float(longitude)
        fusion_pid_val_dict['lat'] = float(latitude)
        fusion_pid_val_dict['clt_timestamp'] = timestamp2unix(upload_time)
        fusion_pid_val_dict['clt_time'] = unix2datetime(timestamp2unix(upload_time)).strftime("%Y-%m-%d %H:%M:%S")
        # fusion_pid_val_dict['clt_time'] = datetime.datetime.strptime(upload_time, '%Y-%m-%d %H:%M:%S')
        fusion_pid_val_dict['data_type'] = row['data_type']
        fusion_pid_val_dict['data_source'] = row['data_source']
        fusion_pid_val_dict['platname'] = '大通'
        
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
    etl_map_df = snat_etl_map_loader(conf)
    # etl_map_sdf = spark.read.format('jdbc') \
    #     .option("url", f"jdbc:mysql://{mysql_properties_prod['host']}:{mysql_properties_prod['port']}/dmp") \
    #     .option("query", etl_map_sql) \
    #     .option("user", mysql_properties_prod["user"]) \
    #     .option("password", mysql_properties_prod["password"]) \
    #     .option("driver", mysql_properties_prod["driver"]) \
    #     .load().persist()  # 数据量较小，可直接放入内存持久化
    # etl_map_df = etl_map_sdf.toPandas()
    etl_map_dict, pid_map_dict = etl_map_to_dict(etl_map_df)

    # 获取全量数据
    snat_df_sql = snat_data_loader(conf)
    print(snat_df_sql)

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
    snat_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", snat_df_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load()

    # snat_sdf = snat_sdf.filter(snat_sdf.vin == 'VSNAT0H9231001216')
    
    # 字段名调整
    snat_sdf = snat_sdf.withColumnRenamed("ein1", "ein")
    snat_sdf = snat_sdf.withColumn('data_type', lit(conf['data_type']))
    snat_sdf = snat_sdf.withColumn('data_source', lit(conf['data_source']))
    snat_sdf = snat_sdf.withColumn('platname', lit(conf['platname']))

    # 3. 对sdf每一行进行解析融合
    partition_num = 32  # 此处分区数量与executors的数量成正比，最好是1~2倍的关系
    # data_rdd = snat_sdf.rdd
    data_rdd = snat_sdf.rdd
    rdd = data_rdd.map(lambda row: sdec_fusion_gc_by_dict(row, etl_map_dict, pid_map_dict, device_all_dict, 'history')).filter(lambda r: r != None)
    print(f">>>>>>>>>>>>>>> {time.time()} 开始转换为dataframe >>>>>>>>>>>>>>>>>>>> ")
    fusion_sdf = spark.createDataFrame(rdd, D_ETL_SCHEMA)
    
    # 5. 输出到ck中
    url = f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp"
    properties = {
        "driver": ck_properties_master['driver'],
        "socket_timeout": "3000000",
        "rewriteBatchedStatements": "true",
        "batchsize": "50000",
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
    spark.stop()
    

def drop_ck_data(clt_date,):
    drop_sql = f"""
    alter table etl_data drop partition ('snat', 'gc', '{clt_date}')
    """
    # delete master
    # ck_engine_dict['master'].execute(
    #     drop_sql
    # )
    print(drop_sql)
    # print(f"delete ck {ck_engine_dict['master']} done.")
    
    # delete slaves
    for con in ck_engine_dict['slaves']:
        print(con)
        con.execute(
            drop_sql
        )
        print(f"delete ck {con} done.")


if __name__ == '__main__':
    t1 = time.time()
    
    conf = get_params(sys.argv[1:])
    print(conf)
    # 删除已有数据的分区
    drop_ck_data(conf['date'])
    time.sleep(5)  # 等待30秒，ck删除数据存在延迟
    spark_main(conf)
    t2 = time.time()
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!! use time: {t2 - t1} seconds. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
