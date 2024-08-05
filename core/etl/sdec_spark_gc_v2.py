# -*- encoding: utf-8 -*-
'''
@File    :   sdec_spark_gc_v2.py
@Time    :   2023/08/24 10:19:03
@Author  :   qiujiayu 
@Version :   2.0
@Contact :   qjy20472@snat.com
@Desc    :   sdec tbox数据融合第二版，适配EOL系统的表格式、增加340个左右测点。
             对改版数据格式进行代码调整
'''

# here put the import lib

# import third-party modules
import pandas as pd
import numpy as np

# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit
from pyspark.storagelevel import StorageLevel
# from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# import python lib
import os
import sys
import getopt
import time
import re
import traceback
from decimal import Decimal

# Import the ETL custom module
cur_file_path = os.path.abspath(__file__)
pydmp_path = os.path.dirname(os.path.dirname(os.path.dirname(cur_file_path)))
print(f"pyetl path = {pydmp_path}")
sys.path.append(pydmp_path)
from core.etl.utils.SchemaV2 import ETL_SCHEMA, ETL_SCHEMA_TEST
from core.conf import ck_engine_dict, ck_properties_master
from core.conf import ck_engine_slave_4 as ck_engine_slave
from core.tools.dt import timestamp2unix, unix2datetime
from core.etl.unit import convert_unit_precision


def get_params(agrs: list) -> dict:
    """
    获取脚本需要的参数

    :param agrs: sys.args
    :return:
    """
    date = ''
    data_source = None
    hos_series = 'all'
    env = 'dev'
    data_type = 'gc'
    mode = ''

    print(agrs)
    p = ['help', 'date=', 'env=', 'data_source=', 'data_type=', 'mode=']
    opts, args = getopt.getopt(agrs[1:], '-h', p)
    print(opts, args)
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '--help'):
            print("[*] Help info")
            exit()

        if opt_name in ('--date'):
            date = opt_value
            print("[*] date is ", date)

        if opt_name in ('--env'):
            env = opt_value
            print("[*] env is ", env)

        if opt_name in ('--data_source',):
            data_source = opt_value
            print("[*] data_source is ", data_source)

        if opt_name in ('--data_type',):
            data_type = opt_value
            print("[*] data_type is ", data_type)

        if opt_name in ('--mode',):
            mode = opt_value
            print("[*] mode is ", mode)

    if date == '':
        raise '请输入日期'
    
    conf = {
        'date': date,
        'data_source': data_source,
        'data_type': data_type,
        'hos_series': hos_series,
        'env': env,
        'mode': mode,
    }
    return conf


def device_all_loader() -> dict:
    """
    load table "device_all", converting dataframe to dict can decrease fusion time.

    :param df:
    :return:
    """
    # 生成device_all数据字典，ein存在重复现象，取create_time最新的那个为准
    device_all_df = pd.read_sql(
        sql=f"""
        SELECT t.ein, any(t.hos_mat_id) as hos_mat_id , any(t.hos_series) as hos_series, any(t.tbox_type) as tbox_type
        FROM (
            SELECT hos_id AS ein, hos_mat_id, hos_series, type AS tbox_type
            FROM ck_mysql.device_all
            ORDER BY hos_id, create_time DESC
        ) AS t
        GROUP BY t.ein
        """,
        con=ck_engine_slave
    )

    device_all_dict = {}
    for _, row in device_all_df.iterrows():
        device_all_dict[row['ein']] = {
            'hos_mat_id': row['hos_mat_id'],
            'hos_series': row['hos_series'],
            'tbox_type': row['tbox_type'],
        }
    return device_all_dict


def snat_etl_map_loader(conf: dict) -> str:
    """
    生成读取etl_map_v2的sql
    :param conf: 任务参数
    :return:
    """
    sql = f"""
           SELECT
               target_name, target_unit, signal_name, signal_unit, signal_type, data_source
           FROM
               dmp_mysql.etl_map_v2
           WHERE
               data_source = 'sdec' and signal_name is not null
           """
    print(sql)
    return sql


def etl_map_to_dict(df: pd.DataFrame) -> dict:
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


def snat_data_loader(conf: dict, ) -> str:
    """
    读取ck原始数据

    :param conf: 任务配置
    :return: 原始数据获取sql
    """
    snat_sql = f"""
    select if(startsWith(ein, '0'), substring(ein, 2, LENGTH(ein)), ein)    as ein1
        , deviceID                                                          as vin
        , formatDateTime(uploadTime1, '%Y-%m-%d %H:%M:%S')                  as uploadTime1_
        , toUnixTimestamp(uploadTime1)                                      as clt_timestamp
        , arrayStringConcat(arrayMap(x -> toString(x), params.valueM), ',') as valueM
        , arrayStringConcat(params.codeM, ',')                              as codeM
        , longitude
        , latitude
        , toString(uploadDate)                                as uploadDate
    from sdecdmp.SDECData2M_all sma
    where
        sma.uploadDate = '{conf['date']}'
        and params.valueM is not null and params.codeM is not null
        and ein <> ''
    -- limit 1000000
    """
    return snat_sql


def drop_ck_data(clt_date, ):
    drop_sql = f"""
    alter table etl_data_v2 drop partition ('sdec', 'gc', '{clt_date}')
    """
    # delete master
    # print(drop_sql)
    # print(ck_engine_dict['master'])
    # ck_engine_dict['master'].execute(
    #     drop_sql
    # )
    # print(drop_sql)
    # print(f"delete ck {ck_engine_dict['master']} done.")

    # delete slaves
    for con in ck_engine_dict['slaves']:
        print(con)
        con.execute(
            drop_sql
        )
        print(f"delete ck {con} done.")


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


def convert_to_decimal(value):
    """
    The string value is converted to decimal value.
    If the conversion succeeds, decimal is returned.
    If the conversion fails, it indicates that the data is non-numeric.
    If it is empty, None is returned, else return initial value.

    :param value:
    :return:
    """
    try:
        result = Decimal(value)
        return result
    except ValueError:
        if value.strip() == '':
            return None
        else:
            return value


def pid_fusion_group(etl_group_df, pid_val_dict):
    """单个pid融合

    Args:
        etl_group_df (pd.DataFrame): 单个配置
        pid_val_dict (dict): pid数值字典
    """
    signal_codes = etl_group_df['signal_name'].tolist()
    units = etl_group_df['signal_unit'].tolist()

    # 单位转换
    for sc, unit in zip(signal_codes, units):
        # 找到pid_val_dict中对应sc的数值，进行单位转换；转换完成记录待return
        if sc in pid_val_dict:
            # 若有对应pid，则进行单位转换并记录
            if unit is None:
                unit_perision = 0
            else:
                unit_perision = convert_unit_precision(unit, etl_group_df['target_unit'].values[0])

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
                    return Decimal(float(pid_val_dict[sc]) * unit_perision)
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
    for target_name, group in etl_map_df.groupby('target_name'):
        if target_name in ['lng', 'lat']:
            # 经纬度等特定数据，无需转换，在fusion外进行输入
            pass
        else:
            # 非特定字段，需要转换
            fusion_val = pid_fusion_group(group, pid_val_dict)
            fusion_pid_val_dict[target_name] = fusion_val

    return fusion_pid_val_dict


def pid_fusion_map_by_dict(etl_map_dict, pid_val_dict):
    """融合

    Args:
        etl_map_dict (pd.DataFrame): etl 配置表
        pid_val_dict (dict): pid数值字典
    """
    fusion_pid_val_dict = {}
    for target_name in etl_map_dict.keys():
        if target_name in ['lng', 'lat']:
            # 经纬度等特定数据，无需转换，在fusion外进行输入
            pass
        else:
            # 非特定字段，需要转换
            fusion_val = col_fusion_dict(etl_map_dict[target_name], pid_val_dict)
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
            val = convert_to_decimal(val)
            pid_val_dict[pid] = val

        fusion_pid_val_dict = pid_fusion_map(etl_map_df, pid_val_dict)
        
        # 添加固定字段
        fusion_pid_val_dict['ein'] = ein
        fusion_pid_val_dict['vin'] = vin
        fusion_pid_val_dict['clt_date'] = upload_date
        fusion_pid_val_dict['lng'] = float(longitude)
        fusion_pid_val_dict['lat'] = float(latitude)
        fusion_pid_val_dict['clt_timestamp'] = timestamp2unix(upload_time)
        fusion_pid_val_dict['clt_time'] = unix2datetime(timestamp2unix(upload_time)).strftime("%Y-%m-%d %H:%M:%S")
        fusion_pid_val_dict['data_type'] = row['data_type']
        fusion_pid_val_dict['data_source'] = row['data_source']

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
            # value_m_list = row['valueM'].split(',')
            # code_m_list = row['codeM'].split(',')
            value_m_list = [float(val.strip()) for val in row['valueM'].strip('[]').split(',')]
            code_m_list = [c.replace("'", "") for c in re.findall(r"[-+]?\d+\.\d+|\b\w+\b", row['codeM'])]
        elif mode == 'stream':
            value_m_list = row['valueM']
            code_m_list = row['codeM']
        else:
            print(f"mode = {mode}，etl结果直接返回None!!!!!!!!")
            return None
        # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # print(value_m_list, len(value_m_list))
        # print(code_m_list, len(code_m_list))
        
        # 记录pid与数值信息
        pid_val_dict = {}
        for val, pid in zip(value_m_list, code_m_list):
            if pid in pid_map_dict.keys():
                print(pid_map_dict[pid])
                if not pid_map_dict[pid]['target_unit'] is None:
                    val = convert_to_float(val)
                pid_val_dict[pid] = val
        # print(pid_val_dict)
        # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        fusion_pid_val_dict = pid_fusion_map_by_dict(etl_map_dict, pid_val_dict)
        # print(fusion_pid_val_dict)
        
        # 添加固定字段
        fusion_pid_val_dict['ein'] = ein
        fusion_pid_val_dict['vin'] = vin
        fusion_pid_val_dict['clt_date'] = upload_date
        fusion_pid_val_dict['lng'] = float(longitude)
        fusion_pid_val_dict['lat'] = float(latitude)
        fusion_pid_val_dict['clt_timestamp'] = timestamp2unix(upload_time)
        fusion_pid_val_dict['clt_time'] = unix2datetime(timestamp2unix(upload_time)).strftime("%Y-%m-%d %H:%M:%S")
        fusion_pid_val_dict['data_type'] = row['data_type']
        fusion_pid_val_dict['data_source'] = row['data_source']

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
    # jar_list = glob.glob('/home/qjy20472/pythonProjects/pyetl/core/etl/jars/*.jar')
    # jars_str = ','.join(jar_list)
    spark = SparkSession \
        .builder \
        .appName(f"etl_v2_{conf['date']}") \
        .getOrCreate()

    # 1. load etl_map_all fusion 配置信息
    etl_map_sql = snat_etl_map_loader(conf)
    etl_map_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", etl_map_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load().persist()  # 数据量较小，可直接放入内存持久化

    etl_map_df = etl_map_sdf.toPandas()
    print(etl_map_df)
    etl_map_dict, pid_map_dict = etl_map_to_dict(etl_map_df)
    # print(etl_map_dict)

    # 2. load device_all
    device_all_dict = device_all_loader()

    # 3. load sdec data
    snat_df_sql = snat_data_loader(conf)
    print(snat_df_sql)
    snat_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", snat_df_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .option("batchsize", "1000000") \
        .load()

    # 字段名调整
    snat_sdf = snat_sdf.withColumnRenamed("ein1", "ein")
    snat_sdf = snat_sdf.withColumn('data_type', lit(conf['data_type']))
    snat_sdf = snat_sdf.withColumn('data_source', lit(conf['data_source']))

    # 3. 对sdf每一行进行解析融合
    partition_num = 64  # 此处分区数量与executors的数量成正比，最好是1~2倍的关系
    data_rdd = snat_sdf.rdd.repartition(partition_num).persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd = data_rdd.map(lambda row: sdec_fusion_gc_by_dict(row, etl_map_dict, pid_map_dict, device_all_dict, conf['mode'])).filter(lambda r: r != None)
    # print(rdd.take(20))
    print(f">>>>>>>>>>>>>>> {time.time()} 开始转换为dataframe >>>>>>>>>>>>>>>>>>>> ")
    fusion_sdf = spark.createDataFrame(rdd, ETL_SCHEMA)
    # fusion_sdf.select("ein", "clt_timestamp", "fuel_injection_mass_hub").filter("fuel_injection_mass_hub is not null").show()

    # 4. output to clickhouse
    url = f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp?rewriteBatchedStatements=true"
    properties = {
        "driver": ck_properties_master['driver'],
        "socket_timeout": "3000000",
        "batchsize": "50000",
        "rewriteBatchedStatements": "true",
        "numPartitions": f"{partition_num}",
        "user": ck_properties_master['user'],
        "password": ck_properties_master['password'],
        "ignore": "true"
    }
    fusion_sdf.printSchema()
    fusion_sdf.write.jdbc(
        url=url,
        table='etl_data_all_v2',
        mode='append',
        properties=properties
    )
    # fusion_sdf.show()
    spark.stop()


# 自定义函数用于处理ein
def process_ein(ein):
    if ein.startswith("0"):
        return ein[1:]
    else:
        return ein


def etl_batch(batch_df, batch_id, query, etl_map_dict, pid_map_dict, device_all_dict, conf, spark):
    batch_cnt = batch_df.count()
    print(f"batchID = {batch_id}, batchSize = {batch_df.count()}")
    if batch_cnt == 0:
        return None

    # batch_df.show()
    t1 = time.time()
    batch_df = batch_df.persist(StorageLevel.MEMORY_ONLY)
    data_rdd = batch_df.rdd.repartition(32)
    rdd = data_rdd.mapPartitions(lambda p: etl_batch_partition(p, batch_id, etl_map_dict, pid_map_dict, device_all_dict, conf)).filter(lambda r: r != None)
    # rdd = data_rdd.map(lambda row: sdec_fusion_gc_by_dict(row, etl_map_dict, pid_map_dict, device_all_dict, conf['mode']))\
    #               .filter(lambda r: r != None)

    fusion_sdf = spark.createDataFrame(rdd, ETL_SCHEMA)
    # fusion_sdf.show()
    print(f"fusion_sdf count: {fusion_sdf.count()}")
    t2 = time.time()

    # 4. output to clickhouse
    url = f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp?rewriteBatchedStatements=true"
    properties = {
        "driver": "ru.yandex.clickhouse.ClickHouseDriver",
        "socket_timeout": "3000000",
        "batchsize": "1000",
        "rewriteBatchedStatements": "true",
        "user": ck_properties_master['user'],
        "password": ck_properties_master['password'],
        "ignore": "true"
    }
    fusion_sdf.write.jdbc(
        url=url,
        table='etl_data_all_v2',
        mode='append',
        properties=properties
    )
    t3 = time.time()
    print(f"fusion time: {t2 - t1}, insert time: {t3 - t2}")


def etl_batch_partition(part, batch_id, etl_map_dict, pid_map_dict, device_all_dict, conf):
    res_list = []
    for row in part:
        res_d = sdec_fusion_gc_by_dict(row, etl_map_dict, pid_map_dict, device_all_dict, conf['mode'])
        res_list.append(res_d)
    return iter(res_list)


def kafka_stream(conf):
    spark = SparkSession.Builder() \
        .appName('sdec_kafka_stream_etl_v2') \
        .config("spark.streaming.concurrentJobs", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")  # suppress some INFO logs

    # 1. load etl_map_all fusion 配置信息
    etl_map_sql = snat_etl_map_loader(conf)
    etl_map_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", etl_map_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load().persist()  # 数据量较小，可直接放入内存持久化

    etl_map_df = etl_map_sdf.toPandas()
    etl_map_dict, pid_map_dict = etl_map_to_dict(etl_map_df)

    # 2. load device_all
    device_all_dict = device_all_loader()

    # 3. kafka message consumer
    kafka_bootstrap_servers = "10.129.65.137:21671,10.129.65.140:21670,10.129.65.136:21669"
    kafka_topic = "dispatcher_sdec_data"
    kafka_group_id = "qjy20472-etl-v2-stream"
    stream_df = spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("group.id", kafka_group_id) \
        .option("maxOffsetsPerTrigger", "1000000") \
        .option("enable.auto.commit", "true") \
        .option("failOnDataLoss", "false") \
        .load()

    # 定义消息格式
    schema = "ein STRING, uploadTime LONG, deviceID STRING, longitude STRING, latitude STRING, " + \
             "items ARRAY<STRUCT<signalname: STRING, name: STRING, value: STRING>>"

    parsed_df = stream_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json("json_value", schema).alias("data")) \
        .selectExpr(
            "IF(substr(data.ein, 1, 1) = '0', substr(data.ein, 2), data.ein) as ein", 
            "from_unixtime(data.uploadTime / 1000, 'yyyy-MM-dd HH:mm:ss') as uploadTime1_",
            "date_format(from_unixtime(data.uploadTime / 1000, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as uploadDate",
            "data.deviceID as vin",
            f"'{conf['data_type']}' as data_type",
            f"'{conf['data_source']}' as data_source",
            "data.longitude",
            "data.latitude",
            "transform(data.items, x -> x.signalname) as codeM", 
            "transform(data.items, x -> x.value) as valueM"
        )

    checkpoint_location = '/home/appuser/python_jobs/pyetl/log/etl_v2_checkpoints'
    query = (parsed_df \
        .writeStream \
        .trigger(processingTime="60 seconds") \
        .foreachBatch(lambda batch, epoch_id: etl_batch(batch, epoch_id, query, etl_map_dict, pid_map_dict, device_all_dict, conf, spark)) \
        .option("checkpointLocation", checkpoint_location)\
        .start())

    query.awaitTermination()


def clickhouse_history(conf):
    # 删除已有数的分区
    t1 = time.time()
    drop_ck_data(conf['date'])
    time.sleep(5)  # 等待n秒，ck删除数据存在延迟
    
    spark_main(conf)
    t2 = time.time()
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!! use time: {t2 - t1} seconds. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


if __name__ == '__main__':
    # t1 = time.time()

    conf = get_params(sys.argv)
    print(conf)

    if conf['mode'] == 'stream':
        kafka_stream(conf)
    elif conf['mode'] == 'history':
        clickhouse_history(conf)
    else:
        raise f"mode请输入 histroy | stream, 当前输入: {conf['mode']}"
