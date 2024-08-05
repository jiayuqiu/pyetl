# -*- encoding: utf-8 -*-
'''
@File    :   structured_streaming_czdf.py
@Time    :   2023/05/07 15:01:14
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   czdf解析
'''

# here put the import lib
import pandas as pd
import numpy as np


import json
import time
import datetime
import pandas as pd
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, struct, array, lit, expr, from_json, to_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, TimestampType, DateType
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.functions import pandas_udf
from pyspark.sql.streaming import DataStreamWriter

import sys
sys.path.append('/home/qjy20472/pythonProjects/pyetl/core/etl')
sys.path.append('/home/qjy20472/pythonProjects/pyetl')
from core.conf import mysql_engine_prod as mysql_engine

from unit import convert_unit_precision


def unix2timestamp(t: int, fmt="%Y-%m-%d %H:%M:%S") -> str:
    """unix时间转字符串

    Args:
        t (int): Unix时间
        fmt (str, optional): 输出字符串时间格式. Defaults to "%Y-%m-%d %H:%M:%S".

    Returns:
        str: 字符串时间
    """
    time_local = time.localtime(t)
    dt = time.strftime(fmt, time_local)
    return dt

def unix2datetime(t: int, fmt="%Y-%m-%d %H:%M:%S") -> datetime:
    """_summary_

    Args:
        t (int): _description_
        fmt (str, optional): _description_. Defaults to "%Y-%m-%d %H:%M:%S".

    Returns:
        datetime: _description_
    """
    dt = datetime.datetime.fromtimestamp(t)
    return dt


spark = SparkSession.Builder() \
    .appName('KafkaStructuredStreaming') \
    .master('local[*]') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")  # suppress some INFO logs


data_schema = StructType([
    StructField("ein", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("clt_timestamp", IntegerType(), True),
    StructField("pid", StringType(), True),
    StructField("value", StringType(), True),
])

msg_schema = schema = ArrayType(data_schema, True)

# target_name, target_unit, signal_code, signal_unit, data_source, hos_series, data_type, hos_mat_id,
# extend_feature, platname
etl_config_schema = StructType([
    StructField("pid", StringType(), True),
    StructField("target_name", StringType(), True),
    StructField("coefficient", FloatType(), True),
    StructField("data_source", StringType(), True),
    StructField("hos_series", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("hos_mat_id", StringType(), True),
    StructField("extend_feature", StringType(), True),
    StructField("platname", StringType(), True),
])

signal_all_schema = StructType([
    StructField("pid", StringType(), True),
    StructField("signal_multiplier", FloatType(), True),
    StructField("signal_offset", FloatType(), True),
])

# 'data_type', 'data_source', 'platname'
czdf_data_schema = StructType([
    StructField("ein", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("platname", StringType(), True),
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

czdf_parsed_schema = ArrayType(czdf_data_schema, True)


def parse_czdf_json_exploded(json_string: str) -> list :
    """解析常州东风json
    
    最终输出格式如下: 
    [{'ein': 'ein1', 'vin': 'vin1', 'clt_timestamp': t1, 'pid': 'pid', 'value': 'val1'}]
    其中，value字段都作为string处理，在最后融合的时候通过udf来进行转换类型

    Args:
        json_string (str): 常州东风kafka消息中, 数据部分
            json_string通过json.loads转换后，是一个list
            list中，每个元素是一个dict：
                ein -> 发动机号
                vin -> 车架号
                time -> 时间戳，int类型，单位 毫秒
                sn -> tbox设备的序列号
                lng -> 经度
                lat -> 纬度
                message -> pid数据部分，list，list中每个元素{'pid': pid1, 'value': 'val1'}

    Returns:
        list: 解析后融合需要的字段，解析结果
    """
    if json_string is None:
        return None
    
    values_list = json.loads(json_string)
    parsed_value_list = []
    for val in values_list:
        # 1. 记录ein, vin, clt_timestamp, lng, lat, high
        for ele_key in ['lng', 'lat', 'high']:
            parsed_value = {}
            parsed_value['ein'] = val['ein']
            parsed_value['vin'] = val['vin']
            parsed_value['clt_timestamp'] = int(int(val['time'])/1000)
            parsed_value['pid'] = ele_key
            try:
                parsed_value['value'] = str(val[ele_key])
            except:
                parsed_value['value'] = None
            parsed_value_list.append(parsed_value)
        
        # 2. 记录message中的gc数据
        for kv in val['message']:
            parsed_value = {}
            parsed_value['ein'] = val['ein']
            parsed_value['vin'] = val['vin']
            parsed_value['clt_timestamp'] = int(int(val['time'])/1000)
            parsed_value['pid'] = kv['pid']
            try:
                parsed_value['value'] = str(kv['value'])
            except:
                parsed_value['value'] = None
            parsed_value_list.append(parsed_value)
    return parsed_value_list


parse_czdf_udf = udf(parse_czdf_json_exploded, returnType=msg_schema)


etl_config_pdf = pd.read_sql(
    sql=f"""
    SELECT 
        target_name, target_unit, signal_code, signal_unit, data_source, hos_series, data_type, hos_mat_id,
        extend_feature, platname
    FROM 
        dmp.etl_map_all
    WHERE
        data_source = 'czdf' and signal_code is not null
    """,
    con=mysql_engine
)

# 转换map格式，用于与解析结果进行join
# 输出格式[{'pid': aaa, 'target_name': bbb, 'coefficient': ccc, 'data_source': ddd, 'hos_series': eee,
#          'data_type': fff, 'hos_mat_id': ggg, 'extend_feature': hhh, 'platname': iii}]
elt_config_exploded_list = []
for _, row in etl_config_pdf.iterrows():
    signal_codes = row['signal_code'].split(';')
    if row['signal_unit'] is None:
        # 字符型数据
        for pid in signal_codes:
            pid_dict = {'pid': pid, 'target_name': row['target_name'], 'coefficient': 0., 
                        'data_source': row['data_source'], 'hos_series': row['hos_series'],
                        'data_type': row['data_type'], 'hos_mat_id': row['hos_mat_id'], 
                        'extend_feature': row['extend_feature'], 'platname': row['platname']}
            elt_config_exploded_list.append(pid_dict)
    else:
        # 数值型数据
        signal_units = row['signal_unit'].split(';')
        if row['target_name'] == 'lng':
            pid_dict = {'pid': 'lng', 'target_name': row['target_name'], 'coefficient': float(1), 
                        'data_source': row['data_source'], 'hos_series': row['hos_series'],
                        'data_type': row['data_type'], 'hos_mat_id': row['hos_mat_id'], 
                        'extend_feature': row['extend_feature'], 'platname': row['platname']}
            elt_config_exploded_list.append(pid_dict)
            continue
        
        if row['target_name'] == 'lat':
            pid_dict = {'pid': 'lat', 'target_name': row['target_name'], 'coefficient': float(1), 
                        'data_source': row['data_source'], 'hos_series': row['hos_series'],
                        'data_type': row['data_type'], 'hos_mat_id': row['hos_mat_id'], 
                        'extend_feature': row['extend_feature'], 'platname': row['platname']}
            elt_config_exploded_list.append(pid_dict)
            continue
        
        if row['target_name'] == 'high':
            pid_dict = {'pid': 'high', 'target_name': row['target_name'], 'coefficient': float(1), 
                        'data_source': row['data_source'], 'hos_series': row['hos_series'],
                        'data_type': row['data_type'], 'hos_mat_id': row['hos_mat_id'], 
                        'extend_feature': row['extend_feature'], 'platname': row['platname']}
            elt_config_exploded_list.append(pid_dict)
            continue
        
        for pid, unit in zip(signal_codes, signal_units):
            coefficient = convert_unit_precision(unit, row['target_unit'])
            pid_dict = {'pid': pid, 'target_name': row['target_name'], 'coefficient': float(coefficient), 
                        'data_source': row['data_source'], 'hos_series': row['hos_series'],
                        'data_type': row['data_type'], 'hos_mat_id': row['hos_mat_id'], 
                        'extend_feature': row['extend_feature'], 'platname': row['platname']}
            elt_config_exploded_list.append(pid_dict)

etl_config_sdf = spark.createDataFrame(elt_config_exploded_list, schema=etl_config_schema)

signal_all_config_df = pd.read_sql(
    sql=f"""
    SELECT 
        signalcode, multiplier, offset
    FROM 
        ck.signal_all
    WHERE 
        isfaultcode = 0
    """,
    con=mysql_engine
)

# 格式转换signal_all表，用于与解析结果进行join
# 输出格式：[{'signalcode': 'pid1', 'multiplier': v1, 'offset': v2}]
# signalcode信号名，multiplier信号值的系数，offset信号值的偏移量
singal_all_config_list = []
for _, row in signal_all_config_df.iterrows():
    singal_all_config_list.append({'pid': row['signalcode'], 'signal_multiplier': row['multiplier'], 'signal_offset': row['offset']})
signal_all_sdf = spark.createDataFrame(singal_all_config_list, schema=signal_all_schema)

kafka_group_id = "qjy20472-spark-app"
stream_df = spark.readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "10.129.65.137:21268,10.129.65.141:21269,10.129.65.138:21270") \
    .option("subscribe", "czdf-gc") \
    .option("startingOffsets", "latest") \
    .option("group.id", kafka_group_id) \
    .option("maxOffsetsPerTrigger", "100") \
    .option("enable.auto.commit", "true") \
    .option("failOnDataLoss", "false") \
    .load()

stream_df = stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")\
       .select(
           explode(parse_czdf_udf('value')).alias('datas'), col('partition'), col('offset')
       )
    
# 将打散后的 DataFrame 中的数据结构应用于新的 schema 类型，并重新创建新的 DataFrame 对象
for field_name in data_schema.fieldNames():
    stream_df = stream_df.withColumn(field_name, stream_df["datas"][field_name].cast(data_schema[field_name].dataType))

parsed_df = stream_df.select(
    col('ein'), col('vin'), col('clt_timestamp'), col('pid'), col('value'), 
)

# Join streaming dataframe with etl_config_sdf
join_df = parsed_df.join(etl_config_sdf, on=["pid"], how="left")

# Join streaming dataframe with signal_all_sdf
join_df = join_df.join(signal_all_sdf, on=['pid'], how='left')
# join_df.show()

# Generate new_age column based on the multiplication of age and mult columns 
# join_df = join_df.withColumn("new_age", expr("age * mult"))

def write_to_mysql(df, epoch_id):
    url = "jdbc:mysql://localhost/dmp"
    
    # print('mysql writing.')
    # 设置批处理大小为1000条
    batch_size = 1000
    num_partition = (df.count() // batch_size) + 1
    # print(f"num_partition = {num_partition}")
    properties = {
      "user": "qiujiayu",
      "password": "123456",
      "driver": "com.mysql.jdbc.Driver",
      "characterEncoding": "utf8",
      "numPartitions": str(num_partition),
      "batchsize":  str(batch_size)
    }

    df.repartition(num_partition).write\
      .jdbc(url=url, table="etl_streaming_join_test", mode="append", properties=properties)
    # print('mysql finish.')


@pandas_udf(czdf_data_schema, functionType=PandasUDFType.GROUPED_MAP)
def format_data_udf(df):
    # print(f"udf batch time: {df['clt_timestamp'].values[0]}")
    format_data = {}
    # print(f"clt_timestamp: {df['clt_timestamp'].values[0]}")
    
    # 添加固定信息
    format_data['ein'] = df['ein'].values[0]
    format_data['vin'] = df['vin'].values[0]
    format_data['platname'] = df['platname'].values[0]
    format_data['data_source'] = df['data_source'].values[0]
    format_data['data_type'] = df['data_type'].values[0]
    format_data['clt_timestamp'] = df['clt_timestamp'].values[0]
    format_data['clt_time'] = int(df['clt_timestamp'].values[0]) * 1000000
    format_data['clt_date'] = unix2datetime(df['clt_timestamp'].values[0]).date()
    format_data['hos_mat_id'] = df['hos_mat_id'].values[0]
    # print(11111, format_data)
    for field_name in czdf_data_schema.fieldNames():
        if format_data.get(field_name):
            # 此前已经经过初始化的key: ein, vin 等直接跳过获取数据阶段。否则会导致已经初始化的数据数值被覆盖为None
            continue
            
        # 对需要的每个字段来查看是否有对应的解析结果
        field_df_match = df.loc[df['target_name'] == field_name]
        if field_df_match.shape[0] > 0:
            # 区分字符型与数值型数据
            if czdf_data_schema[field_name].dataType == StringType():
                # 若是字符型数据，不进行转换
                # 后期若需要做统一含义，则在此进行再添加功能
                field_fusion_val = field_df_match['value'].values[0]
            else:
                # 解析原始数据
                field_parsed_val = float(field_df_match['value'].values[0]) * float(field_df_match['signal_multiplier'].values[0]) + \
                                   float(field_df_match['signal_offset'].values[0])
                # if field_name in ['lng', 'lat']:
                #     print(field_df_match.loc[:, ['pid', 'value', 'signal_multiplier', 'signal_offset', 'coefficient']])
                #     field_parsed_val = float(field_df_match['value'].values[0]) * float(field_df_match['signal_multiplier'].values[0]) + \
                #                        float(field_df_match['signal_offset'].values[0])
                #     print(f"field_name: {field_name}, parsed: {field_parsed_val}")
                
                # 对匹配到的结果，进行单位转换
                if field_df_match['coefficient'].values[0] == 0:
                    # 无需转换
                    field_fusion_val = field_parsed_val
                else:
                    field_fusion_val = field_parsed_val * float(field_df_match['coefficient'].values[0])
                
                # if field_name in ['lng', 'lat']:
                #     field_parsed_val = float(field_df_match['value'].values[0]) * float(field_df_match['signal_multiplier'].values[0]) + \
                #                        float(field_df_match['signal_offset'].values[0])
                #     print(f"field_name: {field_name}, fusion: {field_fusion_val}")
                
            # file_name作为新的key放到format_data中
            format_data[field_name] = field_fusion_val
        else:
            # 没有匹配到对应的pid实时数据，赋空值
            format_data[field_name] = None
    # print(22222, format_data)
    return pd.DataFrame([format_data])


def row_write_to_ck(df, epoch_id, query):
    # mysql properties
    # properties = {
    #   "url": url = "jdbc:mysql://localhost/dmp",
    #   "user": "qiujiayu",
    #   "password": "123456",
    #   "driver": "com.mysql.jdbc.Driver",
    #   "characterEncoding": "utf8",
    # }
    # print(epoch_id)
    ck_properties = {
        # "driver": "com.github.housepower.jdbc.ClickHouseDriver",
        "driver": "ru.yandex.clickhouse.ClickHouseDriver",
        "socket_timeout": "300000",
        "rewriteBatchedStatements": "true",
        "batchsize": "1000000",
        "numPartitions": "8",
        "user": "default",
        "password": "5NsWWQeJ",
    }
    
    # print(f"row_write_to_ck 1")
    df = df \
        .withColumn('signal_multiplier', when(col('signal_multiplier').isNull(), 1.0).otherwise(col('signal_multiplier'))) \
        .withColumn('signal_offset', when(col('signal_offset').isNull(), 0).otherwise(col('signal_offset')))
    # print(f"row_write_to_ck 2")
    row_df = df.groupby(['ein', 'clt_timestamp']).apply(format_data_udf)
    # print(f"row_write_to_ck 3")
    row_df.write\
      .jdbc(url=f"jdbc:clickhouse://10.129.165.72:8123/sdecdmp", 
            table="etl_data_all", mode="append", properties=ck_properties)
    # print('insert finish.')


# # Write the parsed DataFrame to console for debugging purposes
# query = parsed_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false")\
#     .option("checkpointLocation", "/tmp/checkpoints") \
#     .start()

# 将结果写入数据库
query = (join_df \
    .writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(lambda batch, epoch_id: row_write_to_ck(batch, epoch_id, query)) \
    .option("checkpointLocation", "./checkpoints_qjy")\
    .start())

# Wait for the stream to finish
query.awaitTermination()
