# -*- encoding: utf-8 -*-
'''
@File    :   check_yj_msg.py
@Time    :   2023/05/04 15:00:04
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   检查跃进kafka中的数据
'''

# here put the import lib
import pandas as pd
import numpy as np
from kafka import KafkaConsumer

import time
import json
import sys
sys.path.append('/home/qjy20472/pythonProjects/pyetl/core/etl')
sys.path.append('/home/qjy20472/pythonProjects/pyetl')
from core.conf import mysql_engine_prod as mysql_engine
from core.etl.unit import convert_unit_precision


def kv2dict(kv_list):
    res = {}
    for kv in kv_list:
        res[kv['pid']] = kv['value']
    return res


def parse_czdf_json(json_string: str, etl_config_df: pd.DataFrame) -> list:
    """解析常州东风json

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
        etl_config_df (pd.DataFrame): etl 配置

    Returns:
        list: 解析后融合需要的字段
    """
    values_list = json.loads(json_string)
    parsed_value_list = []
    
    for val in values_list:
        parsed_value = {
            'ein': val['ein'], 'vin': val['vin'], 'lng': val['lng'], 'lat': val['lat'], 'clt_timestamp': int(int(val['time'])/1000)
        }
        pid_value_dict = kv2dict(val['message'])
        for _, etl_row in etl_config_df.iterrows():
            if etl_row['target_name'] in ['lng', 'lat', 'high']:
                # 在message中，不对gps信息做处理，在message循环外已经加入gps信息
                continue
            signal_codes = etl_row['signal_code'].split(';')
            signal_units = etl_row['signal_unit']
            if signal_units is None:
                # 字符型字段
                for pid in signal_codes:
                    if pid in pid_value_dict.keys():
                        # 实时数据中存在对应pid数据
                        parsed_value[etl_row['target_name']] = pid_value_dict[pid]
                    else:
                        # 若无且之前的signal_code均无实时数据对应，则置空占位
                        # 若之前已经存在signal_code则不做任何处理
                        if parsed_value.get(etl_row['target_name']) is None:
                            parsed_value[etl_row['target_name']] = None
            else:
                # 字符型数据
                for pid, unit in zip(signal_codes, signal_units):
                    if pid_value_dict.get(pid):
                        # 实时数据中存在对应pid数据
                        # 获取单位转换系数
                        coefficient = convert_unit_precision(unit, etl_row['target_unit'])
                        if coefficient == 0:
                            # 不用进行转换
                            parsed_value[etl_row['target_name']] = float(pid_value_dict[pid])
                        else:
                            parsed_value[etl_row['target_name']] = float(pid_value_dict[pid]) * coefficient
                    else:
                        # 若无且之前的signal_code均无实时数据对应，则置空占位
                        # 若之前已经存在signal_code则不做任何处理
                        if parsed_value.get(etl_row['target_name']) is None:
                            parsed_value[etl_row['target_name']] = None
        parsed_value_list.append(parsed_value)
    return parsed_value_list


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
            parsed_value['clt_time'] = val['time']
            parsed_value['pid'] = ele_key
            parsed_value['value'] = val[ele_key]
            parsed_value_list.append(parsed_value)
        
        # 2. 记录message中的gc数据
        for kv in val['message']:
            parsed_value = {}
            if kv['pid'] == 'SAE_J000168': 
                print(kv)
            parsed_value['ein'] = val['ein']
            parsed_value['vin'] = val['vin']
            parsed_value['clt_time'] = val['time']
            parsed_value['pid'] = kv['pid']
            parsed_value['value'] = kv['value']
            parsed_value_list.append(parsed_value)
    return parsed_value_list


consumer = KafkaConsumer('czdf-gc',
                         group_id='qiujiayu',
                         enable_auto_commit=False,
                         auto_offset_reset='latest',
                         bootstrap_servers=['10.129.65.137:21268', '10.129.65.141:21269', '10.129.65.138:21270'])

etl_config_df = pd.read_sql(
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
    

device_config_df = pd.read_sql(
    sql=f"""
    SELECT
        hos_id,
        vin,
        hos_mat_id,
        `type`
    FROM 
        ck.device_all
    WHERE
        `type` = 'czdftbox'
    """,
    con=mysql_engine
)

cnt = 0
sum_time = 0
for message in consumer:
    print ("%s:%d:%d: key=%s" % (message.topic, message.partition,
                                          message.offset, message.key))
    # t1 = time.time()
    # res = parse_czdf_json_exploded(message.value)
    # res_df = pd.DataFrame(res)
    # t2 = time.time()
    # sum_time += (t2 - t1)
    # cnt += 1
    # print(sum_time, cnt)
    # if cnt > 5000:
    #     break

print(f"parse avg time: {round(sum_time / cnt, 2)} s.")
