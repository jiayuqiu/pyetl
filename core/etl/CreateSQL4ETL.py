#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: macs
@contact: qjy20472@snat.com
@version: 1.0.0
@license: Apache Licence
@file: CreateSQL4ETL.py
@time: 2023-08-18 13:48:00
@desc: 创建clickhouse建表语句
"""


import pandas as pd
import numpy as np
from sqlalchemy import text

import sys
sys.path.append('/home/qjy20472/pythonProjects/pyetl/')
from core.conf import mysql_engine_prod, ck_engine_master

VERSION = 'v3'


def make_unit(l):
    """
    生成单位信息

    :param l: list, 单位列表
    :return:
    """
    if len(l) == 0:
        # 无单位
        return []
    else:
        # 有单位
        unit_list = []
        for unit in l:
            if isinstance(unit, str):
                # 若单位是字符串类型
                if unit == '-':
                    continue
                elif unit == '':
                    continue
                else:
                    unit_list.append(unit)
        return unit_list


SQL_TEMPLATE = """
create table etl_data_{version}
(
    vin           String comment '车辆识别号',
    ein Nullable(String) comment '发动机号',
    data_source   String comment '整车厂名称/数据来源',
    data_type     String comment '数据类型',
    clt_timestamp Int64 comment '数据采集时间戳',
    clt_time      String comment '数据采集时间',
    clt_date      String comment '数据采集时间(日期)',
    hos_series Nullable(String) comment '车辆所述系列',
    hos_mat_id Nullable(String) comment '订单号',
    extend_feature Nullable(String) comment '拓展分类信息',
    is_delete     String   default '0' comment '逻辑删除，0 - 有效; 1 - 无效',
    create_time   DateTime default toDateTime(now(), 'Asia/Shanghai') comment '数据入库时间',
    update_time Nullable(DateTime) comment '数据更新时间',
    lng Nullable(Float64) comment 'gps经度',
    lat Nullable(Float64) comment 'gps纬度',
    high Nullable(Float64) comment 'gps海拔高度, km',
    {cols}
)
"""


SQL_ENGINE_TAIL = """
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/dmp/etl_data_{version}', '{replica}')
        PARTITION BY (data_source, data_type, clt_date)
        ORDER BY (data_source, data_type, clt_date, vin, clt_timestamp)
        SETTINGS storage_policy = 'dist', index_granularity = 8192;
"""


def make_sql():
    df = pd.read_excel(
        "/home/qjy20472/pythonProjects/pyetl/docs/PID统计_v20230901.xlsx", sheet_name="records"
    )
    print(df.columns)

    # group by target_desc, target_desc is column name for etl table.
    gdf = df.groupby('target_name')
    etl_map_list = []
    cols_str_list = []
    for col_name, group in gdf:
        target_desc = group['target_desc'].values[0]
        
        # 单位
        target_unit_unique = make_unit(group['target_unit'].unique().tolist())
        signal_unit_unique = make_unit(group['unit'].tolist())
        
        if len(target_unit_unique) == 0:
            target_unit_str = None
        else:
            target_unit_str = ';'.join(target_unit_unique)
        
        if len(signal_unit_unique) == 0:
            signal_unit_str = None
        else:
            signal_unit_str = ';'.join(signal_unit_unique)
        
        # 检查配置是否存在目标单位、测点单位仅存一项的异常
        if (signal_unit_str is None) & (target_unit_str is None):
            pass
        elif (not signal_unit_str is None) & (not target_unit_str is None):
            pass
        else:
            raise '存在目标单位、测点单位仅存一项的异常，请检查'
        
        if target_unit_str is None:
            type_ = 'String'
        else:
            type_ = 'Float64'
        cols_str_list.append(f"{col_name} Nullable({type_}) comment '{target_desc}, 单位:{target_unit_str}'")
        
        # 记录每个pid的信息，进行自动化SQL生成
        for _, row in group.iterrows():
            if isinstance(row['unit'], str):
                if (row['unit'] == '-') | (row['unit'] is None) | (row['unit'] == ''):
                    singal_unit = None
                else:
                    singal_unit = row['unit']
            else:
                singal_unit = None

            if singal_unit is None:
                type_ = 'String'
            else:
                type_ = 'Float64'
            etl_map_list.append([col_name, target_desc, target_unit_str, row['pid'], row['Description'], singal_unit, type_])

    cols_str = ",\n".join(cols_str_list)
    sql = SQL_TEMPLATE.format(cols=cols_str) + SQL_ENGINE_TAIL
    print(sql)
    ck_engine_master.execute(text(sql))
    etl_map_df = pd.DataFrame(etl_map_list, columns=['target_name', 'target_desc', 'target_unit', 'signal_name',
                                                     'signal_desc', 'signal_unit', 'signal_type'])
    # etl_map_df.to_csv("/home/qjy20472/pythonProjects/pyetl/docs/etl_map.csv", index=False)
    etl_map_df.loc[:, 'data_source'] = ['sdec'] * etl_map_df.shape[0]

    mysql_engine_prod.execute('truncate dmp.etl_map_v2')
    etl_map_df.to_sql(
        name='etl_map_v2',
        schema='dmp',
        if_exists='append',
        index=False,
        con=mysql_engine_prod
    )
    return etl_map_df


def make_spark_schema(version):    
    """
    automatically generate spark schema definitions

    Translated by Youdao Dictionary

    :return:
    """
    df = pd.read_sql(
        sql=f"""
        select 
            *
        from
            dmp.etl_map_v3
        """,
        con=mysql_engine_prod
    )
    print(df.columns)

    # group by target_desc, target_desc is column name for etl table.
    gdf = df.groupby('target_name')
    cols_str_list = []
    col_cnt = 1
    for col_name, group in gdf:
        target_desc = group['target_desc'].values[0]
        # print(target_desc, col_name, group.shape)

        # 单位
        target_unit_unique = make_unit(group['target_unit'].unique().tolist())
        signal_unit_unique = make_unit(group['signal_unit'].tolist())

        if len(target_unit_unique) == 0:
            target_unit_str = None
        else:
            target_unit_str = ';'.join(target_unit_unique)

        if target_unit_str is None:
            type_ = 'StringType'
            # Record the information of each pid and generate the schema configuration
            cols_str_list.append(f"StructField('{col_name}', {type_}(), True),  # {col_cnt}")
        else:
            type_ = 'DecimalType'
            cols_str_list.append(f"StructField('{col_name}', {type_}(20, 7), True),  # {col_cnt}")

        col_cnt += 1

    cols_str = "\n".join(cols_str_list)
    schema = cols_str
    print(schema)


if __name__ == '__main__':
    make_spark_schema()
    # make_sql()
    print('done.')
