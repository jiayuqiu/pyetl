# -*- coding: utf-8 -*-
# @Time    : 2023/3/16 下午4:32
# @Author  : qiujiayu
# @File    : import_config.py
# @Software: PyCharm 
# @Comment : 导入配置信息

import sys

import pandas as pd
from sqlalchemy import create_engine

from core.conf import mysql_properties_dev, mysql_properties_prod

mysql_properties_dev['database'] = 'qiujiayu'
mysql_engine_dev = create_engine(
    f"mysql+pymysql://{mysql_properties_dev['user']}:{mysql_properties_dev['password']}" +
    f"@{mysql_properties_dev['host']}:{mysql_properties_dev['port']}/{mysql_properties_dev['database']}?charset=utf8"
)

mysql_properties_prod['database'] = 'dmp'
mysql_engine_prod = create_engine(
    f"mysql+pymysql://{mysql_properties_prod['user']}:{mysql_properties_prod['password']}" +
    f"@{mysql_properties_prod['host']}:{mysql_properties_prod['port']}/{mysql_properties_prod['database']}?charset=utf8"
)

COLUMN_MAP = {
    '目标英文字段': 'target_name',
    '目标中文名': 'target_desc',
    '目标单位': 'target_unit',
    '信号': 'signal_code',
    '单位': 'signal_unit',
    '精度(*系数)': 'multiplier',
    '偏移量(±数值)': 'offset'
}


def import_config(conf_dict: dict, logger) -> int:
    """_summary_

    Args:
        conf_dict (dict): 参数信息
        logger (logger): 日志类

    Returns:
        int: 0|1 是否导入成功
    """
    res = 0
    if conf_dict['config_path'] == '':
        logger.error(f"app <import_config> need config_path, but not input.")
        return res

    if conf_dict['sheet_name'] == '':
        logger.error(f"app <import_config> need sheet_name, but not input.")
        return res

    # 1. load config
    df = pd.read_excel(conf_dict['config_path'], sheet_name=conf_dict['sheet_name'])
    df = df.loc[:, ['目标英文字段', '目标中文名', '目标单位', '信号', '单位', '精度(*系数)', '偏移量(±数值)']]

    # 2. save to sql
    eng_columns = []
    for chn_col in df.columns:
        eng_columns.append(COLUMN_MAP[chn_col])
    df.columns = eng_columns
    df.loc[:, 'platname'] = conf_dict['platname']
    df.loc[:, 'data_source'] = conf_dict['data_source']
    df.loc[:, 'data_type'] = conf_dict['data_type']
    df.loc[:, 'hos_series'] = conf_dict['hos_series']
    # df.loc[:, 'id'] = list(range(1, df.shape[0]+1))

    if conf_dict['env'] == 'dev':
        mysql_engine = mysql_engine_dev
    elif conf_dict['env'] == 'prod':
        mysql_engine = mysql_engine_prod
        
    df.to_sql(
        name='etl_map_all',
        if_exists='append',
        index=False,
        con=mysql_engine
    )
    res = 1
    return res
