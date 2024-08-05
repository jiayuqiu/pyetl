# -*- coding: utf-8 -*-
# @Time    : 2023/3/16 下午4:30
# @Author  : qiujiayu
# @File    : sdec.py
# @Software: PyCharm 
# @Comment : 添加新动力tbox数据etl集合
import logging
import time
import datetime
import traceback

import ast
import pandas as pd
import numpy as np
from sqlalchemy import text
from pandarallel import pandarallel
pandarallel.initialize(nb_workers=2)
import concurrent.futures

from core.conf import ck_engine_dict
from core.conf import mysql_engine_prod as mysql_engine
from core.etl.unit import convert_unit_precision
from core.tools.dt import get_time_list, timestamp2unix
from core.tools.mlog import Logger


sdec_logger = Logger('sdec_etl.log').get_logger()


def make_select_col_str(group_conf):
    diff_pid_str_list = []
    target_name = group_conf['target_name'].values[0]
    for _, row in group_conf.iterrows():
        signal_codes = row['signal_code'].split(';')
        if row['signal_unit'] is None:
            units = [None] * len(signal_codes)
        else:
            units = row['signal_unit'].split(';')

        for sc, unit in zip(signal_codes, units):
            if unit is None:
                unit_perision = 0
            else:
                unit_perision = convert_unit_precision(unit, row['target_unit'])

            if unit_perision == 0:
                _case_str = f"when indexOf(params.codeM,'{sc}')>0 then params.valueM[indexOf(params.codeM,'{sc}')]"
            else:
                _case_str = f"when indexOf(params.codeM,'{sc}')>0 then params.valueM[indexOf(params.codeM,'{sc}')] * {unit_perision}"

            diff_pid_str_list.append(_case_str)

    s = 'case' + '\n' + '\n'.join(diff_pid_str_list) + '\n' + f"end as {target_name}"
    return s


def str2list(s):
    try:
        a = np.array(ast.literal_eval(s))
        return a
    except:
        traceback.print_exc()
        return np.array([])


class SnatETL(object):
    def __init__(self, series, conf_dict):
        self.series = series
        self.conf_dict = conf_dict

    def get_etl(self, ):
        """
        获取etl工具类
        :return:
        """
        if self.series in ['D', 'E', 'H', 'R']:
            return DSnatETLfromDB(self.series, self.conf_dict)


class DSnatETLfromDB(object):
    def __init__(self, series, conf_dict):
        self.series = series
        self.conf_dict = conf_dict

        # 1. load etl config
        config_df = pd.read_sql(
            sql=f"""
            SELECT 
                target_name, target_unit, signal_code, signal_unit, data_source, hos_series, data_type, hos_mat_id,
                extend_feature, platname
            FROM 
                dmp.etl_map_all
            WHERE
                data_source = 'snat' and hos_series = '{series}' and signal_code is not null
            """,
            con=mysql_engine
        )
        # config_df = config_df.loc[config_df['target_name'].isin(['longitude', 'fuel_level'])]
        self.config_df = config_df.copy()

        # 2. set standard columns
        self.STANDARD_STR_COLUMNS = ['ein', 'clt_time', 'data_source', 'data_type', 'vin', 'clt_date', 'hos_series',
                                     'platname', 'hos_mat_id', 'extend_feature']

    def load_org_data(self, start_time: str, end_time: str) -> pd.DataFrame:
        """
        读取数据库原始数据切片

        :param start_time: 开始时间
        :param end_time: 结束时间
        :return:
        """
        start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        date_str = '%d-%02d-%02d' % (start_time_dt.year, start_time_dt.month, start_time_dt.day)
        sql = f"""
        select
            t1.ein1,
            t1.deviceID      as vin,
            t2.hos_mat_id    as hos_mat_id,
            t1.uploadTime1_  as uploadTime,
            t1.params.valueM as valueM,
            t1.params.codeM  as codeM,
            t1.longitude,
            t1.latitude,
            t1.uploadDate
        from
        (select
            if(startsWith(ein,'0'),substring(ein,2,LENGTH(ein)),ein) as ein1
            ,deviceID
            ,formatDateTime(uploadTime1, '%Y-%m-%d %H:%M:%S') as uploadTime1_
            ,params.valueM
            ,params.codeM
            ,longitude
            ,latitude
            ,uploadDate
        from
            sdecdmp.SDECData2M_all sma
        where
            sma.uploadDate = '{date_str}'
            and sma.uploadTime1 >= '{start_time}' and sma.uploadTime1 < '{end_time}'
            and params.valueM is not null and params.codeM is not null
            and ein <> '') as t1
        left join (
            select
                hos_id,
                vin,
                hos_mat_id,
                hos_series
            from ck_mysql.device_all
            where
                `type` = 'sdectbox'
        ) t2  on t1.ein1=t2.hos_id
        where t2.hos_series = '{self.series + '系列'}' and t2.vin is not null
        order by (ein1, uploadTime)
        """
        df = pd.read_sql(sql=text(sql), con=ck_engine_dict['master'])
        if df.shape[0] > 0:
            df.loc[:, 'codeM_array'] = df['codeM'].parallel_apply(str2list)
            df.loc[:, 'valueM_array'] = df['valueM'].parallel_apply(str2list)
            df.loc[:, 'clt_timestamp'] = df['uploadTime'].parallel_apply(timestamp2unix)
        return df

    @staticmethod
    def find_signal_value(row, unit_dict):
        """
        找到 pid 对应 codeM的索引，通过索引获取数值
        :param codeM: pid矩阵 1dim
        :param valueM: 数值矩阵 1dim
        :param unit_dict: 单位信息
        :return: 返回对应数值，float或string
        """
        valueM_array = row['valueM_array']
        codeM_array = row['codeM_array']
        signal_codes = unit_dict['signal_code'].split(';')
        if unit_dict['signal_unit'] is None:
            units = [None] * len(signal_codes)
        else:
            units = unit_dict['signal_unit'].split(';')

        for sc, unit in zip(signal_codes, units):
            if unit is None:
                unit_perision = 0
            else:
                unit_perision = convert_unit_precision(unit, unit_dict['target_unit'])

            codeM_match_res = np.argwhere(codeM_array == sc).flatten()
            if codeM_match_res > 0:
                if unit_perision == 0:
                    # string value
                    return str(valueM_array[codeM_match_res[0]])
                else:
                    return float(valueM_array[codeM_match_res[0]]) * unit_perision

    def process(self, df: pd.DataFrame, group_key: dict, group_conf: pd.DataFrame) -> pd.DataFrame:
        """筛选该单位数据

        Args:
            df (pd.DataFrame): ck的valueM与codeM数据
            
            group_key (dict): group key信息
            group_key = {
                'target_name': key[0],
                'supplier': key[1],
                'hos_series': key[2],
                'protocol': key[3],
                'hos_mat_id': key[4],
                'extend_feature': key[5]
            }
            
            group_conf (pd.DataFrame): 该字段详细信息，包括单位等信息

        Returns:
            pd.DataFrame: 转换后的df, columns = ['ein', 'vin', 'clt_timestamp', 'clt_time', group_key['target_name']]
        """
        if group_key['target_name'] == 'lng':
            df = df.loc[:, ['ein1', 'vin', 'hos_mat_id', 'clt_timestamp', 'uploadTime', 'longitude']]
        elif group_key['target_name'] == 'lat':
            df = df.loc[:, ['ein1', 'vin', 'hos_mat_id', 'clt_timestamp', 'uploadTime', 'latitude']]
        else:
            unit_dict = group_conf.iloc[0, :].to_dict()
            df.loc[:, group_key['target_name']] = df.apply(lambda row: self.find_signal_value(row, unit_dict), axis=1)
            df = df.loc[:, ['ein1', 'vin', 'hos_mat_id', 'clt_timestamp', 'uploadTime', group_key['target_name']]]

        df.columns = ['ein', 'vin', 'hos_mat_id', 'clt_timestamp', 'clt_time', group_key['target_name']]
        return df

    def to_sql_dev(self, res_df: pd.DataFrame, unit_dict: dict, start_time, end_time):
        """测试阶段，数据入库

        Args:
            res_df (pd.DataFrame): 结果集
            unit_dict (dict): 单位信息
        """
        with ck_engine_dict['master'].connect() as con:
            # 对测试表入库
            try:
                res_df.to_sql(
                    name='etl_data_all',
                    if_exists='append',
                    index=False,
                    con=con
                )
            except:
                sdec_logger.error(f"dev insert failed. [{start_time}, {end_time}], error: {traceback.format_exc()}")
        
    def to_sql_prod(self, res_df: pd.DataFrame, unit_dict: dict, start_time, end_time):
        """生产环境，数据入库

        Args:
            res_df (pd.DataFrame): 结果集
            unit_dict (dict): 单位信息
        """
        with ck_engine_dict['master'].connect() as con:
            # 对生产表入库
            try:
                res_df.to_sql(
                    name='etl_data_all',
                    if_exists='append',
                    index=False,
                    con=con
                )
            except:
                sdec_logger.error(f"prod insert failed. [{start_time}, {end_time}], error: {traceback.format_exc()}")

    def to_sql(self, res_df, unit_dict, start_time, end_time, chunksize=500):
        if self.conf_dict['env'] == 'dev':
            # 测试
            self.to_sql_dev(res_df, unit_dict, start_time, end_time, )
        elif self.conf_dict['env'] == 'prod':
            self.to_sql_prod(res_df, unit_dict, start_time, end_time, )

    def time_process(self, start_time: str, end_time: str, config_gdf):
        """对每段时间分别进行

        Args:
            start_time (str): 开始时间
            end_time (str): 结束时间
            config_gdf (_type_): 配置分组结果
            分组字段: ['target_name', 'data_source', 'hos_series', 'data_type', 'hos_mat_id', 'extend_feature', 'platname']

        Returns:
            _type_: _description_
        """
        sdec_logger.info(f"start_time: {start_time}, end_Time: {end_time} running...")
        start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        date_str = '%d-%02d-%02d' % (start_time_dt.year, start_time_dt.month, start_time_dt.day)
        tp_t1 = time.time()
        start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        date_str = '%d-%02d-%02d' % (start_time_dt.year, start_time_dt.month, start_time_dt.day)
        res_df = pd.DataFrame()
        unit_dict = {}
        
        # # 对融合结果表中的存在的该段时间进行逻辑删除
        # # 由于对分区表无法进行update进行注释
        # logic_delete_sql = f"ALTER TABLE sdecdmp.etl_data_all UPDATE is_delete = '1' " +\
        #                    f"WHERE clt_date = '{date_str}' and clt_time >= '{start_time}' and clt_time < '{end_time}'"
        
        # with ck_engine.connect() as con:
        #     con.execute(
        #         logic_delete_sql
        #     )
        load_t1 = time.time()
        df = self.load_org_data(start_time, end_time)
        load_t2 = time.time()
        sdec_logger.info(f"{self.series} load time: {round(load_t2-load_t1, 2)} seconds.")
        if df.shape[0] == 0:
            # 本段时间内无数据，不进行融合
            tp_t2 = time.time()
            sdec_logger.info(f"start_time: {start_time}, end_Time: {end_time} has no data.")
            sdec_logger.info(f"start_time: {start_time}, end_Time: {end_time} process time: {int(tp_t2-tp_t1)} seconds.")
            return f"start_time: {start_time}, end_Time: {end_time} process time: {int(tp_t2-tp_t1)} seconds."

        for key, group in config_gdf:
            key_dict = {
                'target_name': key[0],
                'data_source': key[1],
                'hos_series': key[2],
                'data_type': key[3],
                'hos_mat_id': key[4],
                'extend_feature': key[5],
                'platname': key[6]
            }
            unit_dict[key_dict['target_name']] = group['target_unit'].values[0]
            process_res_df = self.process(df, key_dict, group)
            if process_res_df.shape[0] == 0:
                continue
            process_res_df.loc[:, 'platname'] = [group['platname'].values[0]] * process_res_df.shape[0]
            process_res_df.loc[:, 'data_source'] = [group['data_source'].values[0]] * process_res_df.shape[0]
            process_res_df.loc[:, 'data_type'] = [group['data_type'].values[0]] * process_res_df.shape[0]
            process_res_df.loc[:, 'extend_feature'] = [group['extend_feature'].values[0]] * process_res_df.shape[0]

            # 合并结果到res_df
            if res_df.shape[1] == 0:
                res_df = process_res_df.copy()
            else:
                res_df.loc[:, key_dict['target_name']] = process_res_df[key_dict['target_name']]

        sdec_logger.info(f"res_df shape is: {res_df.shape}")
        if res_df.shape[0] > 0:
            # 合并基础信息
            res_df.loc[:, 'clt_date'] = [date_str] * res_df.shape[0]
            res_df.loc[:, 'hos_series'] = [self.series] * res_df.shape[0]
            sql_t1 = time.time()
            self.to_sql(res_df, unit_dict, start_time, end_time)
            sql_t2 = time.time()
            sdec_logger.info(f"sql time: {round(sql_t2-sql_t1, 2)} seconds.")
        del res_df
        tp_t2 = time.time()
        sdec_logger.info(f"{self.series}: start_time: {start_time}, end_Time: {end_time} process time: {round(tp_t2-tp_t1, 2)} seconds.")
        return f"start_time: {start_time}, end_Time: {end_time} process time: {round(tp_t2-tp_t1, 2)} seconds."

    def trans(self, start_time, end_time, mode, time_step=180):
        """
        对ck中的原始数据，进行转换后，插入到中间表
        1. 对conf中 hos_series, extend_feature, pid_col_desc 三个维度分组后进行整合
        2. 对每个分组获取数据
        3. 合并每个col的数据后，输出

        :param start_time: 整合开始时间
        :param end_time: 整合结束时间
        :param mode: elt模式, auto|manual
        :param time_step: etl数据时间步长, 单位：秒
        :return:
        """
        t1 = time.time()
        if len(start_time) == 10:
            start_time += ' 00:00:00'
        if len(end_time) == 10:
            end_time += ' 00:00:00'

        config_gdf = self.config_df.groupby(['target_name', 'data_source', 'hos_series', 'data_type',
                                             'hos_mat_id', 'extend_feature', 'platname'], dropna=False)

        if mode == 'manual':
            time_list = get_time_list(start_time, end_time, time_step)
            
            # -----------------------------------------------------------------------
            # 单线程运算
            for ti in range(len(time_list) - 1):
                self.time_process(time_list[ti], time_list[ti+1], config_gdf, )
            
            # # -----------------------------------------------------------------------
            # # 并行运算
            # with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
            #     futures = []
            #     for ti in range(len(time_list) - 1):
            #         futures.append(executor.submit(self.time_process, time_list[ti], time_list[ti+1], config_gdf, ))
                
            #     for future in concurrent.futures.as_completed(futures):
            #         print(future.result())
            
        t2 = time.time()
        sdec_logger.info(f"start_time: {start_time}, end_Time: {end_time} all use time: {round(t2-t1, 2)} seconds.")
