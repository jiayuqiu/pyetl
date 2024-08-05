# -*- encoding: utf-8 -*-
'''
@File    :   test_etl_v2.py
@Time    :   2023/08/31 09:38:49
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   etl v2 单元测试

1. 检查单日，数据条数是否与原始数据一致
2. 随机抽取10个时间切片, 检查发生过单位转换的测点, 转换前后结果是否一致
'''

# here put the import lib
import pandas as pd
import numpy as np
from sqlalchemy import text

import unittest
import datetime
import time
import sys
import os
sys.path.append('/home/qjy20472/pythonProjects/pyetl')
from core.conf import mysql_engine_prod
from core.conf import ck_engine_slave_4 as ck_engine_client
from core.etl.sdec_spark_gc_v2 import convert_to_decimal, etl_map_to_dict
from core.etl.unit import convert_unit_precision


def get_work_idle_data(date, start_unix, end_unix):
    """工作、怠速数据
    """
    data_shape = 50
    etl_work_df = pd.read_sql(
        sql=text(f"""
        select 
            *
        from
            sdecdmp.etl_data_v2
        where
            clt_date = '{date}' 
            and clt_timestamp >= {start_unix}
            and clt_timestamp <= {end_unix}
            and engine_speed > 1000
        ORDER BY RAND()
        LIMIT {data_shape}
        """),
        con=ck_engine_client
    )
    etl_work_df = etl_work_df.loc[(etl_work_df['ein']=='D822700008C') &
                                  (etl_work_df['clt_timestamp']==1688181170)]
    return etl_work_df


def get_ck_data(date, unix_time, ein):
    ck_sql = f"""
        select 
            if(startsWith(ein,'0'),substring(ein,2,LENGTH(ein)),ein) as ein,
            toUnixTimestamp(uploadTime1) as clt_timestamp,
            arrayStringConcat(arrayMap(x -> toString(x), params.valueM), ',') as valueM,
            arrayStringConcat(params.codeM, ',')                              as codeM
        from
            sdecdmp.SDECData2M_all
        where
            uploadDate = '{date}' and ein <> ''
            and clt_timestamp = {unix_time}
        """
    ck_work_df = pd.read_sql(
        sql=text(ck_sql),
        con=ck_engine_client
    )
    ck_work_df = ck_work_df.loc[ck_work_df['ein']==ein]
    ck_work_df.reset_index(drop=True, inplace=True)
    return ck_work_df


class TestEtl(unittest.TestCase):
    def setUp(self,) -> None:
        self.date = '2023-06-01'
        datetime_obj = datetime.datetime.strptime(self.date, '%Y-%m-%d')  # 将日期字符串转换为 datetime 对象
        next_datetime_obj = datetime_obj + datetime.timedelta(days=1)  # 使用 timedelta 增加一天
        self.next_date = next_datetime_obj.strftime('%Y-%m-%d')  # 将 datetime 对象转换为日期字符串
        self.etl_map_df = pd.read_sql(
            sql=f"select * from dmp.etl_map_v2",
            con=mysql_engine_prod
        )
        self.etl_map_dict, self.pid_map_dict = etl_map_to_dict(self.etl_map_df)
        # print(self.etl_map_df)

    def check_val(self, etl_val, org_val, target_unit, signal_unit):
        """检查单位转换后，原始数值与融合后数值

        Args:
            etl_val (obj): 融合后数值对象
            org_val (obj): 原始数值对象
            target_unit (str): 目标单位
            signal_unit (str): 信号单位

        Returns:
            _type_: 0: 数值差异过大, 1: 数值无差异
        """
        # print(etl_val, org_val, target_unit, signal_unit)
        if not isinstance(etl_val, str):
            etl_val = round(etl_val, 7)
            org_val = round(org_val, 7)
            
        if etl_val != org_val:
            if org_val == 0:
                convert_prs = etl_val / (org_val + 1)
            else:
                convert_prs = etl_val / org_val
                
            unit_pres = convert_unit_precision(signal_unit, target_unit)
            if abs(convert_prs - unit_pres) > 0.000001:
                print(f"etl val: {etl_val}, org val: {org_val}")
                print(convert_prs, unit_pres)
                print(1)
                return 0
            else:
                return 1
        else:
            return 1

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')
    
    def test_count(self,):
        """计算单日，数据量差别
           原始数据与融合结果数据量完全一致时，测试通过
        """
        # 1. 获取原始数据数据量
        ck_sdec_df = pd.read_sql(
            sql=f"select count() as cnt from sdecdmp.SDECData2M_all where uploadDate = '{self.date}' and ein <> '' "
                f"and CreateTime <= '{self.next_date} 07:00:00'",
            con=ck_engine_client
        )
        ck_sdec_cnt = ck_sdec_df['cnt'].values[0]
        
        etl_sdec_df = pd.read_sql(
            sql=f"select count() as cnt from sdecdmp.etl_data_all_v2 where clt_date = '{self.date}' "
                f"and data_source = 'sdec'",
            con=ck_engine_client
        )
        etl_sdec_cnt = etl_sdec_df['cnt'].values[0]
        print(f"cnt diff: {abs(etl_sdec_cnt - ck_sdec_cnt)}")

        pass_flag = 0
        print(etl_sdec_cnt, ck_sdec_cnt)
        if abs(etl_sdec_cnt - ck_sdec_cnt) < 1000:
            pass_flag = 1
        self.assertEqual(pass_flag, 1)

    def test_unit_fusion(self, ):
        """对单日，随机抽取n个时间切片，检查单位转换准确性
           这n个时间切片筛选规则：
           1. 处于行车状态，engine > 1000
           随机抽取的切片，原始数据测点数值 : 融合后数值 = 单位转换系数，每个测点等式成立则通过
        """
        date = datetime.datetime.strptime(self.date, "%Y-%m-%d")

        # 获取当天的开始时间
        start_of_day = datetime.datetime(date.year, date.month, date.day, 8, 0, 0)  # 开始时间选择8点
        start_timestamp = int(start_of_day.timestamp())

        # 获取当天的结束时间
        end_of_day = datetime.datetime(date.year, date.month, date.day, 17, 0, 0)  # 结束时间选择17点
        end_timestamp = int(end_of_day.timestamp())
        
        # 在开始和结束时间之间随机时间切片
        print(self.date, start_timestamp, end_timestamp)
        etl_work_df = get_work_idle_data(self.date, start_timestamp, end_timestamp)
        
        error_target_name_list = []
        target_name_cache = []
        for _, etl_row in etl_work_df.iterrows():
            work_df = get_ck_data(self.date, etl_row['clt_timestamp'], etl_row['ein'])
            for _, ck_row in work_df.iterrows():
                if _ > 0:
                    print(11)
                value_m_list = ck_row['valueM'].split(',')
                code_m_list = ck_row['codeM'].split(',')
                
                # 记录pid与数值信息
                pid_val_dict = {}
                for val, pid in zip(value_m_list, code_m_list):
                    unit_match = self.etl_map_df.loc[self.etl_map_df['signal_name']==pid]
                    if unit_match.shape[0] > 0:
                        unit = unit_match['target_unit'].values[0]
                        if not unit is None:
                            val = float(convert_to_decimal(val)) + 0.00000005
                            val = round(val, 7)
                        pid_val_dict[pid] = val
                
                # 对每个etl map中的key 进行数值检查
                for target_name in self.etl_map_dict.keys():
                    if target_name in ['lng', 'lat']:
                        # 经纬度等特定数据，无需转换，在fusion外进行输入
                        pass
                    else:
                        # 非特定字段，检查转换结果
                        etl_val = etl_row[target_name]
                        for pid in self.etl_map_dict[target_name]['pids']:
                            if pid in pid_val_dict.keys():
                                org_val = pid_val_dict[pid]
                                check_ret = self.check_val(
                                    org_val, etl_val, 
                                    self.etl_map_dict[target_name]['target_unit'],
                                    self.pid_map_dict[pid]['signal_unit']
                                )
                                if check_ret == 0:
                                    # 数值转化异常
                                    error_target_name_list.append(pid)
                                break

        # print(set(error_target_name_list), len(error_target_name_list))
        self.assertEqual(len(error_target_name_list), 0)


if __name__ == '__main__':
    unittest.main()
