#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 6/25/23 10:39 PM
# @Author  : qjy20472
# @Site    : 
# @File    : check_df_dict.py
# @Software: IntelliJ IDEA
# @Description: 对比df筛选与dict把ein作为key筛选的效率

import time
import pandas as pd
from core.conf import ck_engine_dict

device_all_df = pd.read_sql(
    sql=f"SELECT hos_id as ein, hos_mat_id, hos_series, type as tbox_type FROM ck_mysql.device_all",
    con=ck_engine_dict['master']
)
# print(device_all_df.loc[device_all_df['hos_series']=='D系列'])


device_all_dict = {}
for _, row in device_all_df.iterrows():
    device_all_dict[row['ein']] = {
        'hos_mat_id': row['hos_mat_id'],
        'hos_series': row['hos_series'],
        'tbox_type': row['tbox_type'],
    }

ein_list = ['D819400013B', 'D9234003732', 'D9233003108', 'xxx']


# 分别筛选10000次
# 添加device_all中的series与mat_id信息
t1 = time.time()
loop_n = 1000
for ein in ein_list:
    cnt1 = 0
    while cnt1 < 1000:
        print(f"1 {ein}, {cnt1}")
        device_match_res = device_all_df.loc[(device_all_df['ein']==ein) &
                                             (device_all_df['tbox_type']=='sdectbox')]
        if device_match_res.shape[0] > 0:
            s = device_match_res['hos_series'].values[0]
            m = device_match_res['hos_mat_id'].values[0]
        else:
            s = None
            m = None
        cnt1 += 1

t2 = time.time()
for ein in ein_list:
    cnt2 = 0
    while cnt2 < loop_n:
        print(f"2 {ein}, {cnt2}")
        if ein in device_all_dict.keys():
            s = device_all_dict[ein]['hos_series']
            m = device_all_dict[ein]['hos_mat_id']
        else:
            s = None
            m = None
        cnt2 += 1
t3 = time.time()

print(f"df use time: {t2 - t1} seconds.")
print(f"dict use time: {t3 - t2} seconds.")