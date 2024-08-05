#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：pyetl 
@File    ：translate_desc.py
@Author  ：qjy20472
@Date    ：2023/8/8 14:28 
@Desc    : 翻译pid整合中，没有target_name的pid
"""

import time
import os
import traceback

import pandas as pd
from core.tools.YouDaoTranslate.trans import createRequest


res_cache = {}


if __name__ == "__main__":
    workpath = '/home/qjy20472/pythonProjects/pyetl/'
    df = pd.read_excel(
        os.path.join(workpath, 'data/PID统计.xlsx')
    )
    print(list(df.columns))
    eng_desc_list = []
    for _, row in df.iterrows():
        if row['Description'] in res_cache:
            # 已经存在翻译结果，直接使用
            eng_desc = res_cache[row['Description']]
        else:
            try:
                eng_desc = createRequest(row['Description'])
                eng_desc = eng_desc.lower()
                eng_desc = eng_desc.replace(' ', '_')
                eng_desc = eng_desc.replace('-', '_')
                res_cache[row['Description']] = eng_desc
            except:
                traceback.print_exc()
                eng_desc = ''
        eng_desc_list.append(eng_desc)
        print(row['Description'], eng_desc)
        time.sleep(1)
    df.loc[:, 'eng_desc'] = eng_desc_list
    df = df.loc[:,
         ['pid', 'Description', 'desc', 'target_name', 'eng_desc', 'target_unit', 'unit', 'app']]
    df.to_csv(
        os.path.join(workpath, 'data/pid_with_eng.csv'), encoding='utf-8-sig'
    )
