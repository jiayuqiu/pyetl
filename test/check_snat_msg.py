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
# sys.path.append('/home/qjy20472/pythonProjects/pyetl/core/etl')
# sys.path.append('/home/qjy20472/pythonProjects/pyetl')
# from core.conf import mysql_engine_prod as mysql_engine
# from core.etl.unit import convert_unit_precision


def kv2dict(kv_list):
    res = {}
    for kv in kv_list:
        res[kv['pid']] = kv['value']
    return res


kafka_config = {
    'bootstrap_servers': '10.129.65.137:21671,10.129.65.140:21670,10.129.65.136:21669',
    'group_id': 'qiujiayu',
    'api_version': (2, 2, 0)
}

consumer = KafkaConsumer(
    'dispatcher_sdec_data',
    **kafka_config
)

# consumer = KafkaConsumer(
#     'dispatcher_sdec_data',
#     auto_offset_reset='earliest',
#     bootstrap_servers=['10.129.65.137:21671', '10.129.65.140:21670', '10.129.65.136:21669']
# )

cnt = 0
sum_time = 0
print('wait for data....')
for message in consumer:
    print("%s:%d:%d: key=%s" % (message.topic, message.partition,
                                message.offset, message.key))
    print(1)

print(f"parse avg time: {round(sum_time / cnt, 2)} s.")
