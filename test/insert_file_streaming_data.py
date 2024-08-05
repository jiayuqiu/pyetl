# -*- encoding: utf-8 -*-
'''
@File    :   insert_file_streaming_data.py
@Time    :   2023/09/11 10:05:57
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   往测试文件中插入数据，模仿文件流
'''

# here put the import lib
import pandas as pd
import numpy as np

import os
import time
import logging

# 创建一个Logger对象
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 创建一个文件处理器，设置日志文件名格式
log_filename = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()) + ".log"
file_handler = logging.FileHandler(os.path.join('/home/qjy20472/pythonProjects/pyetl/log/file_stream_test', log_filename))

# 创建一个格式化器，设置日志记录格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 将文件处理器添加到Logger对象中
logger.addHandler(file_handler)


while True:
    with open(os.path.join('/home/qjy20472/pythonProjects/pyetl/log/file_stream_test',
                           time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()) + ".log"), 'w') as f:
        f.write(f"test {time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime())}")
    time.sleep(1)
        
