# -*- coding: utf-8 -*-
# @Time    : 2023/3/16 下午4:18
# @Author  : qiujiayu
# @File    : mlog.py
# @Software: PyCharm 
# @Comment :

import logging
import re
from logging.handlers import TimedRotatingFileHandler


class Logger:
    """日志类"""
    def __init__(self, log_name, log_level=logging.INFO):
        """初始化logger信息"""
        # 日志输出格式
        log_fmt = '%(asctime)s\tFile \"%(filename)s\",line %(lineno)s\t%(levelname)s: %(message)s'
        formatter = logging.Formatter(log_fmt)

        # 设置文件日志
        log_file_handler = TimedRotatingFileHandler(
            filename=log_name,
            when="D",
            interval=1,
            backupCount=7
        )
        log_file_handler.suffix = "%Y-%m-%d.log"
        log_file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
        log_file_handler.setFormatter(formatter)
        # log_file_handler.setLevel(log_level)
        self.logger = logging.getLogger()
        self.logger.addHandler(log_file_handler)

        console = logging.StreamHandler()
        console.setFormatter(formatter)
        self.logger.addHandler(console)
        self.logger.setLevel(log_level)

    def get_logger(self):
        return self.logger
