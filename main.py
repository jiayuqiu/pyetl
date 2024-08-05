# -*- coding: utf-8 -*-
# @Time    : 2023/3/16 下午4:31
# @Author  : qiujiayu
# @File    : main.py
# @Software: PyCharm 
# @Comment : etl 执行脚本

import getopt
import traceback
import sys

from core.tools.mlog import Logger
from core.conf import mysql_engine_dev, mysql_engine_prod
from core.conf import ck_engine_slave_1 as ck_engine
from core.etl.sdec import SnatETL
from core.etl.czdf_gc import CzdfGcEtl
from core.import_config import import_config


def get_params(agrs: list) -> dict:
    """
    获取脚本需要的参数

    :param agrs: sys.args
    :return:
    """
    start_time = ''
    end_time = ''
    platname = None
    data_source = None
    delivery = None
    hos_series = None
    hos_mat_id = None
    app = None
    env = 'dev'
    config_path = None
    sheet_name = None
    mode = 'manual'
    data_type = 'gc'

    p = ['help', 'starttime=', 'endtime=', 'env=', 'mode=', 'platname=', 'data_source=', 'data_type=', 
         'delivery=', 'hos_series=', 'app=', 'hos_mat_id=', 'config_path=', 'sheet_name=']
    opts, args = getopt.getopt(agrs[1:], '-h-s:-e:-m:E:', p)
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '--help'):
            print("[*] Help info")
            exit()

        if opt_name in ('-s', '--starttime'):
            start_time = opt_value
            print("[*] starttime is ", start_time)

        if opt_name in ('-e', '--endtime'):
            end_time = opt_value
            print("[*] endtime is ", end_time)

        if opt_name in ('-m', '--mode'):
            mode = opt_value
            print("[*] mode is ", mode)
        
        if opt_name in ('--platname'):
            platname = opt_value
            print("[*] platname is ", platname)

        if opt_name in ('-E', '--env'):
            env = opt_value
            print("[*] env is ", env)

        if opt_name in ('--data_source', ):
            data_source = opt_value
            print("[*] data_source is ", data_source)

        if opt_name in ('--delivery', ):
            delivery = opt_value
            print("[*] delivery is ", delivery)

        if opt_name in ('--hos_series', ):
            hos_series = opt_value
            print("[*] hos_series is ", hos_series)

        if opt_name in ('--hos_mat_id', ):
            hos_mat_id = opt_value
            print("[*] hos_mat_id is ", hos_mat_id)
            
        if opt_name in ('--data_type', ):
            data_type = opt_value
            print("[*] data_type is ", data_type)

        if opt_name in ('--app', ):
            app = opt_value
            print("[*] app is ", app)

        if opt_name in ('--config_path', ):
            """
            若app = import_config, 才会生效
            """
            config_path = opt_value
            print("[*] config_path is ", config_path)

        if opt_name in ('--sheet_name', ):
            """
            若app = import_config, 才会生效
            """
            sheet_name = opt_value
            print("[*] sheet_name is ", sheet_name)
    
    conf = {
        'start_time': start_time,
        'end_time': end_time,
        'platname': platname,
        'data_source': data_source,
        'data_type': data_type,
        'delivery': delivery,
        'hos_series': hos_series,
        'hos_mat_id': hos_mat_id,
        'app': app,
        'env': env,
        'mode': mode,
        'config_path': config_path,
        'sheet_name': sheet_name
    }
    return conf


def connect_engine(env, logger):
    if env == 'prod':  # 线上环境
        mysql_engine = mysql_engine_prod
    else:
        mysql_engine = mysql_engine_dev

    try:
        ck_engine.connect()
    except Exception as e:
        print(e)
        logger.error(f"ck链接失败.", traceback.format_exc())

    try:
        mysql_engine.connect()
    except Exception as e:
        print(e)
        logger.error(f"mysql链接失败.", traceback.format_exc())
    return ck_engine, mysql_engine


def main():
    conf = get_params(sys.argv)
    logger = Logger('etl.log').get_logger()

    if conf['app'] == 'etl':
        print('etl')
        if conf['data_source'] == 'snat':
            dsetl = SnatETL(conf['hos_series'], conf).get_etl()
            dsetl.trans(conf['start_time'], conf['end_time'], conf['mode'])
        elif conf['data_source'] == 'czdf':
            print('czdf')
            czdf_etl = CzdfGcEtl(conf).get_etl()
            czdf_etl.trans(conf['start_time'], conf['end_time'], conf['mode'])
    elif conf['app'] == 'import_config':
        import_res = import_config(
            conf,
            logger
        )
        if import_res == 1:
            logger.info(f"import config save success.")
        else:
            logger.error(f"import config failed.")
    else:
        logger.info(f"无当前应用, 输入app: {conf['app']}")


if __name__ == '__main__':
    main()
