# -*- encoding: utf-8 -*-
'''
@File    :   sdec_spark_gc_v3.py
@Time    :   2023/08/24 10:19:03
@Author  :   qiujiayu 
@Version :   2.0
@Contact :   qjy20472@snat.com
@Desc    :   sdec tbox数据融合第三版, 对比第二版新增最小、最大值合理范围判断。
'''

# here put the import lib

# import third-party modules
import pandas as pd
import numpy as np
import json

# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit
from pyspark.storagelevel import StorageLevel
# from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import sum as spark_sum

# import python lib
import os
import sys
import getopt
import time
import re
import traceback
import logging
from decimal import Decimal

# Import the ETL custom module
cur_file_path = os.path.abspath(__file__)
pydmp_path = os.path.dirname(os.path.dirname(os.path.dirname(cur_file_path)))
print(f"pyetl path = {pydmp_path}")
sys.path.append(pydmp_path)
from core.etl.utils.SchemaV3 import ETL_SCHEMA, ETL_SCHEMA_TEST
from core.conf import ck_engine_dict, ck_properties_master
from core.conf import ck_properties_slave_4 as ck_properties_slave
from core.tools.dt import timestamp2unix, unix2datetime
from core.etl.unit import convert_unit_precision


ERROR_VAL = -999


def get_params(agrs: list) -> dict:
    """
    获取脚本需要的参数

    :param agrs: sys.args
    :return:
    """
    date = ''
    data_source = None
    hos_series = 'all'
    env = 'dev'
    data_type = 'gc'
    mode = ''

    p = ['help', 'date=', 'env=', 'data_source=', 'data_type=', 'mode=']
    opts, args = getopt.getopt(agrs[1:], '-h', p)
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '--help'):
            print("[*] Help info")
            exit()

        if opt_name in ('--date'):
            date = opt_value
            print("[*] date is ", date)

        if opt_name in ('--env'):
            env = opt_value
            print("[*] env is ", env)

        if opt_name in ('--data_source',):
            data_source = opt_value
            print("[*] data_source is ", data_source)

        if opt_name in ('--data_type',):
            data_type = opt_value
            print("[*] data_type is ", data_type)

        if opt_name in ('--mode',):
            mode = opt_value
            print("[*] mode is ", mode)

    if date == '':
        raise '请输入日期'
    
    conf = {
        'date': date,
        'data_source': data_source,
        'data_type': data_type,
        'hos_series': hos_series,
        'env': env,
        'mode': mode,
    }
    return conf


def device_all_loader() -> dict:
    """
    load table "device_all", converting dataframe to dict can decrease fusion time.

    :param df:
    :return:
    """
    # 生成device_all数据字典，ein存在重复现象，取create_time最新的那个为准
    device_all_df = pd.read_sql(
        sql=f"""
        SELECT t.ein, any(t.hos_mat_id) as hos_mat_id , any(t.hos_series) as hos_series, any(t.tbox_type) as tbox_type
        FROM (
            SELECT hos_id AS ein, hos_mat_id, hos_series, type AS tbox_type
            FROM ck_mysql.device_all
            ORDER BY hos_id, create_time DESC
        ) AS t
        GROUP BY t.ein
        """,
        con=ck_engine_dict['slaves'][0]
    )

    device_all_dict = {}
    for _, row in device_all_df.iterrows():
        device_all_dict[row['ein']] = {
            'hos_mat_id': row['hos_mat_id'],
            'hos_series': row['hos_series'],
            'tbox_type': row['tbox_type'],
        }
    return device_all_dict


def snat_etl_map_loader() -> str:
    """
    读取etl map
    """
    sql = f"""
           SELECT
               target_name, target_unit, signal_name, signal_unit, signal_type, extend_feature,
               min_val, max_val, default_val, data_source
           FROM
               dmp_mysql.etl_map_v3
           WHERE
               data_source = 'sdec'
           """
    print(sql)
    return sql


def etl_map_to_dict(df: pd.DataFrame) -> tuple:
    """etl配置表 dataframe 转 dict, 用于增加融合效率

    Args:
        df (pd.DataFrame): etl配置表 dataframe

    Returns:
        (tuple):
        t0: etl_map_dict, 配置字典, 包含采集数值相关配置
            {'<target_name>': {'<pids>': {'<pid1>': ... }, 'target_unit': 'kg'}}

        t1: extra_map_dict, 配置字典, 包含拓展数值相关配置(通过采集数值计算得到;other case;)
            {'<target_name>': {'extend_type': 'calculate', 'extend_formula': '...'}}

        t2: pid_map_dict, 配置字典, 记录每个pid对应配置,
            {'signal_name': {'signal_type': 'float', 'signal_unit', 'km', 'target_unit': 'km',
                             'min_val': 0, 'max_val': 100, 'default_val': -999}}
    """
    etl_map_dict = {}
    extend_map_dict = {}
    pid_map_dict = {}
    for target_name, group in df.groupby('target_name'):
        etl_map_dict[target_name] = {'pids': {}, 'target_unit': group['target_unit'].values[0]}
        for _, row in group.iterrows():
            if target_name in ['lng', 'lat']:
                # 经纬度等特定数据，无需转换，在fusion外进行输入
                pass
            else:
                # 非特定字段，需要单位转换
                etl_map_dict[target_name]['pids'][row['signal_name']] = {
                    'signal_type': row['signal_type'],
                    'signal_unit': row['signal_unit'],
                    'target_unit': group['target_unit'].values[0],
                    'min_val': row['min_val'],
                    'max_val': row['max_val'],
                    'default_val': row['default_val']
                }
                pid_map_dict[row['signal_name']] = {
                    'signal_type': row['signal_type'],
                    'signal_unit': row['signal_unit'],
                    'target_unit': group['target_unit'].values[0],
                    'min_val': row['min_val'],
                    'max_val': row['max_val'],
                    'default_val': row['default_val']
                }

                # 添加拓展字段配置信息
                if row['extend_feature'] is None:
                    # 拓展信息为空 pass
                    pass
                else:
                    try:
                        extend_map_dict[target_name] = json.loads(row['extend_feature'])
                        extend_map_dict[target_name]['min_val'] = row['min_val']
                        extend_map_dict[target_name]['max_val'] = row['max_val']
                        extend_map_dict[target_name]['default_val'] = row['default_val']
                    except:
                        traceback.print_exc()

    return etl_map_dict, extend_map_dict, pid_map_dict


def snat_data_loader(conf: dict, ) -> str:
    """
    读取ck原始数据

    :param conf: 任务配置
    :return: 原始数据获取sql
    """
    snat_sql = f"""
    select if(startsWith(ein, '0'), substring(ein, 2, LENGTH(ein)), ein)    as ein1
        , deviceID                                                          as vin
        , formatDateTime(uploadTime1, '%Y-%m-%d %H:%M:%S')                  as uploadTime1_
        , toUnixTimestamp(uploadTime1)                                      as clt_timestamp
        -- , arrayStringConcat(arrayMap(x -> toString(x), params.valueM), ',') as valueM
        -- , arrayStringConcat(params.codeM, ',')                              as codeM
        , toString(params.valueM) as valueM
        , toString(params.codeM) as codeM
        , longitude
        , latitude
        , toString(uploadDate)                                as uploadDate
    from sdecdmp.SDECData2M_all sma
    where
        sma.uploadDate = '{conf['date']}'
        and params.valueM is not null and params.codeM is not null
        and ein <> ''
    limit 1000000
    """
    return snat_sql


def drop_ck_data(clt_date, ):
    drop_sql = f"""
    alter table etl_data_v3 drop partition ('sdec', 'gc', '{clt_date}')
    """
    # # delete master
    # ck_engine_dict['master'].execute(
    #     drop_sql
    # )
    # print(drop_sql)
    # print(f"delete ck {ck_engine_dict['master']} done.")

    # delete slaves
    for con in ck_engine_dict['slaves']:
        print(con)
        try:
            con.execute(drop_sql)
        except:
            traceback.print_exc()
        print(f"delete ck {con} done.")


def convert_to_decimal(value):
    """
    The string value is converted to decimal value.
    If the conversion succeeds, decimal is returned.
    If the conversion fails, it indicates that the data is non-numeric.
    If it is empty, None is returned, else return initial value.

    :param value:
    :return:
    """
    try:
        result = Decimal(value)
        return result
    except ValueError:
        if value.strip() == '':
            return None
        else:
            return value


def convert_to_float(value):
    """
    The string value is converted to float value.
    If the conversion succeeds, float is returned.
    If the conversion fails, it indicates that the data is non-numeric.
    If it is empty, None is returned, else return initial value.

    :param value:
    :return:
    """
    try:
        result = float(value)
        return result
    except ValueError:
        if value.strip() == '':
            return None
        else:
            return value


def col_fusion_dict(etl_col_dict, pid_val_dict):
    """单列融合
    
    做出一个case when的功能，同一个target_name的字段可能存在多个pid存在
    此时，取第一次遇到非None数值的pid将数值进行填充

    Args:
        etl_col_dict (pd.DataFrame): 单个配置
        pid_val_dict (dict): pid数值字典
    """
    # 单位转换
    # for sc, unit in zip(signal_codes, units):
    return_val = None
    for pid in etl_col_dict['pids'].keys():
        # sc = pid
        unit = etl_col_dict['pids'][pid]['signal_unit']
        min_val = etl_col_dict['pids'][pid]['min_val']
        max_val = etl_col_dict['pids'][pid]['max_val']
        default_val = etl_col_dict['pids'][pid]['default_val']
        
        # 找到pid_val_dict中对应sc的数值，进行单位转换；转换完成记录待return
        if pid in pid_val_dict:
            # 判断数值范围，不符合则continue
            if (pid_val_dict[pid] >= min_val) & (pid_val_dict[pid] <= max_val):
                # 在合理范围内
                pass
            else:
                # 不在合理范围内
                return_val = float(default_val)
                continue  # 赋值默认值，不进行单位转换，循环next
            
            # 若有对应pid，则进行单位转换并记录
            if unit is None:
                unit_perision = 0
            else:
                unit_perision = convert_unit_precision(unit, etl_col_dict['target_unit'])

            if unit_perision == 0:
                # string value
                return str(pid_val_dict[pid])
            else:
                # decimal value
                try:
                    # 转换成浮点型
                    return float(pid_val_dict[pid] * unit_perision)
                except:
                    # 若转换失败则赋值空
                    return float(default_val)
        else:
            # 不存在，则进入下一个sc进行判断
            pass
    return return_val


def eval_extend_col(extend_dict: dict, fusion_val_dict: dict) -> dict:
    """
    使用eval，执行拓展信息中的公式

    :param extend_dict: 拓展配置,
        例：计算流阻的公式
        {"extend": {"type": "calculate",
        "formula": "{dpf_pressure_diff} / ({engine_exhaust_air_flow_mass_time} * (273 + {t5_temperature}) * 289 / 101325)"
        }}
    :param fusion_val_dict: pid数值字典
        例: {'dpf_pressure_diff': 123.123}
    :return: float, 拓展列的计算值
    """
    target_extend_val_dict = {}
    for target_name, extend_dict_ in extend_dict.items():
        if extend_dict_['extend']['type'] != 'calculate':
            # 拓展信息并不是计算数值
            continue

        if extend_dict_['extend']['formula'] is None:
            # 是计算数值，但是不存在formula，无法进行公式计算
            continue

        # 1. 用正则表达式匹配出extend_dict_['formula'] {} 之间的字符
        # 这些字符可对应到fusion_val_dict字典中的Key
        formula_str = extend_dict_['extend']['formula']
        pattern = r"\{(.*?)\}"

        # 使用 re.findall 匹配模式
        key_matches = re.findall(pattern, formula_str)
        print(key_matches)
        for matched_key_ in key_matches:
            if matched_key_ in fusion_val_dict:
                formula_str = formula_str.replace("{" + matched_key_ + "}", "%.3f" % fusion_val_dict[matched_key_])

        print(f"formula_str = {formula_str}")
        # 2. 匹配完成，eval输出计算数值
        try:
            calculate_formula_val = eval(formula_str)
            if (calculate_formula_val < extend_dict_['min_val']) | (calculate_formula_val > extend_dict_['max_val']):
                calculate_formula_val = extend_dict_['default_val']
        except:
            traceback.print_exc()
            calculate_formula_val = extend_dict_['default_val']

        print(f"calculate_formula_val = {calculate_formula_val}")
        target_extend_val_dict[target_name] = calculate_formula_val
    return target_extend_val_dict


def pid_fusion_map_by_dict(etl_map_dict, pid_val_dict):
    """融合

    Args:
        etl_map_dict (dict): etl 配置表
            {'<target_name>': {'<pids>': {'<pid1>': ... }, 'target_unit': 'kg'}}

        pid_val_dict (dict): pid数值字典
            例: {'SAE_J00001': 123.123}
    """
    fusion_pid_val_dict = {}
    for target_name in etl_map_dict.keys():
        if target_name in ['lng', 'lat']:
            # 经纬度等特定数据，无需转换，在fusion外进行输入
            pass
        else:
            # 非特定字段，需要转换
            fusion_val = col_fusion_dict(etl_map_dict[target_name], pid_val_dict)

            if not fusion_val is None:
                # fusion_val_tuple = fusion_val.as_tuple()
                # if len(fusion_val_tuple.digits) > 20:
                #     fusion_val = Decimal(ERROR_VAL)

                fusion_val = round(fusion_val, 6)

            # print(f"after: {target_name} fusion_val -> {fusion_val}")
            fusion_pid_val_dict[target_name] = fusion_val
    return fusion_pid_val_dict


def sdec_fusion_gc_by_dict(row, etl_map_dict: dict, extend_map_dict: dict, pid_map_dict: dict,
                           device_all_dict: dict, mode: str):
    """对一行数据进行融合

    Args:
        row (spark.Row): spark dataframe的一行
        etl_map_dict (dict): etl配置
        extend_map_dict(dict): 拓展信息配置
        pid_map_dict (dict): pid etl配置
        device_all_dict (dict): device_all数据，用于匹配hos_series与hos_mat_id
        mode (str): etl模式, stream模式 - codeM与valueM已经是list；history模式 - 从clickhouse拿到的历史数据
                                                                 是以,为连接的字符串，需要分割成List才能处理

    Returns:
        dict: 融合后的一行数据
    """
    try:
        t1 = time.time()
        ein = row['ein']
        vin = row['vin']
        upload_time = row['uploadTime1_']
        longitude = row['longitude']
        latitude = row['latitude']
        upload_date = row['uploadDate']
        
        if mode == 'history':
            # 使用正则表达式读取 [] 中的元素
            # values_result = re.findall(r'\[(.*?)\]', row['valueM'])
            # value_m_list = [x.strip() for x in values_result[0].split(',')]
            value_m_list = [float(val.strip('val ')) for val in row['valueM'].strip('[]').split(',')]

            code_m_list = re.findall(r"'([^']+)'", row['codeM'])
            # value_m_list = row['valueM'].split(',')
            # code_m_list = row['codeM'].split(',')
        elif mode == 'stream':
            value_m_list = row['valueM']
            code_m_list = row['codeM']
        else:
            print(f"mode = {mode}，etl结果直接返回None!!!!!!!!")
            return None

        # 记录pid与数值信息
        pid_val_dict = {}
        for val, pid in zip(value_m_list, code_m_list):
            # print(f"pid floating... target unit = {pid_map_dict[pid]['target_unit']}")
            if pid in pid_map_dict.keys():
                if not pid_map_dict[pid]['target_unit'] is None:
                    val = convert_to_float(val)
                pid_val_dict[pid] = val
        t2 = time.time()

        # fusion all pids
        fusion_pid_val_dict = pid_fusion_map_by_dict(etl_map_dict, pid_val_dict)
        t3 = time.time()

        # TODO: add extend columns
        extend_pid_val_dict = eval_extend_col(extend_map_dict, fusion_pid_val_dict)
        fusion_pid_val_dict.update(extend_pid_val_dict)
        
        # 添加固定字段
        fusion_pid_val_dict['ein'] = ein
        fusion_pid_val_dict['vin'] = vin
        fusion_pid_val_dict['clt_date'] = upload_date
        fusion_pid_val_dict['lng'] = float(longitude)
        fusion_pid_val_dict['lat'] = float(latitude)
        fusion_pid_val_dict['clt_timestamp'] = timestamp2unix(upload_time)
        fusion_pid_val_dict['clt_time'] = unix2datetime(timestamp2unix(upload_time)).strftime("%Y-%m-%d %H:%M:%S")
        fusion_pid_val_dict['data_type'] = row['data_type']
        fusion_pid_val_dict['data_source'] = row['data_source']

        # 添加device_all中的series与mat_id信息
        if ein in device_all_dict:
            fusion_pid_val_dict['hos_series'] = device_all_dict[ein]['hos_series']
            fusion_pid_val_dict['hos_mat_id'] = device_all_dict[ein]['hos_mat_id']
        else:
            fusion_pid_val_dict['hos_series'] = None
            fusion_pid_val_dict['hos_mat_id'] = None
        t4 = time.time()

        # if np.random.uniform(0, 100) < 1:
        #     print(f"ein -> {ein}, time -> {fusion_pid_val_dict['clt_time']}")
        
        fusion_pid_val_dict['prepare_data'] = (t2 - t1) * 1
        fusion_pid_val_dict['fusion_data'] = (t3 - t2) * 1
        fusion_pid_val_dict['add_static_data'] = (t4 - t3) * 1
        return fusion_pid_val_dict
    except:
        traceback.print_exc()
        return None


def spark_main(conf: dict):
    """
    spark任务主函数，不区分车型系列，对SDECData2M_all全量融合

    :param conf: 任务配置
    :return:
    """
    spark = SparkSession \
        .builder \
        .appName(f"etl_v3_{conf['date']}") \
        .getOrCreate()

    # 1. load etl_map_all fusion 配置信息
    etl_map_sql = snat_etl_map_loader()
    etl_map_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", etl_map_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .load()  # 数据量较小，可直接放入内存持久化

    etl_map_df = etl_map_sdf.toPandas()
    etl_map_dict, extend_map_dict, pid_map_dict = etl_map_to_dict(etl_map_df)
    print(extend_map_dict)
    # spark.stop()
    # sys.exit(1)

    # 2. load device_all
    device_all_dict = device_all_loader()

    # 3. load sdec datare
    snat_df_sql = snat_data_loader(conf)
    print(snat_df_sql)
    snat_sdf = spark.read.format('jdbc') \
        .option("url", f"jdbc:clickhouse://{ck_properties_master['host']}:{ck_properties_master['port']}/sdecdmp") \
        .option("query", snat_df_sql) \
        .option("user", ck_properties_master["user"]) \
        .option("password", ck_properties_master["password"]) \
        .option("driver", ck_properties_master["driver"]) \
        .option("batchsize", "1000000") \
        .load()

    # 字段名调整
    snat_sdf = snat_sdf.withColumnRenamed("ein1", "ein")
    snat_sdf = snat_sdf.withColumn('data_type', lit(conf['data_type']))
    snat_sdf = snat_sdf.withColumn('data_source', lit(conf['data_source']))

    # 3. 对sdf每一行进行解析融合
    partition_num = 64  # 此处分区数量与executors的数量成正比，最好是1~2倍的关系
    data_rdd = snat_sdf.rdd.repartition(partition_num).persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd = data_rdd.map(lambda row: sdec_fusion_gc_by_dict(
        row, etl_map_dict, extend_map_dict, pid_map_dict, device_all_dict, conf['mode']
    )).filter(lambda r: r != None)
    # print(rdd.take(20))

    print(f">>>>>>>>>>>>>>> {time.time()} 开始转换为dataframe >>>>>>>>>>>>>>>>>>>> ")
    fusion_sdf = spark.createDataFrame(rdd, ETL_SCHEMA)
    # time_df = fusion_sdf.select(
    #     spark_sum(fusion_sdf.prepare_data).alias("sum_prepare_data"), 
    #     spark_sum(fusion_sdf.fusion_data).alias("sum_fusion_data"),
    #     spark_sum(fusion_sdf.add_static_data).alias("sum_static_data"),
    # ).toPandas()
    # print(time_df)

    # drop_columns = ['prepare_data', 'fusion_data', 'add_static_data']
    fusion_sdf = fusion_sdf.drop('prepare_data')
    fusion_sdf = fusion_sdf.drop('fusion_data')
    fusion_sdf = fusion_sdf.drop('add_static_data')

    # 4. output to clickhouse
    url = f"jdbc:clickhouse://{ck_properties_slave['host']}:{ck_properties_slave['port']}/sdecdmp?rewriteBatchedStatements=true"
    properties = {
        "driver": ck_properties_slave['driver'],
        "socket_timeout": "3000000",
        "batchsize": "10000",
        "rewriteBatchedStatements": "true",
        "numPartitions": f"{partition_num}",
        "user": ck_properties_slave['user'],
        "password": ck_properties_slave['password'],
        "ignore": "true"
    }

    fusion_sdf.write.jdbc(
        url=url,
        table='etl_data_v3',
        mode='append',
        properties=properties
    )
    
    # fusion_sdf.write.format("csv").mode("overwrite").save(f"hdfs:///user/qjy20472/data/etl_data_v3_{conf['date']}")

    # fusion_sdf.repartition(1).write.option("header", "true").mode("overwrite").csv(
    #     f"/data/pythonProjects/pyetl/data/etl_data_v3/{conf['date']}"
    # )
    # fusion_sdf.show()
    # fusion_df = fusion_sdf.toPandas()
    # print(fusion_df)
    # print(fusion_df['dpf_flow'].describe())
    spark.stop()


def clickhouse_history(conf):
    # 删除已有数据的分区
    t1 = time.time()
    drop_ck_data(conf['date'])
    time.sleep(5)  # 等待n秒，ck删除数据存在延迟
    
    spark_main(conf)
    t2 = time.time()
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!! use time: {t2 - t1} seconds. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


if __name__ == '__main__':
    print(sys.argv)
    conf = get_params(sys.argv)
    print(conf)
    clickhouse_history(conf)
