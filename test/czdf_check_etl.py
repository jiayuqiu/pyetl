# -*- encoding: utf-8 -*-
'''
@File    :   czdf_check_etl.py
@Time    :   2023/07/12 20:16:12
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   检查常州东风etl结果
'''

# here put the import lib
import pandas as pd
import numpy as np
from sqlalchemy import text


import sys
# sys.path.append('/data1/pythonProjects/pyetl/core/etl')
sys.path.append('/data1/pythonProjects/pyetl')
from core.conf import ck_engine_master


date = '2023-07-10'


fuel_level_etl_df = pd.read_sql(
    sql=text(
        f"""
        select
            fuel_level
        from
            sdecdmp.etl_data_all
        where
            clt_date = '{date}' and data_source = 'czdf'
        """
    ),
    con=ck_engine_master
)
print(max(fuel_level_etl_df['fuel_level']))

sc = 'SAE_S00096'
fuel_level_nosql_df = pd.read_sql(
    sql=text(
        f"""
        select
                case
                    when indexOf(code,'{sc}')>0 then parse_value[indexOf(code,'{sc}')]
                end as fuel_level
        from
            sdecdmp.signal_data_nosql_all
        where
            clt_date = '{date}' and data_source = 'czdf'
        """
    ),
    con=ck_engine_master
)
fuel_level_nosql_df.loc[:, 'fuel_level'] = fuel_level_nosql_df['fuel_level'].astype(float)
print(max(fuel_level_nosql_df['fuel_level']))
