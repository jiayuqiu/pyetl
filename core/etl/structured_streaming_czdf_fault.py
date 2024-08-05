# -*- encoding: utf-8 -*-
'''
@File    :   structured_streaming_czdf_fault.py
@Time    :   2023/05/23 21:15:11
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qjy20472@snat.com
@Desc    :   常州东风解析实时故障码
'''

# here put the import lib
import pandas as pd
import numpy as np

import json
import time
import datetime
import pandas as pd
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, struct, array, lit, expr, from_json, to_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, TimestampType, DateType
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.functions import pandas_udf
from pyspark.sql.streaming import DataStreamWriter

import sys
sys.path.append('/home/qjy20472/pythonProjects/pyetl/core/etl')
sys.path.append('/home/qjy20472/pythonProjects/pyetl')
from core.conf import mysql_engine_prod as mysql_engine

from unit import convert_unit_precision



