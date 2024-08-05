# -*- coding: utf-8 -*-
# @Time    : 2023/3/16 下午3:57
# @Author  : qiujiayu
# @File    : conf.py
# @Software: PyCharm 
# @Comment : 项目配置


from sqlalchemy import create_engine


ck_properties_master = {  # clickhouse 数据库配置
    # "driver": "com.github.housepower.jdbc.ClickHouseDriver",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    # "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "socket_timeout": "3600000",
    "rewriteBatchedStatements": "true",
    "batchsize": "1000000",
    "numPartitions": "8",
    "user": "default",
    "password": "5NsWWQeJ",
    "host": "10.129.165.72",
    "database": 'sdecdmp',
    "port": 8123
}

ck_properties_slave_1 = {  # clickhouse 数据库配置
    # "driver": "com.github.housepower.jdbc.ClickHouseDriver",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    "socket_timeout": "3600000",
    "rewriteBatchedStatements": "true",
    "batchsize": "1000000",
    "numPartitions": "8",
    "user": "default",
    "password": "5NsWWQeJ",
    "host": "10.129.165.73",
    "database": 'sdecdmp',
    "port": 8123
}

ck_properties_slave_2 = {  # clickhouse 数据库配置
    # "driver": "com.github.housepower.jdbc.ClickHouseDriver",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    "socket_timeout": "3600000",
    "rewriteBatchedStatements": "true",
    "batchsize": "1000000",
    "numPartitions": "8",
    "user": "default",
    "password": "5NsWWQeJ",
    "host": "10.129.165.74",
    "database": 'sdecdmp',
    "port": 8123
}

ck_properties_slave_3 = {  # clickhouse 数据库配置
    # "driver": "com.github.housepower.jdbc.ClickHouseDriver",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    "socket_timeout": "3600000",
    "rewriteBatchedStatements": "true",
    "batchsize": "1000000",
    "numPartitions": "8",
    "user": "default",
    "password": "5NsWWQeJ",
    "host": "10.129.165.71",
    "database": 'sdecdmp',
    "port": 8123
}

ck_properties_slave_4 = {  # clickhouse 数据库配置
    # "driver": "com.github.housepower.jdbc.ClickHouseDriver",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    "socket_timeout": "3600000",
    "rewriteBatchedStatements": "true",
    "batchsize": "1000000",
    "numPartitions": "8",
    "user": "default",
    "password": "5NsWWQeJ",
    "host": "10.129.165.72",
    "database": 'sdecdmp',
    "port": 8123
}


mysql_properties_prod = {  # mysql 生产环境配置
    "host": "10.129.41.147",
    "port": "4918",
    "url": "jdbc:mysql://10.129.41.147:4918/ck",
    "user": "dmp_sys_guest",
    "password": "fTe^3hy*dO_363",
    "driver": "com.mysql.cj.jdbc.Driver",
    "database": "ck"
}

mysql_properties_dev = {  # mysql 开发环境配置
    "host": "10.179.98.13",
    "port": "3306",
    "url": "jdbc:mysql://10.179.98.13:3306/ck",
    "user": "root",
    "password": "DMP_test135#",
    "driver": "com.mysql.cj.jdbc.Driver",
    "database": "ck"
}

doris_properties = {
    "host": "10.66.58.71",
    "port": "9030",
    "url": "jdbc:mysql://10.66.58.71:9030/qiujiayu",
    "user": "qiujiayu",
    "password": "qiujiayu",
    "driver": "com.mysql.cj.jdbc.Driver",
    "database": "qiujiayu"
}

ck_engine_master = create_engine(
    f"clickhouse://{ck_properties_master['user']}:{ck_properties_master['password']}@{ck_properties_master['host']}:{ck_properties_master['port']}/{ck_properties_master['database']}"
)

ck_engine_slave_1 = create_engine(
    f"clickhouse://{ck_properties_slave_1['user']}:{ck_properties_slave_1['password']}@{ck_properties_slave_1['host']}:{ck_properties_slave_1['port']}/{ck_properties_slave_1['database']}"
)

ck_engine_slave_2 = create_engine(
    f"clickhouse://{ck_properties_slave_2['user']}:{ck_properties_slave_2['password']}@{ck_properties_slave_2['host']}:{ck_properties_slave_2['port']}/{ck_properties_slave_2['database']}"
)

ck_engine_slave_3 = create_engine(
    f"clickhouse://{ck_properties_slave_3['user']}:{ck_properties_slave_3['password']}@{ck_properties_slave_3['host']}:{ck_properties_slave_3['port']}/{ck_properties_slave_3['database']}"
)

ck_engine_slave_4 = create_engine(
    f"clickhouse://{ck_properties_slave_4['user']}:{ck_properties_slave_4['password']}@{ck_properties_slave_4['host']}:{ck_properties_slave_4['port']}/{ck_properties_slave_4['database']}"
)

ck_engine_dict = {
    'master': ck_engine_master,
    'slaves': [ck_engine_slave_1, ck_engine_slave_2, ck_engine_slave_4]
}

mysql_engine_prod = create_engine(
    f"mysql+pymysql://{mysql_properties_prod['user']}:{mysql_properties_prod['password']}" +
    f"@{mysql_properties_prod['host']}:{mysql_properties_prod['port']}/{mysql_properties_prod['database']}?charset=utf8"
)

mysql_engine_dev = create_engine(
    f"mysql+pymysql://{mysql_properties_dev['user']}:{mysql_properties_dev['password']}" +
    f"@{mysql_properties_dev['host']}:{mysql_properties_dev['port']}/{mysql_properties_dev['database']}?charset=utf8"
)

doris_engine = create_engine(
    f"mysql+pymysql://{doris_properties['user']}:{doris_properties['password']}" +
    f"@{doris_properties['host']}:{doris_properties['port']}/{doris_properties['database']}?charset=utf8&autocommit=false"
)
