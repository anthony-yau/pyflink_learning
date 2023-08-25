#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: jdbc_example1.py
@公众号: 子睿闲谈
@Description: 使用JDBC读写MySQL表的数据
"""

# Happy coding
from pyflink.table import (
    TableEnvironment,
    EnvironmentSettings
)

"""
相关文档：
JDBC: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/connectors/table/jdbc/
"""

env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

env.get_config().set(
    "pipeline.classpaths",
    "file:///mnt/f/venv/flink/jars/flink-connector-jdbc-1.16.2.jar;\
    file:///mnt/f/venv/flink/jars/mysql-connector-j-8.1.0.jar",
)

"""
定义表结构的字段类型要对应, 如MySQL表定义的是INT，而这里定义为BIGINT
将会报错：Caused by: java.lang.ClassCastException: class java.lang.Integer
cannot be cast to class java.lang.Long (java.lang.Integer and java.lang.Long
are in module java.base of loader 'bootstrap')
"""
env.execute_sql(
    """
    CREATE TABLE words(
        id INT,
        word STRING,
        PRIMARY KEY(id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://127.0.0.1:8033/flink',
        'table-name' = 'words',
        'username' = 'flink',
        'password' = 'xxx'
    )
    """
)

# 使用标准输出source的数据
# env.execute_sql("select * from words").print()

# 定义Sink
env.execute_sql(
    """
    CREATE TABLE wordcount(
        word STRING,
        cnt BIGINT,
        PRIMARY KEY(word) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://127.0.0.1:8033/flink',
        'table-name' = 'wordcount',
        'username' = 'flink',
        'password' = 'xxx'
    )
    """
)

# 查询语句输出的cnt为BIGINT
env.sql_query(
    """
    SELECT word, count(*) as cnt from words GROUP BY word
    """
).execute_insert('wordcount').wait()
