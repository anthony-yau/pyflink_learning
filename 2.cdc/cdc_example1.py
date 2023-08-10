#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: cdc_example1.py
@公众号: 子睿闲谈
@Description: 从MySQL中实时捕获变更，cdc项目：https://github.com/ververica/flink-cdc-connectors
"""

# Happy coding
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.datastream import CheckpointConfig, CheckpointingMode

env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

env.get_config().set(
    "pipeline.classpaths",
    "file:///mnt/f/venv/flink/jars/flink-connector-jdbc-1.16.2.jar;\
    file:///mnt/f/venv/flink/jars/mysql-connector-j-8.1.0.jar;\
    file:///mnt/f/venv/flink/jars/flink-sql-connector-mysql-cdc-2.4.1.jar",
)


# source
env.execute_sql(
    """
    CREATE TABLE words(
        id INT,
        word STRING,
        PRIMARY KEY(id) NOT ENFORCED
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname' = '127.0.0.1',
        'port' = '8033',
        'database-name' = 'flink',
        'table-name' = 'words',
        'username' = 'flink',
        'password' = 'xxx'
    )
    """
)

# sink
env.execute_sql(
    """
    CREATE TABLE words_cdc(
        id INT,
        word STRING,
        PRIMARY KEY(id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://127.0.0.1:22334/test',
        'table-name' = 'words_cdc',
        'username' = 'flink_cdc',
        'password' = 'xxx'
    )
    """
)

# 启动检查点, 否则不会启动Dump线程连接MySQL获取binlog日志，最小10ms
env.get_config().set('execution.checkpointing.interval', '10')

# 获取到数据后同步到其他存储
env.sql_query(
    """
    SELECT id, word from words
    """
).execute_insert('words_cdc').wait()


# Main
if __name__ == "__main__":
    run_code = 0
