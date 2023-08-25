#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: cdc_example2.py
@公众号: 子睿闲谈
@Description: 使用Flink CDC，以及checkpoint进行状态持久化
"""

# Happy coding
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# 设置env
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
env = StreamTableEnvironment.create(None, environment_settings=env_settings)

# 开启checkpoint(为意外失败的作业提供恢复机制)
# 设置 checkpoint 时间间隔，最小为10ms，可以通过单位来指定时间如60s
env.get_config().set('execution.checkpointing.interval', '10s')
# 设置 checkpoint 模式, EXACTLY_ONCE精确一次
env.get_config().set('execution.checkpointing.mode', 'EXACTLY_ONCE')
# 设置 checkpoint 之间的最小间隔
env.get_config().set('execution.checkpointing.min-pause', '100ms')
# 设置 checkpoint 超时时间, 如10分钟
env.get_config().set('execution.checkpointing.timeout', '10min')
# 设置等待未处理检查点的最大数量
env.get_config().set('execution.checkpointing.tolerable-failed-checkpoints', '3')

# checkpoint存储, 默认状态保存在TaskManager的内存中
# 状态后端支持HashMapStateBackend(默认)、EmbeddedRocksDBStateBackend
# 指定后端状态存储类名
state_backend = "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory"
# 指定存储目录
checkpoint_dir = "file:///tmp/checkpoints"

env.get_config().set("state.backend", state_backend)
env.get_config().set("state.checkpoints.dir", checkpoint_dir)
# 为状态后端开启增量 checkpoint 支持（仅当使用 RocksDBStateBackend 时有效）
env.get_config().set("state.backend.incremental", "true")

env.get_config().set(
    "pipeline.classpaths",
    "file:///mnt/f/venv/flink/jars/flink-connector-jdbc-1.16.2.jar;\
    file:///mnt/f/venv/flink/jars/mysql-connector-j-8.1.0.jar;\
    file:///mnt/f/venv/flink/jars/flink-sql-connector-mysql-cdc-2.4.1.jar",
)

# 指定savepoint(用于可移植性和操作灵活性，如升级Flink版本时，进行手动备份和恢复)存储配置
savepoint_dir = "file:///tmp/savepoints"
env.get_config().set("state.savepoints.dir", savepoint_dir)

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

# 获取到数据后同步到其他存储
env.sql_query(
    """
    SELECT id, word from words
    """
).execute_insert('words_cdc').wait()
