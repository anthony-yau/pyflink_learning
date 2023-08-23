#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: session_example1.py
@公众号: 子睿闲谈
@Description: 会话窗口，设置固定间隔5分钟
"""

# Happy coding
from config import *
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session

# 覆盖默认的kafka topic配置
kafka_consumer_group_id = "cluster_cpu_usage_session_group1"
source_topic = "cluster_cpu_usage"

table_env.execute_sql(
    f"""
    CREATE TABLE source (
        cluster_name STRING,
        host_ip STRING,
        cpu_usage DOUBLE,
        ts TIMESTAMP(3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{source_topic}',
        'properties.bootstrap.servers' = '{kafka_servers}',
        'properties.group.id' = '{kafka_consumer_group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'format' = 'json'
    );
    """
)

tab = table_env.from_path("source")
#
# 会话窗口
# 使用固定间隔, 如5分钟
session_windows = Session.with_gap(lit(5).minutes)\
    .on(col('ts'))\
    .alias('w')

results = tab\
    .window(session_windows) \
    .group_by(col('cluster_name'), col('w')) \
    .select(
        col('w').start.alias('start_time'),
        col('w').end.alias('end_time'),
        col('cluster_name'),
        lit(1).count.alias('cnt')
    )

# 标准输出
results.execute().print().wait()
