#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: tumbling_example1.py
@公众号: 子睿闲谈
@Description: 滚动窗口，统计最大、最小、平均值
"""

# Happy coding
from config import *
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

# 覆盖默认的kafka topic配置
kafka_consumer_group_id = "cluster_cpu_usage_group0"
source_topic = "cluster_cpu_usage"
sink_topic = "cluster_max_cpu_usage"

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

table_env.execute_sql(
    f"""
    CREATE TABLE sink (
        start_time TIMESTAMP(3),
        end_time TIMESTAMP(3),
        cluster_name STRING,
        max_cpu_usage DOUBLE,
        min_cpu_usage DOUBLE,
        avg_cpu_usage DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{sink_topic}',
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
# 滚动窗口
# 定义窗口大小10秒
tumbling_windows = Tumble.over(lit(10).seconds)\
    .on(col('ts'))\
    .alias('w')

# 统计时间窗口范围内的最大、最小、平均值
results = tab.window(tumbling_windows)\
    .group_by(col('cluster_name'), col('w'))\
    .select(
        col('w').start.alias('start_time'),
        col('w').end.alias('end_time'),
        col('cluster_name'),
        col('cpu_usage').max.alias('max_cpu_usage'),
        col('cpu_usage').min.alias('min_cpu_usage'),
        col('cpu_usage').avg.alias('avg_cpu_usage')
    )

# 标准输出
# results.execute("cluster_cpu_usage").print().wait()

# 将结果集写入到kafka topic中
results.execute_insert("sink").wait()

