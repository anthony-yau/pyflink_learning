#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: sliding_example1.py
@公众号: 子睿闲谈
@Description: 滑动窗口, 统计窗口时间内产生事件的条数
"""

# Happy coding
from config import *
from pyflink.table.expressions import col, lit
from pyflink.table.window import Slide

# 覆盖默认的kafka topic配置
kafka_consumer_group_id = "cluster_cpu_usage_group2"
source_topic = "cluster_cpu_usage"
sink_topic = "cluster_host_count"

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
        cnt BIGINT
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

# 定义滑动窗口，窗口大小为10分钟，滑动步长为60秒
sliding_windows = Slide.over(lit(10).minutes)\
    .every(lit(60).seconds)\
    .on(col('ts'))\
    .alias('w')

# 统计计数
results = tab\
    .window(sliding_windows)\
    .group_by(col('cluster_name'), col('w'))\
    .select(
        col('w').start.alias('start_time'),
        col('w').end.alias('end_time'),
        col('cluster_name'),
        lit(1).count.alias('cnt')
    )

# 标准输出
# results.execute().print().wait()

# 写入到kafka topic
results.execute_insert('sink').wait()
