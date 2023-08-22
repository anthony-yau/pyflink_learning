#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: config.py.py
@公众号: 子睿闲谈
@Description: 公用kafka连接配置信息
"""

from pyflink.table import TableEnvironment, EnvironmentSettings
import os

# 指定kafka的基础信息
kafka_servers = "localhost:9092"
kafka_consumer_group_id = "rank_group1"
source_topic = "user_action"
sink_topic = "click_rank"
sex_count = "sex_count"
sex_topn = "sex_topn"

# 设置env
table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# 加载Jar
flink_sql_kafka_jar = os.path.join(os.path.abspath('/mnt/f/venv/flink/jars/'), 'flink-sql-connector-kafka-1.16.2.jar')
table_env.get_config().set('pipeline.jars', 'file://' + flink_sql_kafka_jar)
