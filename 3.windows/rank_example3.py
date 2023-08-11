#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: rank_example2.py
@公众号: 子睿闲谈
@Description: 使用Flink滑动窗口，自定义Java函数实现每1秒钟统计过去1分钟内点击量最高的10个男性和10个女性用户
"""

# Happy coding
import os
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes, Row
from pyflink.table.expressions import col, lit
from pyflink.table.udf import AggregateFunction, udaf
import json
import logging
import sys

# 指定kafka的基础信息
kafka_servers = "localhost:9092"
kafka_consumer_group_id = "rank_group1"
source_topic = "user_action"
sink_topic = "click_rank3"

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

# 设置env
table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# 加载Jar
flink_sql_kafka_jar = os.path.join(os.path.abspath('/mnt/f/venv/flink/jars/'), 'flink-sql-connector-kafka-1.16.2.jar')
table_env.get_config().set('pipeline.jars', f'file://{flink_sql_kafka_jar}')


# 自定义聚合函数
class TopN(AggregateFunction):
    """
    将前N个排名的返回
    """
    # 初始化累加器, 使用行对象作为累加器
    def create_accumulator(self):
        return {"names": {}, "top_n": 0}

    # 根据输入更新 累加器的逻辑
    def accumulate(self, accumulator, name, count):
        if name in accumulator["names"]:
            accumulator["names"][name] += 1
        else:
            accumulator["names"][name] = 1
        accumulator["top_n"] = count

    # 如何返回累加器中存储的中间结果，作为UDAF的最终结果
    def get_value(self, accumulator):
        names = accumulator["names"]
        top_n = accumulator["top_n"]
        logging.info(f"accumulator: {accumulator}")

        sorted_names = sorted(names.items(), key=lambda x: x[1], reverse=True)
        top_n_names = sorted_names[:top_n]
        return json.dumps(dict(top_n_names))

    # 使用窗口聚合需要实现该方法
    def merge(self, accumulator, accumulators):
        logging.info(f"merge accumulators: {accumulators}")
        for acc in accumulators:
            for name, cnt in acc['names'].items():
                self.accumulate(accumulator, name, cnt)

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [
                DataTypes.FIELD("names", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                DataTypes.FIELD("top_n", DataTypes.INT())
            ]
        )


# 注册函数
TopN = udaf(TopN())
table_env.create_temporary_function("TopN", TopN)

"""
创建source
使用kafka sql连接器从kafka topic中消费数据
ts为事件事件属性，并且用延迟5s的策略来生成watermark
"""
table_env.execute_sql(
    f"""
    CREATE TABLE source (
        name STRING,
        sex STRING,
        action STRING,
        is_delete BIGINT,
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
    )
    """
)

# 定义Sink
table_env.execute_sql(
    f"""
    CREATE TABLE sink(
        sex STRING,
        top10 STRING,
        start_time TIMESTAMP(3),
        end_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{sink_topic}',
        'properties.bootstrap.servers' = '{kafka_servers}',
        'properties.group.id' = '{kafka_consumer_group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'format' = 'json'
    )
    """
)

# 执行聚合
table_env.sql_query(
    """
    SELECT
        sex,
        TopN(name, 10) AS top10,
        HOP_START(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND) AS start_time,
        HOP_END(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND) AS end_time
    FROM
        source
    WHERE
        action = 'click'
        AND is_delete = 0
    GROUP BY
        sex,
        HOP(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND)
    """
).execute_insert("sink")


