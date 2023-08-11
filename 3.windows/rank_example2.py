#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: rank_example2.py
@公众号: 子睿闲谈
@Description: 使用自定义表聚合函数实现TopN
"""

# Happy coding
import os
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes, Row
from pyflink.table.expressions import col
from pyflink.table.udf import udtaf, TableAggregateFunction


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


# 自定义聚合函数
class TopN(TableAggregateFunction):
    # 初始化累加器
    def create_accumulator(self):
        return {"names": {}, "top_n": 0}

    # 实现逻辑, row为输入
    def accumulate(self, accumulator, name, count):
        if name in accumulator["names"]:
            accumulator["names"][name] += 1
        else:
            accumulator["names"][name] = 1
        accumulator["top_n"] = count

    # 最终返回结果
    def emit_value(self, accumulator):
        names = accumulator["names"]
        top_n = accumulator["top_n"]

        sorted_names = sorted(names.items(), key=lambda x: x[1], reverse=True)
        top_n_names = sorted_names[:top_n]
        top_n_name_cnt = [{"name": name, "cnt": cnt} for name, cnt in top_n_names]
        for n in top_n_name_cnt:
            yield Row(n['name'], n['cnt'])

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [
                DataTypes.FIELD("names", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                DataTypes.FIELD("top_n", DataTypes.INT())
            ]
        )

    def get_result_type(self) -> DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT())]):
        return DataTypes.ROW(
            [
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("cnt", DataTypes.BIGINT())
            ]
        )


TopN = udtaf(TopN())

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

# 使用自定义聚合函数
# 表聚合函数不支持窗口，只支持GroupBy
# https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/python/table/udfs/python_udfs/

# 没有使用窗口时，下面语句可以使用python执行, 使用flink run提交到集群报错：
# Caused by: org.apache.flink.streaming.runtime.tasks.StreamTaskException: \
# Cannot load user class: org.apache.flink.table.runtime.operators.python.\
# aggregate.PythonStreamGroupTableAggregateOperator
# 需要复制opt/flink-python包到lib目录
tab = table_env.from_path("source")
results = tab \
    .group_by(col('sex'))\
    .flat_aggregate((col('name'), 10))\
    .select(col('*'))\
    .execute().print().wait()
