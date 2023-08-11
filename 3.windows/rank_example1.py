#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: rank_example1.py
@公众号: 子睿闲谈
@Description: 使用Flink滑动窗口，每1秒钟统计过去1分钟内点击量最高的10个男性和10个女性用户
"""

# Happy coding
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# 指定kafka的基础信息
kafka_servers = "localhost:9092"
kafka_consumer_group_id = "rank_group1"
source_topic = "user_action"
sink_topic = "click_rank"
sex_count = "sex_count"
sex_topn = "sex_topn"

# 设置env
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# 加载Jar
flink_sql_kafka_jar = os.path.join(os.path.abspath('/mnt/f/venv/flink/jars/'), 'flink-sql-connector-kafka-1.16.2.jar')
table_env.get_config().set('pipeline.jars', 'file://' + flink_sql_kafka_jar)

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

# 定义Sink, 存储使用自定义函数的结果
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

# 定义sink, 存储使用内置count函数的结果
table_env.execute_sql(
    f"""
    CREATE TABLE sex_count(
        sex STRING,
        cnt BIGINT,
        start_time TIMESTAMP(3),
        end_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{sex_count}',
        'properties.bootstrap.servers' = '{kafka_servers}',
        'properties.group.id' = '{kafka_consumer_group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'format' = 'json'
    )
    """
)

# 定义sink, 存储使用内置TopN函数的结果
table_env.execute_sql(
    f"""
    CREATE TABLE sex_topn(
        start_time TIMESTAMP(3),
        end_time TIMESTAMP(3),
        sex STRING,
        name STRING,
        cnt BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{sex_topn}',
        'properties.bootstrap.servers' = '{kafka_servers}',
        'properties.group.id' = '{kafka_consumer_group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'format' = 'json'
    )
    """
)

"""
使用了窗口函数的该示例脚本使用python直接执行没有数据输出，提交到flink集群后正常
"""
# 流处理
# 使用内置的count聚合函数
# HOP窗口也可以使用下面语句
"""
SELECT
    sex,
    count(*) as cnt,
    window_start AS start_time,
    window_end AS end_time
FROM
    TABLE(HOP(TABLE source, DESCRIPTOR(ts), INTERVAL '1' SECOND, INTERVAL '60' SECOND))
GROUP BY
    sex,
    window_start,
	window_end;
"""
table_env.sql_query(
    """
    SELECT
        sex,
        count(*) as cnt,
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
).execute_insert('sex_count')


# 使用内置TopN窗口函数
# https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/table/sql/queries/window-topn/
# 如果将时间格式化，可以使用函数DATE_FORMAT(start_time, 'hh:mm:ss') start_time , 格式为：yyyy-MM-dd hh:mm:ss
table_env.sql_query(
    """
    SELECT
        start_time,
        end_time,
        sex,
        name,
        cnt
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY start_time, end_time, sex
                ORDER BY cnt desc) AS rownum
        FROM
            (
            SELECT 
                window_start AS start_time,
                window_end AS end_time,
                sex,
                name,
                count(*) cnt
            FROM 
                TABLE(HOP(TABLE source, DESCRIPTOR(ts), INTERVAL '1' SECOND, INTERVAL '60' SECOND))
            WHERE
                action = 'click'
                AND is_delete = 0
            GROUP BY
                window_start,
                window_end,
                sex,
                name
            )
    )
    WHERE rownum <= 5  
    """
).execute_insert('sex_topn')

# 使用Table API
# 创建长度为60s，滑动步长为1s的滑动窗口
# slide_windows = Slide.over('60.seconds').every('1.seconds').on('ts').alias('w')
#
# table_env.from_path('source')\
#     .filter("action='click'")\
#     .filter("is_delete=1")\
#     .window(slide_windows)\
#     .group_by('w,sex')\
#     .select("sex,count(*) as cnt,w.start AS start_time,w.end AS end_time") \
#     .execute().print()

