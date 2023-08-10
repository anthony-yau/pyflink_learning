#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: wordcount_sql_batch.py
@公众号: 子睿闲谈
@Description: PyFlink使用SQL从文件source中读取数据，并进行单词计数
"""

# Happy coding
import os
from pyflink.table import (
    TableEnvironment, EnvironmentSettings
)


def wordcount(input_file, output_path):
    """
    :param input_file: 指定source文件(相对路径或绝对路径)
    :param output_path: 指定sink输出的目录(相对路径或绝对路径)
    :return: None
    """
    # 将相对路径转成绝对路径
    if not os.path.isabs(input_file):
        input_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), input_file)

    if not os.path.isabs(output_path):
        output_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), output_path)

    # 定义环境
    """
    使用流模式，sink使用文件时会报以下错误：
    pyflink.util.exceptions.TableException: org.apache.flink.table.api.TableException: Table sink \
    'xxx' doesn't support consuming update changes which is \
    produced by node GroupAggregate(groupBy=[word], select=[word, COUNT(*) AS cnt])
    """
    env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    env.get_config().set("parallelism.default", "1")

    # 定义source
    env.execute_sql(f"""
        CREATE TABLE words(
            id BIGINT,
            word STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{input_path}',
            'format' = 'csv'
        )
    """)

    # 定义sink: 输出为文件系统的文件
    env.execute_sql(f"""
        CREATE TABLE wordcount(
            word STRING,
            cnt BIGINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{output_path}',
            'format' = 'csv'
        )
    """)

    # 定义sink: 使用标准输出
    env.execute_sql(f"""
        CREATE TABLE wordcount_stdout(
            word STRING,
            cnt BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 使用SQL查询输出source的数据
    # env.execute_sql("select * from words").print()

    # 转换
    env.sql_query(f"""
        SELECT word, count(1) as cnt
        FROM words
        GROUP BY word
    """).execute_insert('wordcount_stdout').wait()


# Main
if __name__ == "__main__":
    wordcount('words.csv', './output3')
