#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: udf_examples1.py
@公众号: 子睿闲谈
@Description: 用户自定义函数当前支持标准(一次处理一行)和向量(一次处理一批)
"""

# Happy coding
from pyflink.table.expressions import call, col
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.udf import ScalarFunction, udf


class HashCode(ScalarFunction):
    """
    定义标量函数, 通过继承ScalarFunction, 并需要实现eval抽象方法
    eval支持可变长参数, 如*args
    """
    def __init__(self):
        self.factor = 12

    def eval(self, s):
        # 返回hash值 乘以 factor的结果
        return hash(s) * self.factor


"""
在流模式下，执行完后会下面错：
py4j.protocol.Py4JJavaError: An error occurred while calling o69.print.
: java.lang.RuntimeException: Failed to fetch next result

在批模式下，Windows下开发工具执行，SQL API使用自定义函数的没有输出，使用flink run提交没有问题
"""
env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

# 定义数据
words = env.from_elements(
    elements=[
        (1, 'Flink'),
        (2, 'hello'),
        (3, 'world'),
        (4, 'PyFlink')
    ],
    schema=['id', 'word']
)

# 注册函数
hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

# Table API中使用自定义函数
table = words.select(col('id'), col('word'), hash_code(col('id')))
table.execute().print()

# 使用SQL API输出表数据内容
# 注意: 表名需要使用格式化方法，否则会报Encountered "<EOF>"错误
env.sql_query(f"SELECT * FROM {words}").execute().print()

# SQL API中使用自定义函数
# SQL API使用的函数需要使用该方法进行注册
env.create_temporary_function("hash_code2", udf(HashCode(), result_type=DataTypes.BIGINT()))
env.sql_query(f"SELECT id, word, hash_code2(id) as hash_id FROM {words}").execute().print()
