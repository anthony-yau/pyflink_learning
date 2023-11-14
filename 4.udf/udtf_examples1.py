#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: udtf_examples2.py
@公众号: 子睿闲谈
@Description: 表值函数（TableFunction）以零个、一个或多个列作为输入参数，可以返回任意数量的行来作为输出，返回类型可以为可迭代子类、迭代器、生成器。
"""

# Happy coding
from pyflink.table import DataTypes
from pyflink.table.udf import udtf
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import json


# 表值函数
@udtf(input_types=DataTypes.STRING(), result_types=[
    DataTypes.STRING(),
    DataTypes.INT(),
    DataTypes.INT(),
    DataTypes.INT(),
    DataTypes.STRING(),
    DataTypes.STRING()
])
def split_dict(input_map):
    print("input:", input_map)
    if input_map:
        for line in json.loads(input_map):
            yield line['bs3_name'], int(line['bs3_name_id']), int(line['bs1_name_id']), \
                  int(line['bs2_name_id']), line['bs2_name'], line['bs1_name']
    else:
        return None


# 设置流执行环境
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# 注册split_dict UDF
t_env.create_temporary_function("split_dict", split_dict)

# 创建输入表
t_env.execute_sql("""
    create table input_table(
    id string,
    bs_info string)
    with (
    'connector'='filesystem',
    'path'='file:///tmp/data1.csv',
    'format'='csv');
""")

# 创建输出表（这里使用打印输出）
t_env.execute_sql("""
    CREATE TEMPORARY TABLE output_table (
        id string,
        bs3_name STRING,
        bs3_name_id INT,
        bs1_name_id INT,
        bs2_name_id INT,
        bs2_name STRING,
        bs1_name STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# 使用split_dict UDF将输入表中的数据拆分为多行和多列，并将结果插入输出表
# 使用cross join, 如果UDTF输出为空, 将不会显示
t_env.execute_sql("""
    INSERT INTO output_table
    SELECT
        id,
        bs3_name,
        bs3_name_id,
        bs1_name_id,
        bs2_name_id,
        bs2_name,
        bs1_name
    FROM input_table,
    LATERAL TABLE(split_dict(bs_info)) AS T(bs3_name,bs3_name_id,bs1_name_id,bs2_name_id,bs2_name,bs1_name)
""").wait()

# 使用left join, 如果UDTF输出为空，将使用null填充
t_env.execute_sql("""
    SELECT
        id,
        bs3_name,
        bs3_name_id,
        bs1_name_id,
        bs2_name_id,
        bs2_name,
        bs1_name
    FROM input_table LEFT JOIN
    LATERAL TABLE(split_dict(bs_info)) AS T(bs3_name,bs3_name_id,bs1_name_id,bs2_name_id,bs2_name,bs1_name) ON TRUE
""").print()