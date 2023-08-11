#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: kafka_producer.py
@公众号: 子睿闲谈
@Description: 生成消息，并写入到Kafka的Topic中
"""

# Happy coding
import random
import numpy as np
from json import dumps
from time import sleep
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer


# 随机生成数据
# 设置随机数种子, 保证每次运行的结果都一样
seed = 2020
# 50个用户
num_users = 50
# 每秒钟的最大消息数
max_msg_per_second = 20
# 脚本最长运行时间, 避免将kafka写满
run_seconds = 3600
# topic名称
topic = 'user_action'
# kafka bootstrap server
bootstrap_servers = ['localhost:9092']

fake = Faker(locale='zh_CN')
Faker.seed(seed)
random.seed(seed)


class UserGroup:
    def __init__(self):
        """
        为指定数量的用户分配不同的出现概率，每次按概率分布获取用户姓名
        """
        self.users = [self.gen_male() if random.random() < 0.6 else self.gen_female() for _ in range(num_users)]
        # 用户点击率的累加
        # numpy.random.uniform[low, high, size] 从一个均匀分布[low, high]中随机采样，左闭右开的值
        # numpy.cumsum进行累加
        prob = np.cumsum(np.random.uniform(1, 100, num_users))
        # 压缩到 0 ~ 1
        self.prob = prob / prob.max()

    @staticmethod
    def gen_male():
        """
        生成男人
        """
        return {'name': fake.name_male(), 'sex': '男'}

    @staticmethod
    def gen_female():
        """
        生成女人
        """
        return {'name': fake.name_female(), 'sex': '女'}

    def get_user(self):
        """
        随机获得用户
        """
        # 生成0~1的随机数
        r = random.random()
        # searchsorted 在数组中插入数组r, 返回下标
        index = np.searchsorted(self.prob, r)
        return self.users[index]


def write_data():
    group = UserGroup()
    start_time = datetime.now()

    # 初始化生产者
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    while True:
        now = datetime.now()

        # 生成数据, 发送到kafka
        user = group.get_user()
        # action为用户操作, is_delete指定10%的概率丢弃这条数据
        # 时间格式化为"%Y-%m-%d %H:%M:%S", 才能进行窗口聚合, 否则时间不符合规范
        cur_data = {
            "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
            "name": user['name'],
            "sex": user['sex'],
            "action": 'click' if random.random() < 0.9 else 'scroll',
            "is_delete": 0 if random.random() < 0.9 else 1
        }

        producer.send(topic, value=cur_data)

        # 超过最大运行时间, 终止运行
        if (now - start_time).seconds > run_seconds:
            break

        # 暂停时间
        sleep(1/max_msg_per_second)


# Main
if __name__ == "__main__":
    write_data()
