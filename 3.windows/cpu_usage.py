#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: Anthony
@license: Apache Licence
@file: generate_cpu_usage_data.py
@公众号: 子睿闲谈
@Description: 生成cpu利用率随机数据
"""

# Happy coding
from faker import Faker
import json
from datetime import datetime
from time import sleep
from kafka import KafkaProducer
import logging
import sys

fake = Faker()
Faker.seed(2000)

run_time = 3600
bootstrap_servers = ['localhost:9092']
topic = 'cluster_cpu_usage'
max_msg_count = 20

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")


def generate_cpu_usage(count=10):
    # 生成集群名称, 定义了默认的前缀
    cluster_name = fake.pystr(min_chars=3, max_chars=5, prefix='mysql_')
    cluster_ips = []

    for i in range(count):
        # 生成ip地址
        host_ip = fake.ipv4()
        cluster_ips.append(host_ip)

    try:
        start_time = datetime.now()

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        while True:
            now = datetime.now()

            for ip in cluster_ips:
                # 生成cpu利用率
                usage = round(abs(fake.pyfloat(2)), 2)
                msg = {
                    "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "cluster_name": cluster_name,
                    "host_ip": ip,
                    "cpu_usage": usage
                }
                logging.info(json.dumps(msg).encode('utf-8'))
                producer.send(topic, value=msg)

            # 超过最大运行时间, 终止运行
            if (now - start_time).seconds > run_time:
                break

            # 暂停时间
            sleep(1 / max_msg_count)
            # print(json.dumps(msg))
    except Exception as e:
        print(e)


generate_cpu_usage()
