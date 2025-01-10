#!/user/bin/env python3
# -*- coding: utf-8 -*-
# /************************************************************************************************************************
#  * @File Name: oracle_starrocks_compare.py
#  * @Description: 对比Oracle和StarRocks数据库中的数据条数，如果不一致，则发送企业微信告警
#  * @Author: shaorui zhang
#  * @Date: 2025-01-07
#  * @Version: 1.0
#  * 从外部获取参数，并使用argparse解析命令行参数
#  **********************************************************************************************************************/
import cx_Oracle
import pymysql
import requests  # 导入requests库用于发送HTTP请求
import argparse  # 添加argparse库用于解析命令行参数

# 添加命令行参数解析
parser = argparse.ArgumentParser(description='对比Oracle和StarRocks数据库中的数据条数')
parser.add_argument('--oracle_dsn', required=True, help='Oracle数据库DSN')
parser.add_argument('--oracle_user', required=True, help='Oracle数据库用户名')
parser.add_argument('--oracle_password', required=True, help='Oracle数据库密码')
parser.add_argument('--oracle_database', required=True, help='Oracle数据库名称')
parser.add_argument('--starrocks_host', required=True, help='StarRocks数据库主机')
parser.add_argument('--starrocks_port', type=int, required=True, help='StarRocks数据库端口')
parser.add_argument('--starrocks_user', required=True, help='StarRocks数据库用户名')
parser.add_argument('--starrocks_password', required=True, help='StarRocks数据库密码')
parser.add_argument('--starrocks_database', required=True, help='StarRocks数据库名称')
parser.add_argument('--starrocks_query', required=True, help='StarRocks查询语句')
args = parser.parse_args()

# 使用解析后的参数
oracle_dsn = cx_Oracle.makedsn(*args.oracle_dsn.split(':'))
oracle_user = args.oracle_user
oracle_password = args.oracle_password
oracle_database = args.oracle_database

starrocks_host = args.starrocks_host
starrocks_port = args.starrocks_port
starrocks_user = args.starrocks_user
starrocks_password = args.starrocks_password
starrocks_database = args.starrocks_database
starrocks_query = args.starrocks_query

# 连接到Oracle数据库
oracle_connection = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
oracle_cursor = oracle_connection.cursor()

# 连接到StarRocks数据库
starrocks_connection = pymysql.connect(host=starrocks_host, port=starrocks_port, user=starrocks_user, password=starrocks_password, database=starrocks_database)
starrocks_cursor = starrocks_connection.cursor()

# 查询StarRocks中的所有MIS_HIS_开头的表
starrocks_cursor.execute(starrocks_query)
starrocks_tables = [table[0] for table in starrocks_cursor.fetchall()]

# 初始化告警消息
alarm_messages = []

for table_name in starrocks_tables:
    # 查询Oracle中的数据条数
    oracle_query = f"SELECT COUNT(1) FROM {oracle_database}.{table_name}"
    oracle_cursor.execute(oracle_query)
    oracle_count = oracle_cursor.fetchone()[0]

    # 查询StarRocks中的数据条数
    starrocks_query = f"SELECT COUNT(1) FROM {table_name}"
    starrocks_cursor.execute(starrocks_query)
    starrocks_count = starrocks_cursor.fetchone()[0]

    # 输出结果
    print(f"Oracle表 {table_name} 的数据条数: {oracle_count}")
    print(f"StarRocks表 {table_name} 的数据条数: {starrocks_count}")

    # 比较数据条数
    if oracle_count != starrocks_count:
        alarm_message = f"Oracle表 {table_name} 和 StarRocks表 {table_name} 的数据条数不一致。\nOracle条数: {oracle_count}\nStarRocks条数: {starrocks_count}"
        alarm_messages.append(alarm_message)
        # 输出结果
        print(f"{alarm_message}")

# 发送汇总的企业微信告警
if alarm_messages:
    webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=5e6565b1-6759-4a8d-8ad2-3b2f22198338"
    message = "\n".join(alarm_messages)
    payload = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }
    response = requests.post(webhook_url, json=payload)
    print(f"告警发送结果: {response.status_code}")
else:
    print("数据全部一致")

# 关闭连接
oracle_cursor.close()
oracle_connection.close()
starrocks_cursor.close()
starrocks_connection.close()
