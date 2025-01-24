#!/user/bin/env python3
# -*- coding: utf-8 -*-
# /************************************************************************************************************************
#  * @File Name: oracle_starrocks_compare.py
#  * @Description: 对比Oracle和StarRocks数据库中的数据条数，如果不一致，则发送企业微信告警
#  * @Author: shaorui zhang
#  * @Date: 2025-01-07
#  * @Version: 1.0
#  **********************************************************************************************************************/
import cx_Oracle
import pymysql
import requests  # 导入requests库用于发送HTTP请求
import datetime

# Oracle数据库连接配置
oracle_dsn = cx_Oracle.makedsn('xxxx', '1521', service_name='xxxx')
oracle_user = 'xxxx'
oracle_password = 'xxxx'
oracle_database = 'xxxx'

# StarRocks数据库连接配置
starrocks_host = 'xxxx'
starrocks_port = 9030  # 默认端口
starrocks_user = 'xxxx'
starrocks_password = 'xxxx'
starrocks_database = 'xxxx'

# 连接到Oracle数据库
oracle_connection = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
oracle_cursor = oracle_connection.cursor()

# 连接到StarRocks数据库
starrocks_connection = pymysql.connect(host=starrocks_host, port=starrocks_port, user=starrocks_user, password=starrocks_password, database=starrocks_database)
starrocks_cursor = starrocks_connection.cursor()

# 查询StarRocks中的所有MIS_HIS_开头的表
# starrocks_query = "SHOW TABLES LIKE 'MIS_HIS_R_GZ_MJ%'"
# starrocks_cursor.execute(starrocks_query)
# starrocks_tables = [table[0] for table in starrocks_cursor.fetchall()]

# 获取20241201之后的日期范围

# 用于存储不一致的告警信息
alert_messages = []

# 记录任务开始时间
start_time = datetime.datetime.now()

# 查询Oracle中的数据条数，按照STORECODE进行对比
oracle_query = f"SELECT STORECODE, COUNT(1) FROM {oracle_database}.UF_DATA_XS GROUP BY STORECODE"
oracle_cursor.execute(oracle_query)
oracle_counts = {row[0]: row[1] for row in oracle_cursor.fetchall()}

# 查询StarRocks中的数据条数，按照STORECODE进行对比
starrocks_query = f"SELECT STORECODE, COUNT(1) FROM UF_DATA_XS GROUP BY STORECODE"
starrocks_cursor.execute(starrocks_query)
starrocks_counts = {row[0]: row[1] for row in starrocks_cursor.fetchall()}

# 初始化不一致条数的统计变量
total_mismatch_count = 0

# 比较数据条数
for storecode in set(oracle_counts.keys()).union(set(starrocks_counts.keys())):
    oracle_count = oracle_counts.get(storecode, 0)
    starrocks_count = starrocks_counts.get(storecode, 0)
    if oracle_count != starrocks_count:
        alert_message = f"Oracle表 UF_DATA_XS 和 StarRocks表 UF_DATA_XS 的数据条数在STORECODE: {storecode} 处不一致。\nOracle条数: {oracle_count}\nStarRocks条数: {starrocks_count}"
        alert_messages.append(alert_message)
        print(f"{alert_message}")
        # 统计不一致的条数
        total_mismatch_count += abs(oracle_count - starrocks_count)

# 检查是否有告警信息需要发送
# if alert_messages:
#     webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=5e6565b1-6759-4a8d-8ad2-3b2f22198338"
#     combined_message = "\n".join(alert_messages)
#     payload = {
#         "msgtype": "text",
#         "text": {
#             "content": combined_message
#         }
#     }
#     response = requests.post(webhook_url, json=payload)
#     print(f"告警发送结果: {response.status_code}")
# else:
#     print("数据全部一致")

# 输出不一致的条数统计
print(f"总不一致条数: {total_mismatch_count}")

# 记录任务结束时间
end_time = datetime.datetime.now()

# 计算任务执行时间
execution_time = end_time - start_time
print(f"任务执行时间: {execution_time}")

# 关闭连接
oracle_cursor.close()
oracle_connection.close()
starrocks_cursor.close()
starrocks_connection.close()
