#!/user/bin/env python3
# -*- coding: utf-8 -*-
# /************************************************************************************************************************
#  * @File Name: look_storecode_ip.py
#  * @Description: 查找指定店铺所在的的SQL Server服务器
#  * @Author: shaorui zhang
#  * @Date: 2025-01-16
#  * @Version: 1.0
#  **********************************************************************************************************************/
import os
import pyodbc
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 从环境变量中读取敏感信息
username = os.getenv('DB_USERNAME', 'xxxx')
password = os.getenv('DB_PASSWORD', 'xxxx')

server_list = [
    '10.1.0.125', '10.1.0.126', '10.1.0.130', '10.1.0.131', '10.1.0.132', '10.1.0.134',
    '10.1.2.121', '10.1.2.122', '10.1.2.124', '10.1.2.127', '10.1.2.128', '10.1.2.135', '10.1.2.23',
    '10.1.2.26', '10.1.2.28', '10.1.2.29', '10.1.2.30', '10.1.2.33', '10.1.2.38',
    '10.1.2.40', '10.1.2.41', '10.1.2.42', '10.1.2.43', '10.1.2.44', '10.1.2.45',
    '10.1.2.46', '10.1.2.47', '10.1.2.53', '10.1.2.68', '10.1.2.69'
]

database = 'RPT'

def check_server(server, database_name):
    try:
        conn_str = (
            f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};'
            f'PWD={password};Encrypt=True;TrustServerCertificate=True;'  # 添加加密和信任服务器证书参数
        )
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name FROM sys.databases WHERE name LIKE ?", (f'%{database_name}%',))
                result = cursor.fetchone()
                if result:
                    logging.info(f"Found database '{database_name}' on server: {server}")
                # else:
                    # logging.info(f"No matching database found on server: {server}")
    except pyodbc.Error as e:
        logging.error(f"Failed to connect to server {server}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error on server {server}: {e}")

def main():
    database_name = '0005'  # 这里可以修改为其他数据库名称
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(check_server, server, database_name) for server in server_list]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    main()
