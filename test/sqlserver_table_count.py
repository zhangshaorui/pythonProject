#!/user/bin/env python3
# -*- coding: utf-8 -*-
# /************************************************************************************************************************
#  * @File Name: sqlserver_table_count.py
#  * @Description: 获取SQL Server数据库中指定表的数据条数，并输出到控制台
#  * @Author: shaorui zhang
#  * @Date: 2025-01-21
#  * @Version: 1.0
#  **********************************************************************************************************************/
import pyodbc

# 数据库连接信息
server = 'xxxx'
database = 'xxxx'
username = 'xxxx'
password = 'xxxx'

# 连接到SQL Server
conn_str = f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 获取所有数据库名称
cursor.execute("SELECT name FROM sys.databases WHERE state = 0 AND name NOT IN ('master', 'tempdb', 'model', 'msdb')")
databases = cursor.fetchall()

# 添加统计所有count的和
total_count = 0

# 遍历每个数据库
for db in databases:
    db_name = db[0]
    # 添加过滤条件，只保留类似 CMT_DATA_6553 的数据库名称
    if len(db_name.split('_')) == 4 and len(db_name.split('_')[2]) == 4:
        continue
    try:
        # 切换到当前数据库
        cursor.execute(f"USE [{db_name}]")

        # 检查 dbo.MIS_XS 表是否存在
        table_name = "SY_HIS_GZFK"
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?", table_name)
        table_exists = cursor.fetchone()[0]

        if table_exists == 0:
            # print(f"Database: {db_name}, Table: dbo.{table_name} does not exist.")
            continue

        # 查询 dbo.MIS_XS 表的统计条数
        cursor.execute(f"SELECT COUNT(*) FROM dbo.[{table_name}] WHERE RQ>='2024-01-01' AND RQ<'2025-01-24'")
        count = cursor.fetchone()[0]

        # 如果条数为零，则跳过该数据库的输出
        if count == 0:
            continue

        # 输出结果
        print(f"Database: {db_name}, Table: dbo.{table_name}, Count: {count}")
        total_count += count
    except pyodbc.Error as e:
        print(f"Error accessing database {db_name}: {e}")

# 输出总和
print(f"Total Count: {total_count}")

# 关闭连接
cursor.close()
conn.close()



