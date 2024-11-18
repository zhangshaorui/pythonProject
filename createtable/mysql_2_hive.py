#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
from pyhive import hive
import re
import argparse

# 获取MySQL表结构
def get_mysql_table_schema(host, user, password, database, table):
    conn = pymysql.connect(host=host, user=user, password=password, database=database)
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table}")
    columns = cursor.fetchall()

    # 获取列的详细信息
    detailed_columns = []
    for col in columns:
        col_name = col[0]
        col_type = col[1].lower()

        if 'decimal' in col_type:
            # 解析精度和标度
            match = re.search(r'\((\d+),(\d+)\)', col_type)
            if match:
                precision, scale = match.groups()
                detailed_columns.append((col_name, f"DECIMAL({precision},{scale})"))
            else:
                detailed_columns.append((col_name, 'DECIMAL'))

        elif 'float' in col_type or 'double' in col_type:
            # 解析精度
            match = re.search(r'\((\d+)(?:,(\d+))?\)', col_type)
            if match:
                precision, scale = match.groups()
                if scale is None:
                    scale = 0
                detailed_columns.append((col_name, f"{col_type.upper()}({precision},{scale})"))
            else:
                detailed_columns.append((col_name, col_type.upper()))

        else:
            detailed_columns.append((col_name, col_type))

    cursor.close()
    conn.close()
    return detailed_columns

# 生成Hive的DDL
def generate_hive_ddl(table_name, columns):
    # MySQL到Hive的类型映射
    type_mapping = {
        'int': 'INT',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'TINYINT',
        'varchar': 'STRING',
        'char': 'STRING',
        'datetime': 'STRING',
        'date': 'STRING',
        'timestamp': 'STRING',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'decimal': 'DECIMAL',
        'text': 'STRING',
        'bool': 'BOOLEAN',
        'json': 'STRING'
    }

    # 遍历MySQL列，根据类型映射到Hive
    hive_columns = []
    for col in columns:
        col_name = col[0]
        col_type = col[1].upper()

        if 'DECIMAL' in col_type or 'FLOAT' in col_type or 'DOUBLE' in col_type:
            hive_columns.append(f"    {col_name} {col_type}")
        else:
            hive_type = 'STRING'  # 默认映射为STRING
            # 根据MySQL类型映射 注意这里采用的是in匹配的，当类型是date和datetime容易识别错误，所以统一都映射为STRING
            for mysql_type, hive_type in type_mapping.items():
                if mysql_type in col_type.lower():
                    hive_type = type_mapping[mysql_type]
                    break
            hive_columns.append(f"    {col_name} {hive_type}")

    hive_columns_str = ',\n'.join(hive_columns)
    ddl = f"""
CREATE TABLE {table_name} (
{hive_columns_str}
)
PARTITIONED BY (ods_date STRING)
STORED AS PARQUET;
"""
    return ddl

def create_hive_table(host, port, database, table_name, ddl):
    conn = hive.Connection(host=host, port=port, database=database)
    cursor = conn.cursor()
    cursor.execute(ddl)
    cursor.close()
    conn.close()

def main():
    # 创建解析器
    parser = argparse.ArgumentParser(description="Generate and execute Hive DDL from MySQL table schema.")
    
    # 添加参数
    parser.add_argument('--mysql_host', required=True, help='MySQL host')
    parser.add_argument('--mysql_user', required=True, help='MySQL user')
    parser.add_argument('--mysql_password', required=True, help='MySQL password')
    parser.add_argument('--mysql_database', required=True, help='MySQL database')
    parser.add_argument('--mysql_table', required=True, help='MySQL table')
    # parser.add_argument('--hive_host', required=True, help='Hive host')
    # parser.add_argument('--hive_port', required=True, type=int, help='Hive port')
    # parser.add_argument('--hive_database', required=True, help='Hive database')
    # parser.add_argument('--hive_table_name', required=True, help='Hive table name')

    # 解析参数
    args = parser.parse_args()

    # 获取MySQL表结构
    columns = get_mysql_table_schema(args.mysql_host, args.mysql_user, args.mysql_password, args.mysql_database, args.mysql_table)

    # 生成Hive DDL
    ddl = generate_hive_ddl(args.mysql_table, columns)

    # 在Hive中创建表
    # create_hive_table(args.hive_host, args.hive_port, args.hive_database, args.hive_table_name, ddl)

    # 打印SQL语句
    print(ddl)

if __name__ == "__main__":
    main()
