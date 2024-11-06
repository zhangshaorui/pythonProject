#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import cx_Oracle
import argparse

# 数据类型映射字典
type_mapping = {
    'VARCHAR2': 'VARCHAR',
    'NUMBER': 'DECIMAL',
    'FLOAT': 'FLOAT',
    'DATE': 'DATE',
    'TIMESTAMP': 'DATETIME',
}

def get_oracle_table_structure(oracle_dsn, username, password, table_name):
    # Oracle数据库连接信息
    oracle_connection = cx_Oracle.connect(username, password, dsn=oracle_dsn)

    # 获取表的列信息
    cursor = oracle_connection.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type, data_length, data_precision, data_scale
        FROM all_tab_columns
        WHERE table_name = '{table_name}'
    """)
    columns = cursor.fetchall()

    # 获取主键信息
    cursor.execute(f"""
        SELECT column_name
        FROM all_cons_columns
        WHERE constraint_name = (
            SELECT constraint_name
            FROM user_constraints
            WHERE table_name = '{table_name}' AND constraint_type = 'P'
        )
    """)
    primary_keys = [row[0] for row in cursor.fetchall()]

    # 关闭连接
    cursor.close()
    oracle_connection.close()

    return columns, primary_keys

def build_starrocks_create_table_sql(table_name, columns, primary_keys, type_mapping):
    create_table_sql = f"CREATE TABLE {table_name} (\n"
    for column in columns:
        column_name = column[0]
        data_type = column[1]
        data_length = column[2]
        data_precision = column[3]
        data_scale = column[4]

        # 根据Oracle的数据类型映射到StarRocks的数据类型
        starrocks_type = type_mapping.get(data_type, 'STRING')

        # 处理精度信息
        if data_type == 'NUMBER':
            if data_precision is not None and data_scale is not None:
                starrocks_type = f"DECIMAL({data_precision},{data_scale})"
            elif data_precision is not None:
                starrocks_type = f"DECIMAL({data_precision})"
            else:
                starrocks_type = 'DECIMAL(38,0)'
        elif data_type == 'VARCHAR2':
            starrocks_type = f"VARCHAR({data_length})"

        create_table_sql += f"  {column_name} {starrocks_type},\n"

    # 去掉最后一个逗号和换行符，并添加表引擎
    create_table_sql = create_table_sql.rstrip(',\n') + '\n) ENGINE=OLAP \n'

    # 添加主键信息
    if primary_keys:
        create_table_sql += f"PRIMARY KEY ({','.join(primary_keys)})"

    # 添加其他属性，根据实际情况进行修改，分区先写死为STORECODE
    create_table_sql += (f"""
PARTITION BY (STORECODE)
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"compression" = "LZ4"
);
    """)

    return create_table_sql

def main():
    # 执行脚本样例 python oracle_2_starrocks.py --dsn "xxxx:1521/xxxx" --username "xxxx" --password "xxxx" --table_name "xxxx"
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="Generate StarRocks CREATE TABLE SQL from Oracle table structure.")
    parser.add_argument("--dsn", required=True, help="Oracle DSN (Data Source Name)")
    parser.add_argument("--username", required=True, help="Oracle username")
    parser.add_argument("--password", required=True, help="Oracle password")
    parser.add_argument("--table_name", required=True, help="Oracle table name")

    # 解析命令行参数
    args = parser.parse_args()

    # 获取Oracle表结构
    columns, primary_keys = get_oracle_table_structure(args.dsn, args.username, args.password, args.table_name)

    # 构建StarRocks的SQL语句
    create_table_sql = build_starrocks_create_table_sql(args.table_name, columns, primary_keys, type_mapping)

    # 打印SQL语句
    print(create_table_sql)

if __name__ == "__main__":
    main()
