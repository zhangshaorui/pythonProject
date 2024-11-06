#!/user/bin/env python3
# -*- coding: utf-8 -*-
import pyodbc
import cx_Oracle
import argparse

# 数据类型映射关系
data_type_map = {
    'char': 'VARCHAR2',
    'nvarchar': 'VARCHAR2',
    'varchar': 'VARCHAR2',
    'money': 'NUMBER(19,4)',
    'int': 'NUMBER(38,0)',
    'bigint': 'NUMBER',
    'datetime': 'TIMESTAMP',
    'numeric': 'NUMBER',
    'bit': 'NUMBER(1)',
    'decimal': 'NUMBER'
}

# 连接SQL Server数据库
def connect_to_sqlserver(server, database, username, password):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)
    return conn

# 连接Oracle数据库
def connect_to_oracle(username, password, dsn):
    conn = cx_Oracle.connect(username, password, dsn)
    return conn

# 从SQL Server获取表结构
def get_table_structure(cursor, table_name):
    cursor.execute(f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH, 
            IS_NULLABLE, 
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = '{table_name}'
    """)
    columns = cursor.fetchall()
    return columns

# 从SQL Server获取主键信息
def get_primary_key(cursor, table_name):
    cursor.execute(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    primary_keys = [row.COLUMN_NAME for row in cursor.fetchall()]
    return primary_keys

# 生成Oracle建表语句
def generate_oracle_create_table_sql(table_name, columns, primary_keys):
    create_table_sql = f"CREATE TABLE {table_name} (\n"

    for col in columns:
        col_name = col.COLUMN_NAME
        data_type = col.DATA_TYPE
        max_length = col.CHARACTER_MAXIMUM_LENGTH
        is_nullable = col.IS_NULLABLE
        numeric_precision = col.NUMERIC_PRECISION
        numeric_scale = col.NUMERIC_SCALE

        # 转换SQL Server数据类型到Oracle数据类型
        oracle_type = data_type_map.get(data_type, "VARCHAR2(255)")

        # 处理字符类型长度
        if data_type in ['char', 'nvarchar', 'varchar']:
            oracle_type += f"({max_length})"

        # 处理数值类型精度和小数位数
        if data_type in ['numeric', 'decimal']:
            oracle_type += f"({numeric_precision}, {numeric_scale})"

        # 处理是否为空
        nullable = "NOT NULL" if is_nullable == 'NO' else ""

        create_table_sql += f"  {col_name} {oracle_type} {nullable},\n"

    # 处理主键
    if primary_keys:
        primary_key_sql = "CONSTRAINT PK_{} PRIMARY KEY ({})".format(
            table_name, ', '.join(primary_keys)
        )
        create_table_sql += primary_key_sql

    create_table_sql = create_table_sql.rstrip(",\n") + "\n)"

    return create_table_sql

# 在Oracle中创建表
def create_table_in_oracle(cursor, create_table_sql):
    cursor.execute(create_table_sql)

# 主函数
def main():
    # 执行脚本样例 python sqlserver_2_oracle.py --sqlserver_server xxxx --sqlserver_database xxxx --sqlserver_username xxxx --sqlserver_password xxxx --table_name xxxx
    parser = argparse.ArgumentParser(description="Create Oracle table from SQL Server table structure.")
    parser.add_argument('--sqlserver_server', required=True, help='SQL Server host')
    parser.add_argument('--sqlserver_database', required=True, help='SQL Server database name')
    parser.add_argument('--sqlserver_username', required=True, help='SQL Server username')
    parser.add_argument('--sqlserver_password', required=True, help='SQL Server password')
    parser.add_argument('--table_name', required=True, help='Table name to migrate')
    # parser.add_argument('--oracle_username', required=True, help='Oracle username')
    # parser.add_argument('--oracle_password', required=True, help='Oracle password')
    # parser.add_argument('--oracle_dsn', required=True, help='Oracle DSN')

    args = parser.parse_args()

    sqlserver_server = args.sqlserver_server
    sqlserver_database = args.sqlserver_database
    sqlserver_username = args.sqlserver_username
    sqlserver_password = args.sqlserver_password
    table_name = args.table_name
    # oracle_username = args.oracle_username
    # oracle_password = args.oracle_password
    # oracle_dsn = args.oracle_dsn

    # 如果是DUR_开头的，去掉DUR_前缀
    if table_name.startswith('DUR_'):
        target_name = table_name[4:]
    else:
        target_name = table_name

    # 连接到SQL Server
    sqlserver_conn = connect_to_sqlserver(sqlserver_server, sqlserver_database, sqlserver_username, sqlserver_password)
    sqlserver_cursor = sqlserver_conn.cursor()

    # 获取表结构
    columns = get_table_structure(sqlserver_cursor, table_name)

    # 获取主键信息
    primary_keys = get_primary_key(sqlserver_cursor, table_name)

    # 生成Oracle建表语句
    create_table_sql = generate_oracle_create_table_sql(target_name, columns, primary_keys)
    print(create_table_sql)

    # 连接到Oracle
    # oracle_conn = connect_to_oracle(oracle_username, oracle_password, oracle_dsn)
    # oracle_cursor = oracle_conn.cursor()

    # 在Oracle中创建表
    # create_table_in_oracle(oracle_cursor, create_table_sql)

    # 提交事务并关闭连接
    # oracle_conn.commit()
    # oracle_cursor.close()
    # oracle_conn.close()
    sqlserver_cursor.close()
    sqlserver_conn.close()

if __name__ == "__main__":
    main()
