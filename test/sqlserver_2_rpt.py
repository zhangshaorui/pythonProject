#!/user/bin/env python3
# -*- coding: utf-8 -*-
import pyodbc

# 数据库连接信息
server = 'xxxx'
database = 'xxxx'
username = 'xxxx'
password = 'xxxx'

# 表名
original_table_name = 'xxxx'
new_table_name = f'DUR_{original_table_name}'

# 连接到SQL Server
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 获取原表的结构
cursor.execute(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{original_table_name}'
""")

columns = cursor.fetchall()

# 构建新表的CREATE TABLE语句
create_table_sql = f"CREATE TABLE [RPT].[dbo].[{new_table_name}] (\n"
create_table_sql += "    [STORECODE] [varchar](8) NOT NULL,\n"

for col in columns:
    column_name = col.COLUMN_NAME
    data_type = col.DATA_TYPE
    max_length = col.CHARACTER_MAXIMUM_LENGTH
    is_nullable = col.IS_NULLABLE

    # 处理数据类型和长度
    if data_type == 'varchar' or data_type == 'char':
        column_definition = f"    [{column_name}] [{data_type}]({max_length})"
    else:
        column_definition = f"    [{column_name}] [{data_type}]"

    if data_type == 'numeric':
        column_definition += f"({col.NUMERIC_PRECISION}, {col.NUMERIC_SCALE})"

    # 处理是否为空
    if is_nullable == 'NO':
        column_definition += " NOT NULL"
    else:
        column_definition += " NULL"

    create_table_sql += column_definition + ",\n"

# 获取原表的主键
cursor.execute(f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_NAME = '{original_table_name}'
    ORDER BY ORDINAL_POSITION
""")

primary_keys = [row.COLUMN_NAME for row in cursor.fetchall()]

# 添加联合主键
primary_keys.append('STORECODE')
primary_key_definition = ", ".join([f"[{pk}]" for pk in primary_keys])
create_table_sql += f"    CONSTRAINT [PK_{new_table_name}] PRIMARY KEY NONCLUSTERED ({primary_key_definition})\n"

create_table_sql += ")"

# 执行创建表的SQL语句
# cursor.execute(create_table_sql)
print(create_table_sql)
# conn.commit()

# 关闭连接
cursor.close()
conn.close()