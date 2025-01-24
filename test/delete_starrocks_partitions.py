#!/user/bin/env python3
# -*- coding: utf-8 -*-
# /************************************************************************************************************************
#  * @File Name: connect_starrocks
#  * @Description: 查询StarRocks数据库中表的分区信息，并删除大小为0的分区
#  * @Author: shaorui zhang
#  * @Date: 2025-01-16
#  * @Version: 1.0
#  **********************************************************************************************************************/
import pymysql

# StarRocks数据库连接配置
starrocks_host = 'xxxxx'
starrocks_port = 9030  # 默认端口
starrocks_user = 'xxxxx'
starrocks_password = 'xxxxx'
starrocks_database = 'xxxxx'

def connect_to_starrocks():
    try:
        connection = pymysql.connect(
            host=starrocks_host,
            port=starrocks_port,
            user=starrocks_user,
            password=starrocks_password,
            database=starrocks_database
        )
        if connection.open:
            print("Successfully connected to StarRocks database")
            cursor = connection.cursor()
            # 查询 UF_DATA_XS 表的分区信息
            query = """
            SHOW PARTITIONS FROM SY_HIS_GZFK;
            """
            cursor.execute(query)
            partitions = cursor.fetchall()
            partition_count = len(partitions)
            print(f"Total number of partitions in UF_DATA_XS: {partition_count}")

            # 删除 datasize 为 0 的分区
            # for partition in partitions:
            #     if partition[-1] == '0':
            #         drop_query = f"ALTER TABLE UF_DATA_XS DROP PARTITION {partition[1]} FORCE"
            #         cursor.execute(drop_query)
            #         print(f"Dropped partition: {partition[1]}")

            # 统计最后一个大小等于0的分区数量
            zero_size_partition_count = sum(1 for partition in partitions if partition[-1] == '0')
            print(f"Number of partitions with size 0: {zero_size_partition_count}")

            # connection.commit()  # 提交事务
            return connection
    except pymysql.Error as err:
        print(f"Error: {err}")
        return None

# 示例调用
if __name__ == "__main__":
    conn = connect_to_starrocks()
    if conn:
        conn.close()