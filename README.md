**自动创建表脚本**

安装对应的数据库驱动

pip install pyodbc

pip install cx_Oracle

win11上面，读取Oracle表，需要在本地安装Oracle驱动，下载地址 https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
下载最新的驱动就行，然后配置一下环境变量path就行了

**执行脚本样例**

python sqlserver_2_rpt.py --sqlserver_server xxxx --sqlserver_database xxxx --sqlserver_username xxxx --sqlserver_password xxxx --table_name xxxx

python sqlserver_2_oracle.py --sqlserver_server xxxx --sqlserver_database xxxx --sqlserver_username xxxx --sqlserver_password xxxx --table_name xxxx

python oracle_2_starrocks.py --dsn "xxxx:1521/xxxx" --username "xxxx" --password "xxxx" --table_name "xxxx"