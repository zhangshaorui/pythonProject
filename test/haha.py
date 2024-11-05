#!/user/bin/env python3
# -*- coding: utf-8 -*-
# 原始 SQL 语句
sql_statement = """
CREATE TABLE MIS_HIS_GZKK (
  ID DECIMAL(38,0),
   STORECODE VARCHAR(8),
   RQ DATE,
   GZH VARCHAR(9),
   HTH VARCHAR(6),
   KL DECIMAL(5, 2),
   XSJE DECIMAL(20, 4),
   CBJE DECIMAL(20, 4),
   YFJE DECIMAL(20, 4),
   CDHYJF DECIMAL(20, 4),
   CDCRDT DECIMAL(20, 4),
   CDCXZK DECIMAL(20, 4),
   CDGWQ DECIMAL(20, 4),
   CDWBK DECIMAL(20, 4),
   CDTGK DECIMAL(20, 4),
   CXBZ DECIMAL(38,0),
   TZBZ DECIMAL(38,0),
   BZ VARCHAR(50),
   MJJE DECIMAL(20, 4),
   HIS DECIMAL(38,0),

"""

# 移除最后一个逗号
sql_statement = sql_statement.rstrip(',\n') + '\n'

print(sql_statement)
