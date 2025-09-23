import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# 添加项目根目录到路径
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

db = PostgreSQLManager()
hfq_close = pd.read_csv("D:/program_learning/光大实习/raw_data/hfq/close.csv",index_col='date')


# success = db.insert_data("equity_fundamental_data.price_hfq_close",
#                         c,update_existing=False)
# a = db._load_data(c)
c = db.query_data("price_hfq_close")
print(c)
# print(c.info())