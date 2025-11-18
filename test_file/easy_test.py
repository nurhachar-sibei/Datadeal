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
import_df = pd.read_csv("D:/program_learning/光大实习/processed_data/RAWDATA_Industry_I.csv",index_col=0)
success = db.create_table("equity_fundamental_data.industry1",import_df,overwrite=True)
# a = db._load_data(c)
# c = db.query_data("equity_fundamental_data.price_hfq_close",start_date='2020-01-01',end_date='2021-01-05')
# print(c)
# print(c.info())