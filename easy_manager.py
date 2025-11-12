"""
PostgreSQL数据管理系统
用于简单的数据存储和管理

Author: Nurhachar
Date: 2025
"""

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import json
import logging
import time
from functools import wraps
from typing import Union, List, Dict, Optional, Any
from pathlib import Path
import warnings

print("Easy Manager is running...")
# 配置日志
# 配置日志格式，使其更符合用户要求的格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('datadeal.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def function_timer(func):
    """
    函数计时装饰器
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(__name__)
        logger.info(f'[Function: {func.__name__} started...]')
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info(f'[Function: {func.__name__} completed, elapsed time: {elapsed_time:.2f}s]')
            return result
        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.error(f'[Function: {func.__name__} failed after {elapsed_time:.2f}s, error: {str(e)}]')
            raise
    
    return wrapper


class EasyManager:
    """
    简易PostgreSQL数据管理类
    支持创建表格、插入数据（去重）、删除表格、导入表格等操作
    """
    
    def __init__(self, 
                 database: str = "test_data_base",
                 user: str = "postgres", 
                 password: str = "cbw88982449",
                 host: str = "localhost",
                 port: str = "5432"):
        """
        初始化数据库连接
        
        Args:
            database: 数据库名
            user: 用户名
            password: 密码
            host: 主机地址
            port: 端口号
        """
        self.logger = logging.getLogger(__name__)
        self.db_config = {
            'database': database,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        
        self.conn = None
        self.cursor = None
        self._connect()
    
    def _connect(self):
        """建立数据库连接"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.logger.info(f"数据库连接成功: {self.db_config['database']}")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise
    
    def _ensure_connection(self):
        """确保数据库连接有效"""
        try:
            self.cursor.execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            self.logger.warning("数据库连接已断开，正在重新连接...")
            self._connect()
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """
        推断pandas列的PostgreSQL数据类型
        
        Args:
            series: pandas Series
            
        Returns:
            PostgreSQL数据类型字符串
        """
        dtype = series.dtype
        
        # 数值类型
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        # 布尔类型
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        # 日期时间类型
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        # 字符串类型
        else:
            # 检查最大长度
            max_length = series.astype(str).str.len().max()
            if pd.isna(max_length) or max_length == 0:
                return "TEXT"
            elif max_length <= 255:
                return f"VARCHAR({int(max_length * 1.5)})"  # 留点余量
            else:
                return "TEXT"
    
    @function_timer
    def create_table(self, table_name: str, dataframe: pd.DataFrame, 
                     overwrite: bool = False) -> bool:
        """
        在数据库中创建表格
        
        Args:
            table_name: 表名
            dataframe: pandas DataFrame
            overwrite: 是否覆盖已存在的表
            
        Returns:
            bool: 创建是否成功
        """
        self._ensure_connection()
        
        try:
            # 检查表是否存在
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name.split('.')[-1],))
            
            table_exists = self.cursor.fetchone()['exists']
            
            if table_exists and not overwrite:
                self.logger.warning(f"表 {table_name} 已存在，使用overwrite=True来覆盖")
                return False
            
            if table_exists and overwrite:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                self.logger.info(f"已删除现有表 {table_name}")
            
            # 创建表结构
            df = dataframe.copy()
            
            # 处理索引：如果索引有名称且不是默认的RangeIndex，将其作为列
            if df.index.name and not isinstance(df.index, pd.RangeIndex):
                df = df.reset_index()
            elif not isinstance(df.index, pd.RangeIndex):
                df.index.name = 'index'
                df = df.reset_index()
            
            # 构建列定义
            columns_sql = []
            for col in df.columns:
                col_type = self._infer_column_type(df[col])
                # 清理列名，确保符合SQL标准
                clean_col = col.replace('.', '_').replace('-', '_').replace(' ', '_')
                columns_sql.append(f'"{clean_col}" {col_type}')
            
            create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns_sql)}
                )
            """
            
            self.cursor.execute(create_sql)
            self.conn.commit()
            
            self.logger.info(f"表 {table_name} 创建成功，包含 {len(df.columns)} 列")
            
            # 插入数据
            self._insert_dataframe(table_name, df)
            
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"创建表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _insert_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        将DataFrame插入到表中
        
        Args:
            table_name: 表名
            df: 要插入的DataFrame
        """
        if df.empty:
            self.logger.warning("DataFrame为空，跳过插入")
            return
        
        # 清理列名
        df_clean = df.copy()
        df_clean.columns = [col.replace('.', '_').replace('-', '_').replace(' ', '_') 
                           for col in df.columns]
        
        # 准备数据
        columns = ', '.join([f'"{col}"' for col in df_clean.columns])
        placeholders = ', '.join(['%s'] * len(df_clean.columns))
        
        insert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        """
        
        # 转换数据为元组列表
        data_tuples = []
        for _, row in df_clean.iterrows():
            # 处理NaN值
            row_data = tuple(None if pd.isna(x) else x for x in row)
            data_tuples.append(row_data)
        
        # 批量插入
        psycopg2.extras.execute_batch(
            self.cursor, insert_sql, data_tuples, page_size=1000
        )
        self.conn.commit()
        
        self.logger.info(f"成功插入 {len(data_tuples)} 行数据到表 {table_name}")
    
    @function_timer
    def insert_data(self, table_name: str, dataframe: pd.DataFrame, 
                    deduplicate: bool = True) -> bool:
        """
        向表中插入数据，如果表存在则只插入不重复的行
        
        Args:
            table_name: 表名
            dataframe: pandas DataFrame
            deduplicate: 是否去重（默认True）
            
        Returns:
            bool: 插入是否成功
        """
        self._ensure_connection()
        
        try:
            # 检查表是否存在
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name.split('.')[-1],))
            
            if not self.cursor.fetchone()['exists']:
                self.logger.error(f"表 {table_name} 不存在，请先使用create_table创建表")
                return False
            
            df = dataframe.copy()
            
            # 处理索引
            if df.index.name and not isinstance(df.index, pd.RangeIndex):
                df = df.reset_index()
            elif not isinstance(df.index, pd.RangeIndex):
                df.index.name = 'index'
                df = df.reset_index()
            
            if deduplicate:
                # 读取现有数据
                existing_df = self.load_table(table_name)
                
                if existing_df is not None and not existing_df.empty:
                    # 找出不重复的行
                    # 合并两个DataFrame并标记重复
                    df_combined = pd.concat([existing_df, df], ignore_index=True)
                    df_new = df_combined.drop_duplicates(keep=False)
                    
                    # 如果没有新数据
                    if df_new.empty:
                        self.logger.info(f"没有新数据需要插入到表 {table_name}")
                        return True
                    
                    self.logger.info(f"发现 {len(df_new)} 行新数据（去重后）")
                    df = df_new
            
            # 清理列名
            df.columns = [col.replace('.', '_').replace('-', '_').replace(' ', '_') 
                         for col in df.columns]
            
            # 插入数据
            self._insert_dataframe(table_name, df)
            
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"插入数据到表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    @function_timer
    def drop_table(self, table_name: str) -> bool:
        """
        删除表格
        
        Args:
            table_name: 表名
            
        Returns:
            bool: 删除是否成功
        """
        self._ensure_connection()
        
        try:
            # 检查表是否存在
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name.split('.')[-1],))
            
            if not self.cursor.fetchone()['exists']:
                self.logger.warning(f"表 {table_name} 不存在")
                return False
            
            # 删除表
            self.cursor.execute(f"DROP TABLE {table_name} CASCADE")
            self.conn.commit()
            
            self.logger.info(f"成功删除表: {table_name}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"删除表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    @function_timer
    def load_table(self, table_name: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        从数据库导入表格到Python
        
        Args:
            table_name: 表名
            limit: 限制返回行数（可选）
            
        Returns:
            pandas DataFrame 或 None
        """
        self._ensure_connection()
        
        try:
            # 检查表是否存在
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name.split('.')[-1],))
            
            if not self.cursor.fetchone()['exists']:
                self.logger.error(f"表 {table_name} 不存在")
                return None
            
            # 构建查询
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            # 执行查询
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            if not results:
                self.logger.warning(f"表 {table_name} 为空")
                return pd.DataFrame()
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            self.logger.info(f"成功从表 {table_name} 加载数据，形状: {df.shape}")
            return df
            
        except Exception as e:
            import traceback
            self.logger.error(f"加载表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return None
    
    def list_tables(self, schema: str = 'public') -> List[str]:
        """
        列出数据库中所有表
        
        Args:
            schema: 模式名（默认为public）
            
        Returns:
            表名列表
        """
        self._ensure_connection()
        
        try:
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name
            """, (schema,))
            
            tables = [row['table_name'] for row in self.cursor.fetchall()]
            self.logger.info(f"找到 {len(tables)} 个表")
            return tables
            
        except Exception as e:
            self.logger.error(f"获取表列表失败: {str(e)}")
            return []
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        获取表信息
        
        Args:
            table_name: 表名
            
        Returns:
            表信息字典
        """
        self._ensure_connection()
        
        try:
            # 获取列信息
            self.cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name.split(".")[-1],))
            
            columns = self.cursor.fetchall()
            
            # 获取行数
            self.cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
            row_count = self.cursor.fetchone()['row_count']
            
            info = {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count
            }
            
            self.logger.info(f"表 {table_name} 信息: {len(columns)} 列, {row_count} 行")
            return info
            
        except Exception as e:
            self.logger.error(f"获取表信息失败: {str(e)}")
            return {}
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("数据库连接已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 使用示例
if __name__ == "__main__":
    # 创建管理器实例
    with EasyManager() as em:
        # 列出所有表
        tables = em.list_tables()
        print("现有表:", tables)
