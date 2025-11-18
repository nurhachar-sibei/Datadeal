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
    
    def _clean_column_name(self, column_name: str) -> str:
        """
        清理列名，确保符合SQL标准
        
        Args:
            column_name: 原始列名
            
        Returns:
            清理后的列名
        """
        # 替换特殊字符为下划线
        clean_name = column_name.replace('.', '_').replace('-', '_').replace(' ', '_')
        return clean_name
    
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
                clean_col = self._clean_column_name(col)
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
        df_clean.columns = [self._clean_column_name(col) for col in df.columns]
        
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
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        获取表的列名列表
        
        Args:
            table_name: 表名
            
        Returns:
            列名列表
        """
        self.cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name.split('.')[-1],))
        
        return [row['column_name'] for row in self.cursor.fetchall()]
    
    @function_timer
    def add_columns(self, table_name: str, dataframe: pd.DataFrame, 
                    merge_on_index: bool = True) -> bool:
        """
        在表中增加新列，按索引合并数据
        
        Args:
            table_name: 表名
            dataframe: 包含新列的 DataFrame
            merge_on_index: 是否基于索引合并（默认True）
            
        Returns:
            bool: 添加是否成功
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
                return False
            
            # 获取现有列
            existing_columns = self._get_table_columns(table_name)
            self.logger.info(f"表 {table_name} 现有列: {existing_columns}")
            
            # 处理DataFrame
            df = dataframe.copy()
            
            # 处理索引
            index_name = None
            if df.index.name and not isinstance(df.index, pd.RangeIndex):
                index_name = df.index.name
                df = df.reset_index()
            elif not isinstance(df.index, pd.RangeIndex):
                index_name = 'index'
                df.index.name = index_name
                df = df.reset_index()
            
            # 清理列名
            df.columns = [col.replace('.', '_').replace('-', '_').replace(' ', '_') 
                         for col in df.columns]
            if index_name:
                index_name = index_name.replace('.', '_').replace('-', '_').replace(' ', '_')
            
            # 识别新列（排除已存在的列）
            new_columns = [col for col in df.columns if col not in existing_columns]
            
            if not new_columns:
                self.logger.warning(f"没有新列需要添加到表 {table_name}")
                return True
            
            self.logger.info(f"识别到 {len(new_columns)} 个新列: {new_columns}")
            
            # 添加新列到表结构
            for col in new_columns:
                col_type = self._infer_column_type(df[col])
                alter_sql = f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type}'
                self.cursor.execute(alter_sql)
                self.logger.info(f"添加列 {col} (类型: {col_type})")
            
            self.conn.commit()
            
            # 按索引合并数据
            if merge_on_index and index_name:
                # 加载现有数据
                existing_df = self.load_table(table_name)
                
                if existing_df is not None and not existing_df.empty:
                    self.logger.info(f"按索引列 '{index_name}' 合并数据")
                    
                    # 只保留新列和索引列
                    df_new_cols = df[[index_name] + new_columns]
                    
                    # 更新每一行的新列数据
                    update_count = 0
                    for _, row in df_new_cols.iterrows():
                        index_value = row[index_name]
                        
                        # 构建UPDATE语句
                        set_clause = ', '.join([f'"{col}" = %s' for col in new_columns])
                        update_sql = f"""
                            UPDATE {table_name}
                            SET {set_clause}
                            WHERE "{index_name}" = %s
                        """
                        
                        # 准备参数
                        values = [None if pd.isna(row[col]) else row[col] for col in new_columns]
                        values.append(index_value)
                        self.cursor.execute(update_sql, values)
                        if self.cursor.rowcount > 0:
                            update_count += 1
                    
                    self.conn.commit()
                    self.logger.info(f"成功更新 {update_count} 行的新列数据")
            
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"添加列到表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            self.logger.error("请优先检查数据格式问题,注:所有的时间格式都需要pd.to_datatime后方可录入")
            return False
    
    @function_timer
    def insert_data(self, table_name: str, dataframe: pd.DataFrame, 
                    mode: str = 'skip') -> bool:
        """
        向表中插入数据，支持多种重复数据处理模式
        
        Args:
            table_name: 表名
            dataframe: pandas DataFrame
            mode: 重复数据处理模式
                - 'skip': 忽略重复数据，只插入新数据（默认）
                - 'update': 覆盖重复数据，基于索引更新
                - 'append': 直接追加，不检查重复
            
        Returns:
            bool: 插入是否成功
        """
        self._ensure_connection()
        
        if mode not in ['skip', 'update', 'append']:
            self.logger.error(f"不支持的模式: {mode}，请使用 'skip', 'update' 或 'append'")
            return False
        
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
            index_name = None
            if df.index.name and not isinstance(df.index, pd.RangeIndex):
                index_name = df.index.name
                df = df.reset_index()
            elif not isinstance(df.index, pd.RangeIndex):
                index_name = 'index'
                df.index.name = index_name
                df = df.reset_index()
            
            # 清理列名
            df.columns = [col.replace('.', '_').replace('-', '_').replace(' ', '_') 
                         for col in df.columns]
            if index_name:
                index_name = index_name.replace('.', '_').replace('-', '_').replace(' ', '_')
            
            # 根据模式处理数据
            if mode == 'append':
                # 直接插入，不检查重复
                self.logger.info(f"使用 append 模式，直接插入 {len(df)} 行数据")
                self._insert_dataframe(table_name, df)
                
            elif mode == 'skip':
                # 忽略重复索引的数据，只插入新索引的数据
                if not index_name:
                    self.logger.error("skip 模式需要有索引列，但未找到索引")
                    return False
                
                existing_df = self.load_table(table_name)
                
                if existing_df is not None and not existing_df.empty:
                    # 检查索引列是否存在
                    if index_name not in existing_df.columns:
                        self.logger.error(f"索引列 '{index_name}' 不存在于表中")
                        return False
                    
                    # 基于索引找出不重复的行
                    existing_indices = set(existing_df[index_name].values)
                    new_indices = set(df[index_name].values)
                    
                    # 只保留索引不重复的行
                    indices_to_insert = new_indices - existing_indices
                    
                    # 如果没有新数据
                    if not indices_to_insert:
                        self.logger.info(f"没有新的索引数据需要插入到表 {table_name}")
                        return True
                    
                    # 过滤出要插入的数据
                    df_to_insert = df[df[index_name].isin(indices_to_insert)]
                    
                    self.logger.info(f"使用 skip 模式，基于索引过滤后有 {len(df_to_insert)} 行新数据")
                    self._insert_dataframe(table_name, df_to_insert)
                else:
                    self.logger.info(f"表为空，插入 {len(df)} 行数据")
                    self._insert_dataframe(table_name, df)
                    
            elif mode == 'update':
                # 覆盖重复数据，基于索引更新
                if not index_name:
                    self.logger.error("update 模式需要有索引列，但未找到索引")
                    return False
                
                existing_df = self.load_table(table_name)
                
                if existing_df is None or existing_df.empty:
                    self.logger.info(f"表为空，直接插入 {len(df)} 行数据")
                    self._insert_dataframe(table_name, df)
                else:
                    # 检查索引列是否存在
                    if index_name not in existing_df.columns:
                        self.logger.error(f"索引列 '{index_name}' 不存在于表中")
                        return False
                    
                    # 获取所有列（排除索引列）
                    data_columns = [col for col in df.columns if col != index_name]
                    
                    # 找出需要更新的行和需要插入的行
                    existing_indices = set(existing_df[index_name].values)
                    new_indices = set(df[index_name].values)
                    
                    indices_to_update = existing_indices & new_indices
                    indices_to_insert = new_indices - existing_indices
                    
                    update_count = 0
                    insert_count = 0
                    
                    # 更新重复的行
                    if indices_to_update:
                        self.logger.info(f"使用 update 模式，更新 {len(indices_to_update)} 行")
                        df_to_update = df[df[index_name].isin(indices_to_update)]
                        
                        for _, row in df_to_update.iterrows():
                            index_value = row[index_name]
                            
                            # 构建UPDATE语句
                            set_clause = ', '.join([f'"{col}" = %s' for col in data_columns])
                            update_sql = f"""
                                UPDATE {table_name}
                                SET {set_clause}
                                WHERE "{index_name}" = %s
                            """
                            
                            # 准备参数
                            values = [None if pd.isna(row[col]) else row[col] for col in data_columns]
                            values.append(index_value)
                            
                            self.cursor.execute(update_sql, values)
                            if self.cursor.rowcount > 0:
                                update_count += 1
                        
                        self.conn.commit()
                        self.logger.info(f"成功更新 {update_count} 行数据")
                    
                    # 插入新的行
                    if indices_to_insert:
                        self.logger.info(f"插入 {len(indices_to_insert)} 行新数据")
                        df_to_insert = df[df[index_name].isin(indices_to_insert)]
                        self._insert_dataframe(table_name, df_to_insert)
                        insert_count = len(df_to_insert)
                    
                    self.logger.info(f"update 模式完成: 更新 {update_count} 行, 插入 {insert_count} 行")
            
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
    
    def list_tables(self, schema: str = 'public', verbose: bool = False, 
                    pattern: str = None, print_table: bool = False) -> List[Dict[str, Any]]:
        """
        列出数据库中所有表
        
        Args:
            schema: 模式名（默认为public）
            verbose: 是否显示详细信息（行数、大小等）
            pattern: 表名过滤模式（支持SQL LIKE语法，如 'stock%'）
            print_table: 是否以美观的表格形式打印
            
        Returns:
            表信息列表，包含表名、行数、大小等信息
        """
        self._ensure_connection()
        
        try:
            if verbose:
                # 获取详细信息
                query = """
                    SELECT 
                        t.table_name,
                        pg_size_pretty(pg_total_relation_size(quote_ident(t.table_name)::regclass)) as size,
                        (SELECT COUNT(*) FROM information_schema.columns 
                         WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count
                    FROM information_schema.tables t
                    WHERE t.table_schema = %s
                """
                
                params = [schema]
                if pattern:
                    query += " AND t.table_name LIKE %s"
                    params.append(pattern)
                
                query += " ORDER BY t.table_name"
                
                self.cursor.execute(query, params)
                tables = []
                
                for row in self.cursor.fetchall():
                    table_name = row['table_name']
                    # 获取行数
                    self.cursor.execute(f'SELECT COUNT(*) as row_count FROM "{table_name}"')
                    row_count = self.cursor.fetchone()['row_count']
                    
                    tables.append({
                        'table_name': table_name,
                        'row_count': row_count,
                        'column_count': row['column_count'],
                        'size': row['size']
                    })
                
                if print_table:
                    self._print_tables_info(tables)
                
                self.logger.info(f"找到 {len(tables)} 个表")
                return tables
            else:
                # 简单模式：只返回表名
                query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s
                """
                
                params = [schema]
                if pattern:
                    query += " AND table_name LIKE %s"
                    params.append(pattern)
                
                query += " ORDER BY table_name"
                
                self.cursor.execute(query, params)
                tables = [{'table_name': row['table_name']} for row in self.cursor.fetchall()]
                
                if print_table:
                    print(f"\n[数据库表列表] (共 {len(tables)} 个):")
                    print("-" * 40)
                    for i, table in enumerate(tables, 1):
                        print(f"  {i}. {table['table_name']}")
                    print("-" * 40)
                
                self.logger.info(f"找到 {len(tables)} 个表")
                return tables
            
        except Exception as e:
            self.logger.error(f"获取表列表失败: {str(e)}")
            return []
    
    def _print_tables_info(self, tables: List[Dict[str, Any]]):
        """打印表信息的美观格式"""
        if not tables:
            print("\n[表列表] 没有找到任何表")
            return
        
        print(f"\n{'='*80}")
        print(f"[数据库表列表] (共 {len(tables)} 个表)")
        print(f"{'='*80}")
        print(f"{'序号':<6} {'表名':<30} {'行数':<12} {'列数':<8} {'大小':<10}")
        print("-" * 80)
        
        for i, table in enumerate(tables, 1):
            print(f"{i:<6} {table['table_name']:<30} {table['row_count']:>10,}  "
                  f"{table['column_count']:>6}  {table['size']:>10}")
        
        print("=" * 80)
        
        # 统计信息
        total_rows = sum(t['row_count'] for t in tables)
        print(f"[统计] 总行数: {total_rows:,}")
        print("=" * 80 + "\n")
    
    def get_table_info(self, table_name: str, print_info: bool = False) -> Dict[str, Any]:
        """
        获取表的详细信息
        
        Args:
            table_name: 表名
            print_info: 是否以美观格式打印信息
            
        Returns:
            表信息字典，包含列信息、索引、约束、大小等
        """
        self._ensure_connection()
        
        try:
            table_name_only = table_name.split(".")[-1]
            
            # 1. 获取列信息（包含默认值和约束）
            self.cursor.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name_only,))
            
            columns = self.cursor.fetchall()
            
            # 2. 获取行数
            self.cursor.execute(f'SELECT COUNT(*) as row_count FROM "{table_name_only}"')
            row_count = self.cursor.fetchone()['row_count']
            
            # 3. 获取表大小
            self.cursor.execute("""
                SELECT pg_size_pretty(pg_total_relation_size(%s::regclass)) as size
            """, (table_name_only,))
            size = self.cursor.fetchone()['size']
            
            # 4. 获取索引信息
            self.cursor.execute("""
                SELECT
                    indexname as index_name,
                    indexdef as index_definition
                FROM pg_indexes
                WHERE tablename = %s
            """, (table_name_only,))
            
            indexes = self.cursor.fetchall()
            
            # 5. 获取主键信息
            self.cursor.execute("""
                SELECT a.attname as column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (table_name_only,))
            
            primary_keys = [row['column_name'] for row in self.cursor.fetchall()]
            
            # 6. 组装信息
            info = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(columns),
                'size': size,
                'columns': columns,
                'indexes': indexes,
                'primary_keys': primary_keys
            }
            
            if print_info:
                self._print_table_info(info)
            
            self.logger.info(f"表 {table_name} 信息: {len(columns)} 列, {row_count} 行, {size}")
            return info
            
        except Exception as e:
            import traceback
            self.logger.error(f"获取表信息失败: {str(e)}")
            self.logger.error(f"详细错误: {traceback.format_exc()}")
            return {}
    
    def _print_table_info(self, info: Dict[str, Any]):
        """以美观格式打印表信息"""
        print(f"\n{'='*80}")
        print(f"[表信息] {info['table_name']}")
        print(f"{'='*80}")
        
        # 基本信息
        print(f"\n[基本统计]")
        print(f"  - 总行数: {info['row_count']:,}")
        print(f"  - 总列数: {info['column_count']}")
        print(f"  - 表大小: {info['size']}")
        
        # 主键信息
        if info['primary_keys']:
            print(f"\n[主键]")
            for pk in info['primary_keys']:
                print(f"  - {pk}")
        
        # 列信息
        print(f"\n[列详情]")
        print(f"{'序号':<6} {'列名':<25} {'类型':<20} {'可空':<8} {'默认值':<15}")
        print("-" * 80)
        
        for i, col in enumerate(info['columns'], 1):
            col_name = col['column_name']
            data_type = col['data_type']
            if col.get('character_maximum_length'):
                data_type += f"({col['character_maximum_length']})"
            
            nullable = "Y" if col['is_nullable'] == 'YES' else "N"
            default = str(col['column_default'])[:15] if col['column_default'] else "-"
            
            # 标记主键
            if col_name in info['primary_keys']:
                col_name += " [PK]"
            
            print(f"{i:<6} {col_name:<25} {data_type:<20} {nullable:<8} {default:<15}")
        
        # 索引信息
        if info['indexes']:
            print(f"\n[索引] (共 {len(info['indexes'])} 个)")
            for i, idx in enumerate(info['indexes'], 1):
                print(f"  {i}. {idx['index_name']}")
                # 简化索引定义显示
                idx_def = idx['index_definition']
                if len(idx_def) > 70:
                    idx_def = idx_def[:70] + "..."
                print(f"     {idx_def}")
        
        print("=" * 80 + "\n")
    
    @staticmethod
    def help():
        """
        显示 EasyManager 的功能帮助信息
        """
        help_text = """
╔════════════════════════════════════════════════════════════════════════════╗
║                      EasyManager 使用帮助 (v2.2)                           ║
╚════════════════════════════════════════════════════════════════════════════╝

📚 核心功能：

  1. create_table(table_name, dataframe, overwrite=False)
     └─ 创建表并导入数据
     └─ 示例: em.create_table('my_table', df, overwrite=True)

  2. add_columns(table_name, dataframe, merge_on_index=True)  
     └─ 在已存在的表中添加新列（自动屏蔽已存在的列）
     └─ 示例: em.add_columns('my_table', df_new_cols)

  3. insert_data(table_name, dataframe, mode='skip')  
     └─ 插入数据，支持三种模式：
        • skip   - 忽略重复索引（默认）
        • update - 覆盖重复索引的数据
        • append - 直接追加，不检查重复
     └─ 示例: em.insert_data('my_table', df, mode='skip')

  4. load_table(table_name, limit=None)
     └─ 从数据库导入表到 Python
     └─ 示例: df = em.load_table('my_table', limit=100)

  5. drop_table(table_name)
     └─ 删除表
     └─ 示例: em.drop_table('my_table')

  6. list_tables(schema='public', verbose=False, pattern=None)  ⭐ 升级版
     └─ 列出所有表（支持详细模式和过滤）
     └─ 示例: em.list_tables(pattern='stock%', verbose=True, print_table=True)

  7. get_table_info(table_name, print_info=False)  ⭐ 升级版
     └─ 获取表详细信息（列、行数、大小、主键、索引）
     └─ 示例: em.get_table_info('my_table', print_info=True)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 三种插入模式详解：

  ┌─────────┬──────────┬──────────┬──────────┬───────────────────┐
  │  模式   │ 检查方式 │ 需要索引 │   性能   │     适用场景      │
  ├─────────┼──────────┼──────────┼──────────┼───────────────────┤
  │ skip    │ 基于索引 │   ✅     │   中等   │ 增量更新，避免重复│
  │ update  │ 基于索引 │   ✅     │   较慢   │ 数据修正，UPSERT  │
  │ append  │ 不检查   │   ❌     │   最快   │ 快速批量导入      │
  └─────────┴──────────┴──────────┴──────────┴───────────────────┘

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 快速示例：

  # 1. 连接数据库
  from easy_manager import EasyManager
  import pandas as pd
  
  with EasyManager() as em:
      
      # 2. 创建表
      df = pd.read_csv('data.csv', index_col=0)
      em.create_table('stocks', df)
      
      # 3. 添加新列
      df_new = pd.read_csv('new_factors.csv', index_col=0)
      em.add_columns('stocks', df_new)
      
      # 4. 插入数据（三种模式）
      em.insert_data('stocks', df, mode='skip')    # 忽略重复索引
      em.insert_data('stocks', df, mode='update')  # 覆盖重复数据
      em.insert_data('stocks', df, mode='append')  # 直接追加
      
      # 5. 导入表
      df_loaded = em.load_table('stocks')
      
      # 6. 查询表信息
      tables = em.list_tables()
      info = em.get_table_info('stocks')
      
      # 7. 删除表
      em.drop_table('stocks')

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️  注意事项：

  • skip 和 update 模式需要 DataFrame 有索引列
  • 列名中的特殊字符（., -, 空格）会自动转换为 _
  • 所有操作记录在 datadeal.log 文件中

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📖 更多信息：

  • 完整手册：EasyManager完整使用手册.md
  • 测试示例：test_new_features.py
  • 在线帮助：EasyManager.help()

╚════════════════════════════════════════════════════════════════════════════╝
        """
        print(help_text)
    
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


class LongManager(EasyManager):
    """
    长格式数据管理器（Panel Data Manager）
    
    专门用于处理长格式面板数据，特点：
    1. 数据格式：每行是一个公司在某时间点的观测
    2. 复合键：使用（时间，公司）作为唯一标识
    3. 列顺序：时间列在第一列，公司列在第二列
    4. 索引：使用自增序列，而不是时间索引
    
    适用场景：
    - 多公司多时间点的因子数据
    - Panel Data 分析
    - 时间序列横截面数据
    """
    
    def __init__(self, 
                 database: str = 'test_data_base',
                 user: str = 'postgres',
                 password: str = 'cbw88982449',
                 host: str = 'localhost',
                 port: int = 5432,
                 time_col: str = 'datetime',
                 entity_col: str = 'company'):
        """
        初始化长格式数据管理器
        
        Args:
            database: 数据库名
            user: 用户名
            password: 密码
            host: 主机地址
            port: 端口
            time_col: 时间列名（默认：'datetime'）
            entity_col: 实体列名（默认：'company'）
        """
        super().__init__(database, user, password, host, port)
        self.time_col = time_col
        self.entity_col = entity_col
        self.logger.info(f"LongManager 初始化完成，复合键：({time_col}, {entity_col})")
    
    @function_timer
    def create_table(self, table_name: str, dataframe: pd.DataFrame, 
                     overwrite: bool = False) -> bool:
        """
        创建长格式数据表
        
        特点：
        1. 确保时间列在第一列，公司列在第二列
        2. 不使用这两列作为索引，使用自增序列
        3. 自动检查和调整列顺序
        
        Args:
            table_name: 表名
            dataframe: DataFrame（必须包含时间列和实体列）
            overwrite: 是否覆盖已存在的表
            
        Returns:
            bool: 是否成功
        """
        self._ensure_connection()
        
        try:
            # 检查必需的列
            if self.time_col not in dataframe.columns:
                self.logger.error(f"DataFrame 缺少时间列: {self.time_col}")
                return False
            
            if self.entity_col not in dataframe.columns:
                self.logger.error(f"DataFrame 缺少实体列: {self.entity_col}")
                return False
            
            # 重置索引（如果有）
            df = dataframe.copy()
            if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
                df = df.reset_index(drop=True)
            
            # 调整列顺序：时间列第一，公司列第二，其他列保持顺序
            other_cols = [col for col in df.columns 
                         if col not in [self.time_col, self.entity_col]]
            df = df[[self.time_col, self.entity_col] + other_cols]
            
            # 确保时间列是 datetime 类型
            if not pd.api.types.is_datetime64_any_dtype(df[self.time_col]):
                self.logger.warning(f"时间列 {self.time_col} 不是 datetime 类型，正在转换...")
                df[self.time_col] = pd.to_datetime(df[self.time_col])
            
            # 检查是否有重复的（时间，公司）组合
            duplicates = df.duplicated(subset=[self.time_col, self.entity_col], keep=False)
            if duplicates.any():
                dup_count = duplicates.sum()
                self.logger.warning(f"发现 {dup_count} 个重复的 ({self.time_col}, {self.entity_col}) 组合")
                self.logger.warning("将保留第一次出现的记录")
                df = df.drop_duplicates(subset=[self.time_col, self.entity_col], keep='first')
            
            # 检查表是否存在
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name.split('.')[-1],))
            
            table_exists = self.cursor.fetchone()['exists']
            
            if table_exists and not overwrite:
                self.logger.warning(f"表 {table_name} 已存在，使用 overwrite=True 来覆盖")
                return False
            
            if table_exists and overwrite:
                self.logger.info(f"删除已存在的表 {table_name}")
                self.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                self.conn.commit()
            
            # 清理列名
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            # 创建表结构（添加自增主键）
            columns_def = ['id SERIAL PRIMARY KEY']
            
            for column in df.columns:
                col_type = self._infer_column_type(df[column])
                nullable = "NULL" if df[column].isnull().any() else "NOT NULL"
                columns_def.append(f'"{column}" {col_type} {nullable}')
            
            create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns_def)})'
            
            self.logger.info(f"创建表 {table_name}，列数: {len(df.columns)}")
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            
            # 在（时间，公司）列上创建复合索引，提高查询性能
            index_name = f"{table_name}_{self.time_col}_{self.entity_col}_idx"
            create_index_sql = f'''
                CREATE INDEX "{index_name}" 
                ON "{table_name}" ("{self.time_col}", "{self.entity_col}")
            '''
            self.cursor.execute(create_index_sql)
            self.conn.commit()
            self.logger.info(f"已创建复合索引: {index_name}")
            
            # 插入数据
            self._insert_dataframe(table_name, df)
            
            self.logger.info(f"成功创建表 {table_name}，插入 {len(df)} 行数据")
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"创建表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    @function_timer
    def insert_data(self, table_name: str, dataframe: pd.DataFrame, 
                    mode: str = 'skip') -> bool:
        """
        插入长格式数据
        
        基于（时间，公司）复合键判断重复：
        - skip: 忽略重复的（时间，公司）组合
        - update: 更新重复的（时间，公司）组合
        - append: 直接追加，不检查重复
        
        Args:
            table_name: 表名
            dataframe: DataFrame
            mode: 'skip', 'update', 'append'
            
        Returns:
            bool: 是否成功
        """
        self._ensure_connection()
        
        if mode not in ['skip', 'update', 'append']:
            self.logger.error(f"不支持的模式: {mode}")
            return False
        
        try:
            # 检查必需的列
            if self.time_col not in dataframe.columns:
                self.logger.error(f"DataFrame 缺少时间列: {self.time_col}")
                return False
            
            if self.entity_col not in dataframe.columns:
                self.logger.error(f"DataFrame 缺少实体列: {self.entity_col}")
                return False
            
            # 准备数据
            df = dataframe.copy()
            if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
                df = df.reset_index(drop=True)
            
            # 确保列顺序
            other_cols = [col for col in df.columns 
                         if col not in [self.time_col, self.entity_col]]
            df = df[[self.time_col, self.entity_col] + other_cols]
            
            # 确保时间列是 datetime 类型
            if not pd.api.types.is_datetime64_any_dtype(df[self.time_col]):
                df[self.time_col] = pd.to_datetime(df[self.time_col])
            
            # 清理列名
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            # append 模式：直接插入
            if mode == 'append':
                self._insert_dataframe(table_name, df)
                self.logger.info(f"append 模式：插入 {len(df)} 行数据")
                return True
            
            # skip 和 update 模式：需要检查重复
            # 加载现有数据的（时间，公司）组合
            query = f'''
                SELECT "{self.time_col}", "{self.entity_col}"
                FROM "{table_name}"
            '''
            existing_df = pd.read_sql(query, self.conn)
            existing_df[self.time_col] = pd.to_datetime(existing_df[self.time_col])
            
            # 创建复合键
            existing_keys = set(
                zip(existing_df[self.time_col], existing_df[self.entity_col])
            )
            df_keys = list(zip(df[self.time_col], df[self.entity_col]))
            
            if mode == 'skip':
                # skip 模式：只插入新的（时间，公司）组合
                mask = [key not in existing_keys for key in df_keys]
                df_to_insert = df[mask].copy()
                
                if len(df_to_insert) == 0:
                    self.logger.info("skip 模式：所有数据都已存在，无需插入")
                    return True
                
                self._insert_dataframe(table_name, df_to_insert)
                skipped = len(df) - len(df_to_insert)
                self.logger.info(
                    f"skip 模式：插入 {len(df_to_insert)} 行新数据，"
                    f"跳过 {skipped} 行重复数据"
                )
                return True
            
            elif mode == 'update':
                # update 模式：更新已存在的，插入新的
                mask_update = [key in existing_keys for key in df_keys]
                mask_insert = [key not in existing_keys for key in df_keys]
                
                df_to_update = df[mask_update].copy()
                df_to_insert = df[mask_insert].copy()
                
                # 插入新数据
                if len(df_to_insert) > 0:
                    self._insert_dataframe(table_name, df_to_insert)
                    self.logger.info(f"插入 {len(df_to_insert)} 行新数据")
                
                # 更新已存在的数据
                if len(df_to_update) > 0:
                    update_count = 0
                    columns = [col for col in df_to_update.columns 
                              if col not in [self.time_col, self.entity_col]]
                    
                    for _, row in df_to_update.iterrows():
                        set_clause = ', '.join([f'"{col}" = %s' for col in columns])
                        update_sql = f'''
                            UPDATE "{table_name}"
                            SET {set_clause}
                            WHERE "{self.time_col}" = %s AND "{self.entity_col}" = %s
                        '''
                        
                        values = [None if pd.isna(row[col]) else row[col] 
                                 for col in columns]
                        values.extend([row[self.time_col], row[self.entity_col]])
                        
                        self.cursor.execute(update_sql, values)
                        if self.cursor.rowcount > 0:
                            update_count += 1
                    
                    self.conn.commit()
                    self.logger.info(f"更新 {update_count} 行数据")
                
                return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"插入数据到表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            self.logger.error("请优先检查数据格式问题,注:所有的时间格式都需要pd.to_datetime后方可录入")
            return False
    
    @function_timer
    def add_columns(self, table_name: str, dataframe: pd.DataFrame, 
                    merge_on_keys: bool = True) -> bool:
        """
        向长格式表添加新列
        
        基于（时间，公司）复合键合并数据
        
        Args:
            table_name: 表名
            dataframe: 包含新列的 DataFrame
            merge_on_keys: 是否基于（时间，公司）合并数据
            
        Returns:
            bool: 是否成功
        """
        self._ensure_connection()
        
        try:
            # 检查必需的列
            if self.time_col not in dataframe.columns:
                self.logger.error(f"DataFrame 缺少时间列: {self.time_col}")
                return False
            
            if self.entity_col not in dataframe.columns:
                self.logger.error(f"DataFrame 缺少实体列: {self.entity_col}")
                return False
            
            # 准备数据
            df = dataframe.copy()
            if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
                df = df.reset_index(drop=True)
            
            # 确保时间列是 datetime 类型
            if not pd.api.types.is_datetime64_any_dtype(df[self.time_col]):
                df[self.time_col] = pd.to_datetime(df[self.time_col])
            
            # 清理列名
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            # 获取现有列
            existing_columns = self._get_table_columns(table_name)
            
            # 识别新列（排除时间列和实体列）
            new_columns = [col for col in df.columns 
                          if col not in existing_columns 
                          and col not in [self.time_col, self.entity_col]]
            
            if not new_columns:
                self.logger.info("没有需要添加的新列")
                return True
            
            self.logger.info(f"准备添加 {len(new_columns)} 个新列: {new_columns}")
            
            # 添加新列到表结构
            for column in new_columns:
                col_type = self._infer_column_type(df[column])
                alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" {col_type}'
                self.cursor.execute(alter_sql)
                self.logger.info(f"添加列: {column} ({col_type})")
            
            self.conn.commit()
            
            # 如果需要合并数据
            if merge_on_keys and new_columns:
                self.logger.info("开始基于（时间，公司）键合并数据...")
                
                # 加载现有数据
                query = f'SELECT "{self.time_col}", "{self.entity_col}" FROM "{table_name}"'
                existing_df = pd.read_sql(query, self.conn)
                existing_df[self.time_col] = pd.to_datetime(existing_df[self.time_col])
                
                # 创建键集合
                existing_keys = set(
                    zip(existing_df[self.time_col], existing_df[self.entity_col])
                )
                
                update_count = 0
                for _, row in df.iterrows():
                    key = (row[self.time_col], row[self.entity_col])
                    
                    if key in existing_keys:
                        set_clause = ', '.join([f'"{col}" = %s' for col in new_columns])
                        update_sql = f'''
                            UPDATE "{table_name}"
                            SET {set_clause}
                            WHERE "{self.time_col}" = %s AND "{self.entity_col}" = %s
                        '''
                        
                        values = [None if pd.isna(row[col]) else row[col] 
                                 for col in new_columns]
                        values.extend([row[self.time_col], row[self.entity_col]])
                        
                        self.cursor.execute(update_sql, values)
                        if self.cursor.rowcount > 0:
                            update_count += 1
                
                self.conn.commit()
                self.logger.info(f"成功更新 {update_count} 行的新列数据")
            
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"添加列到表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            self.logger.error("请优先检查数据格式问题,注:所有的时间格式都需要pd.to_datetime后方可录入")
            return False
    
    @staticmethod
    def help():
        """显示 LongManager 的帮助信息"""
        help_text = """
╔════════════════════════════════════════════════════════════════════════════╗
║                    LongManager 使用帮助 (v1.0)                             ║
║                   长格式（Panel Data）数据管理器                            ║
╚════════════════════════════════════════════════════════════════════════════╝

[核心特点]

  - 专门处理长格式面板数据（Panel Data）
  - 使用（时间，公司）作为复合键判断唯一性
  - 时间列在第一列，公司列在第二列
  - 使用自增序列作为主键，而非时间索引

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[数据格式]

  长格式数据的特点：
  - 每行是一个公司在某时间点的观测
  - 同一时间有多个公司
  - 同一公司有多个时间点

  示例：
  +------------+---------+----------+----------+-----+
  |  datetime  | company | factor_A | factor_B | ... |
  +------------+---------+----------+----------+-----+
  | 2020-01-01 |  AAPL   |   25.3   |   0.15   | ... |
  | 2020-01-01 |  GOOGL  |   28.7   |   0.18   | ... |
  | 2020-01-02 |  AAPL   |   25.5   |   0.16   | ... |
  | 2020-01-02 |  GOOGL  |   28.9   |   0.19   | ... |
  +------------+---------+----------+----------+-----+

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[主要功能]

  1. create_table(table_name, dataframe, overwrite=False)
     - 创建长格式数据表
     - 自动调整列顺序（时间列第一，公司列第二）
     - 自动创建复合索引提高查询性能

  2. insert_data(table_name, dataframe, mode='skip')
     - 基于（时间，公司）判断重复
     - skip: 忽略重复键
     - update: 更新重复键
     - append: 直接追加

  3. add_columns(table_name, dataframe, merge_on_keys=True)
     - 添加新因子列
     - 基于（时间，公司）合并数据

  4. 继承 EasyManager 的所有其他功能
     - load_table, drop_table, list_tables, get_table_info

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[快速示例]

  from easy_manager import LongManager
  import pandas as pd

  # 1. 初始化（可自定义时间列和实体列名）
  with LongManager(time_col='datetime', entity_col='company') as lm:
      
      # 2. 读取长格式数据
      df = pd.read_csv('long_data/full_factors.csv')
      df['datetime'] = pd.to_datetime(df['datetime'])
      
      # 3. 创建表（自动处理列顺序和索引）
      lm.create_table('factor_panel', df)
      
      # 4. 添加新因子列
      df_new = pd.read_csv('long_data/new_factors.csv')
      df_new['datetime'] = pd.to_datetime(df_new['datetime'])
      lm.add_columns('factor_panel', df_new)
      
      # 5. 插入增量数据（基于时间-公司键去重）
      df_new_data = pd.read_csv('long_data/incremental.csv')
      df_new_data['datetime'] = pd.to_datetime(df_new_data['datetime'])
      lm.insert_data('factor_panel', df_new_data, mode='skip')
      
      # 6. 查看表信息
      lm.get_table_info('factor_panel', print_info=True)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[重要提示]

  - 时间列必须命名为 'datetime'（或自定义）
  - 公司列必须命名为 'company'（或自定义）
  - 时间列必须是 pd.to_datetime() 转换后的格式
  - 重复判断基于（时间，公司）组合，而非单一索引

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[更多信息]

  - EasyManager 完整手册：EasyManager完整使用手册.md
  - 长格式数据说明：long_data/README.md
  - 导入示例：long_data/import_example.py

╚════════════════════════════════════════════════════════════════════════════╝
        """
        print(help_text)


# 使用示例
if __name__ == "__main__":
    # 创建管理器实例
    with EasyManager() as em:
        # 列出所有表
        tables = em.list_tables()
        print("现有表:", tables)