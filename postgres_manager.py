"""
PostgreSQL数据管理系统
用于权益因子分析师的数据存储和管理

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


class PostgreSQLManager:
    """
    PostgreSQL数据管理类
    支持创建表格、导入数据、查询、更新、删除等操作
    """
    
    def __init__(self, 
                 database: str = "datafeed",
                 user: str = "postgres", 
                 password: str = "123456",
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
            self.logger.info("数据库连接成功")
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
            self.cursor.execute(f"DROP TABLE {table_name}")
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
    def create_table(self, table_name: str, data_source: Union[str, pd.DataFrame] = None, 
                     overwrite: bool = False) -> bool:
        """
        创建新表格，支持从文件或DataFrame导入历史数据
        
        Args:
            table_name: 表名
            data_source: 数据源，可以是文件路径或DataFrame
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
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.logger.info(f"已删除现有表 {table_name}")
            
            # 如果提供了数据源，根据数据结构创建表
            if data_source is not None:
                df = self._load_data(data_source)
                if df is not None:
                    self._create_table_from_dataframe(table_name, df)
                    self._insert_dataframe(table_name, df)
                    self.logger.info(f"表 {table_name} 创建成功，导入了 {len(df)} 行数据")
                else:
                    return False
            else:
                # 创建标准的宽表格式表结构
                self._create_standard_table(table_name)
                self.logger.info(f"标准表 {table_name} 创建成功")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"创建表 {table_name} 失败: {str(e)}")
            import traceback
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _load_data(self, data_source: Union[str, pd.DataFrame, dict]) -> Optional[pd.DataFrame]:
        """
        加载数据从各种源
        
        Args:
            data_source: 数据源，支持以下类型：
                - str: 文件路径 (CSV, Excel, JSON)
                - pd.DataFrame: pandas DataFrame对象
                - dict: 字典数据，将转换为DataFrame
            
        Returns:
            DataFrame或None
        """
        if isinstance(data_source, pd.DataFrame):
            self.logger.info(f"直接使用传入的DataFrame，形状: {data_source.shape}")
            df = data_source.copy()
        elif isinstance(data_source, dict):
            try:
                df = pd.DataFrame(data_source)
                self.logger.info(f"从字典创建DataFrame，形状: {df.shape}")
            except Exception as e:
                self.logger.error(f"无法从字典创建DataFrame: {str(e)}")
                return None
        elif isinstance(data_source, str):
            file_path = Path(data_source)
            if not file_path.exists():
                self.logger.error(f"文件不存在: {data_source}")
                return None
            
            try:
                if file_path.suffix.lower() == '.csv':
                    df = pd.read_csv(data_source, index_col=0)
                elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                    df = pd.read_excel(data_source, index_col=0)
                elif file_path.suffix.lower() == '.json':
                    df = pd.read_json(data_source)
                else:
                    self.logger.error(f"不支持的文件格式: {file_path.suffix}")
                    return None
                
                self.logger.info(f"成功加载数据文件 {data_source}, 形状: {df.shape}")
            except Exception as e:
                self.logger.error(f"加载数据文件失败 {data_source}: {str(e)}")
                return None
        else:
            self.logger.error(f"不支持的数据源类型: {type(data_source)}")
            return None
        
        # 处理重复列名
        if df.columns.duplicated().any():
            self.logger.warning("发现重复列名，正在处理...")
            # 为重复列名添加后缀
            cols = df.columns.tolist()
            seen = {}
            new_cols = []
            
            for col in cols:
                if col in seen:
                    seen[col] += 1
                    new_cols.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    new_cols.append(col)
            
            df.columns = new_cols
            self.logger.info(f"重复列名已处理，新列名: {df.columns.tolist()}")
        
        # 确保索引是datetime类型
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                self.logger.error(f"无法将索引转换为日期时间格式: {str(e)}")
                return None
        
        # 排序索引
        df = df.sort_index()
        
        return df
    
    def _create_table_from_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        根据DataFrame结构创建表
        
        Args:
            table_name: 表名
            df: DataFrame
        """
        # 创建表结构
        columns_sql = ["datetime TIMESTAMP PRIMARY KEY"]
        
        for col in df.columns:
            # 假设所有数据列都是数值型
            columns_sql.append(f'"{col}" DOUBLE PRECISION')
        
        create_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns_sql)}
            )
        """
        
        self.cursor.execute(create_sql)
        self.logger.info(f"根据DataFrame创建表结构: {table_name}")
    
    def _create_standard_table(self, table_name: str):
        """
        创建标准表结构
        
        Args:
            table_name: 表名
        """
        create_sql = f"""
            CREATE TABLE {table_name} (
                datetime TIMESTAMP,
                code VARCHAR(20),
                metric VARCHAR(100),
                value DOUBLE PRECISION,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (datetime, code, metric)
            )
        """
        
        self.cursor.execute(create_sql)
        self.logger.info(f"创建标准表结构: {table_name}")
    
    def _insert_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        将DataFrame插入到表中
        
        Args:
            table_name: 表名
            df: 要插入的DataFrame
        """
        from datetime import datetime
        import pandas as pd
        
        # 记录开始导入时间
        start_time = datetime.now()
        self.logger.info(f"数据开始导入表 {table_name}")
        
        # 准备数据并按时间分组进行详细日志记录
        data_tuples = []
        time_groups = {}
        
        # 分析数据的时间范围和频率
        if not df.empty:
            # 获取时间索引的信息
            time_index = df.index
            if hasattr(time_index, 'to_pydatetime'):
                time_values = time_index.to_pydatetime()
            else:
                time_values = pd.to_datetime(time_index).to_pydatetime()
            
            # 根据数据密度决定日志粒度
            time_span = max(time_values) - min(time_values)
            total_records = len(df)
            
            if time_span.days > 365:  # 超过一年的数据，按月记录
                log_frequency = 'monthly'
                self.logger.info(f"检测到跨度 {time_span.days} 天的数据，将按月显示导入进度")
            elif time_span.days > 30:  # 超过一个月的数据，按周记录
                log_frequency = 'weekly'
                self.logger.info(f"检测到跨度 {time_span.days} 天的数据，将按周显示导入进度")
            elif time_span.days > 1:  # 超过一天的数据，按天记录
                log_frequency = 'daily'
                self.logger.info(f"检测到跨度 {time_span.days} 天的数据，将按天显示导入进度")
            else:  # 一天内的数据，按小时记录
                log_frequency = 'hourly'
                self.logger.info(f"检测到当日数据，将按小时显示导入进度")
        
        # 处理数据并分组记录
        processed_count = 0
        current_period = None
        period_count = 0
        
        for idx, row in df.iterrows():
            # 将datetime索引和行数据组合
            row_data = [idx] + [None if pd.isna(val) else float(val) for val in row.values]
            data_tuples.append(tuple(row_data))
            processed_count += 1
            
            # 确定当前时间点的周期
            if hasattr(idx, 'strftime'):
                current_time = idx
            else:
                current_time = pd.to_datetime(idx)
            
            if log_frequency == 'monthly':
                period_key = current_time.strftime('%Y-%m')
                period_desc = current_time.strftime('%Y年%m月')
            elif log_frequency == 'weekly':
                # 计算周数
                week_num = current_time.isocalendar()[1]
                period_key = f"{current_time.year}-W{week_num:02d}"
                period_desc = f"{current_time.year}年第{week_num}周"
            elif log_frequency == 'daily':
                period_key = current_time.strftime('%Y-%m-%d')
                period_desc = current_time.strftime('%Y年%m月%d日')
            else:  # hourly
                period_key = current_time.strftime('%Y-%m-%d %H')
                period_desc = current_time.strftime('%Y年%m月%d日 %H时')
            
            # 如果进入新的时间周期，记录上一周期的完成情况
            if current_period is None:
                current_period = period_key
                period_count = 1
            elif current_period != period_key:
                # 记录上一周期完成
                prev_desc = period_desc if log_frequency == 'monthly' else (
                    current_time.strftime('%Y年%m月') if log_frequency == 'weekly' else (
                        current_time.strftime('%Y年%m月%d日') if log_frequency == 'daily' else
                        current_time.strftime('%Y年%m月%d日 %H时')
                    )
                )
                
                # 获取因子名称（表名去掉前缀）
                factor_name = table_name.replace('fundamental_', '').replace('price_', '').replace('technical_', '')
                
                self.logger.info(f"当前时间 {start_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                               f"导入时间点 {current_time.strftime('%Y-%m-%d')} "
                               f"因子 {factor_name} 导入成功")
                
                current_period = period_key
                period_count = 1
            else:
                period_count += 1
        
        # 记录最后一个周期
        if current_period and processed_count > 0:
            factor_name = table_name.replace('fundamental_', '').replace('price_', '').replace('technical_', '')
            last_time = df.index[-1] if not df.empty else start_time
            if hasattr(last_time, 'strftime'):
                last_time_str = last_time.strftime('%Y-%m-%d')
            else:
                last_time_str = pd.to_datetime(last_time).strftime('%Y-%m-%d')
            
            self.logger.info(f"当前时间 {start_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                           f"导入时间点 {last_time_str} "
                           f"因子 {factor_name} 导入成功")
        
        # 构建列名列表
        columns = ['datetime'] + [f'"{col}"' for col in df.columns]
        
        # 构建INSERT语句 - 使用ON CONFLICT处理重复键
        placeholders = ', '.join(['%s'] * len(columns))
        update_cols = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns])
        
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES ({placeholders})
            ON CONFLICT (datetime) DO UPDATE SET {update_cols}
        """
        
        # 批量插入数据
        import psycopg2.extras
        psycopg2.extras.execute_batch(
            self.cursor, insert_sql, data_tuples, page_size=1000
        )
        
        # 记录最终完成信息
        end_time = datetime.now()
        self.logger.info(f"数据成功导入表 {table_name}, 共 {len(data_tuples)} 行, "
                        f"耗时 {(end_time - start_time).total_seconds():.2f} 秒")
    
    @function_timer
    def insert_data(self, table_name: str, data_source: Union[str, pd.DataFrame, dict], 
                    update_existing: bool = True) -> bool:
        """
        向表中插入新数据
        
        Args:
            table_name: 表名
            data_source: 数据源
            update_existing: 是否更新已存在的数据
            
        Returns:
            bool: 插入是否成功
        """
        self._ensure_connection()
        
        try:
            df = self._load_data(data_source)
            if df is None:
                return False
            
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
            
            # 获取表结构
            self.cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name.split('.')[-1],))
            
            table_columns = {row['column_name']: row['data_type'] for row in self.cursor.fetchall()}
            print('table_columns')
            print(table_columns)
            # 检查DataFrame列是否匹配表结构
            missing_cols = set(df.columns) - set(table_columns.keys())
            if missing_cols:
                self.logger.warning(f"DataFrame中的列 {missing_cols} 在表中不存在，将被忽略")
                df = df.drop(columns=missing_cols)
            
            if len(df.columns) == 0:
                self.logger.error("没有匹配的列可以插入")
                return False
            
            self._insert_dataframe(table_name, df)
            self.conn.commit()
            
            self.logger.info(f"成功向表 {table_name} 插入数据")
            return True
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            self.logger.error(f"插入数据到表 {table_name} 失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    @function_timer
    def query_data(self, table_name: str, 
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None,
                   codes: Optional[List[str]] = None,
                   columns: Optional[List[str]] = None,
                   limit: Optional[int] = None,
                   return_format: str = 'pandas') -> Union[pd.DataFrame, np.ndarray, None]:
        """
        查询数据
        
        Args:
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期
            codes: 代码列表
            columns: 列名列表
            limit: 限制返回行数
            return_format: 返回格式 ('pandas', 'numpy')
            
        Returns:
            查询结果
        """
        self._ensure_connection()
        
        try:
            # 构建查询条件
            conditions = []
            params = []
            
            if start_date:
                conditions.append("datetime >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("datetime <= %s")
                params.append(end_date)
            
            # 构建SELECT子句
            if columns:
                select_cols = ['datetime'] + [f'"{col}"' for col in columns if col != 'datetime']
            else:
                select_cols = ['*']
            
            # 构建查询SQL
            query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY datetime"
            
            # 添加LIMIT子句
            if limit:
                query += f" LIMIT {limit}"
            
            self.logger.info(f"执行查询: {query}")
            self.cursor.execute(query, params)
            
            # 获取结果
            results = self.cursor.fetchall()
            
            if not results:
                self.logger.warning("查询结果为空")
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            df.set_index('datetime', inplace=True)
            df.index = pd.to_datetime(df.index)
            
            # 如果指定了codes，进行过滤（适用于宽表格式）
            if codes and len(df.columns) > 1:
                available_codes = [col for col in codes if col in df.columns]
                if available_codes:
                    df = df[available_codes]
                else:
                    self.logger.warning(f"指定的代码 {codes} 在表中不存在")
            
            self.logger.info(f"查询完成，返回数据形状: {df.shape}")
            
            if return_format == 'numpy':
                return df.values
            else:
                return df
                
        except Exception as e:
            self.logger.error(f"查询数据失败: {str(e)}")
            return None
    
    @function_timer
    def query_data_multifactor(self, table_names: List[str], 
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              codes: Optional[List[str]] = None,
                              return_format: str = 'pandas') -> Union[pd.DataFrame, None]:
        """
        多因子查询功能，将多个因子表合并为长格式数据
        
        Args:
            table_names: 因子表名列表
            start_date: 开始日期
            end_date: 结束日期  
            codes: 股票代码列表
            return_format: 返回格式 ('pandas')
            
        Returns:
            包含date, code, factors, values列的长格式DataFrame
        """
        self._ensure_connection()
        
        try:
            if not table_names:
                self.logger.error("必须指定至少一个表名")
                return None
            
            # 构建时间条件
            time_conditions = []
            time_params = []
            
            if start_date:
                time_conditions.append("datetime >= %s")
                time_params.append(start_date)
            
            if end_date:
                time_conditions.append("datetime <= %s")
                time_params.append(end_date)
            
            time_where = ""
            if time_conditions:
                time_where = " WHERE " + " AND ".join(time_conditions)
            
            # 为每个表构建UNION查询
            union_queries = []
            all_params = []
            
            for table_name in table_names:
                self.logger.info(f"处理因子表: {table_name}")
                
                # 检查表是否存在
                self.cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """, (table_name,))
                
                if not self.cursor.fetchone()['exists']:
                    self.logger.warning(f"表 {table_name} 不存在，跳过")
                    continue
                
                # 获取表的列信息（排除datetime列）
                self.cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND column_name != 'datetime'
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = [row['column_name'] for row in self.cursor.fetchall()]
                
                if not columns:
                    self.logger.warning(f"表 {table_name} 没有数据列，跳过")
                    continue
                
                # 如果指定了codes，只选择匹配的列
                if codes:
                    available_codes = [col for col in columns if col in codes]
                    if not available_codes:
                        self.logger.warning(f"表 {table_name} 中没有找到指定的股票代码，跳过")
                        continue
                    columns = available_codes
                
                # 为每个股票代码列创建一个SELECT语句
                for code in columns:
                    # 使用UNPIVOT的替代方案：为每个列创建单独的查询
                    query = f"""
                        SELECT 
                            datetime as date,
                            '{code}' as code,
                            '{table_name}' as factors,
                            "{code}" as values
                        FROM {table_name}
                        {time_where}
                        AND "{code}" IS NOT NULL
                    """
                    union_queries.append(query)
                    all_params.extend(time_params)
            
            if not union_queries:
                self.logger.error("没有有效的表或列可以查询")
                return None
            
            # 合并所有查询
            final_query = " UNION ALL ".join(union_queries) + " ORDER BY date, code, factors"
            
            self.logger.info(f"执行多因子查询，涉及 {len(table_names)} 个因子表")
            self.logger.info(f"查询语句长度: {len(final_query)} 字符")
            
            # 执行查询
            self.cursor.execute(final_query, all_params)
            results = self.cursor.fetchall()
            
            if not results:
                self.logger.warning("多因子查询结果为空")
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            df['date'] = pd.to_datetime(df['date'])
            
            # 确保values列为数值类型
            df['values'] = pd.to_numeric(df['values'], errors='coerce')
            
            # 移除NaN值
            df = df.dropna(subset=['values'])
            
            self.logger.info(f"多因子查询完成，返回数据形状: {df.shape}")
            self.logger.info(f"包含因子: {df['factors'].unique().tolist()}")
            self.logger.info(f"包含股票代码: {len(df['code'].unique())} 个")
            self.logger.info(f"时间范围: {df['date'].min()} 到 {df['date'].max()}")
            
            return df
                
        except Exception as e:
            import traceback
            self.logger.error(f"多因子查询失败: {str(e)}")
            self.logger.error(f"详细错误信息: {traceback.format_exc()}")
            return None
    
    @function_timer
    def delete_data(self, table_name: str, 
                    start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    codes: Optional[List[str]] = None) -> bool:
        """
        删除数据
        
        Args:
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期
            codes: 代码列表
            
        Returns:
            bool: 删除是否成功
        """
        self._ensure_connection()
        
        try:
            conditions = []
            params = []
            
            if start_date:
                conditions.append("datetime >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("datetime <= %s")
                params.append(end_date)
            
            if codes:
                # 对于标准表格式
                conditions.append("code = ANY(%s)")
                params.append(codes)
            
            if not conditions:
                self.logger.error("删除操作必须指定至少一个条件")
                return False
            
            delete_sql = f"DELETE FROM {table_name} WHERE " + " AND ".join(conditions)
            
            self.cursor.execute(delete_sql, params)
            deleted_rows = self.cursor.rowcount
            
            self.conn.commit()
            self.logger.info(f"成功删除 {deleted_rows} 行数据")
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"删除数据失败: {str(e)}")
            return False
    
    @function_timer
    def time_slice(self, table_name: str, start_date: str, end_date: str,
                   return_format: str = 'pandas') -> Union[pd.DataFrame, np.ndarray, None]:
        """
        时间切片功能
        
        Args:
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期
            return_format: 返回格式
            
        Returns:
            切片结果
        """
        return self.query_data(table_name, start_date=start_date, end_date=end_date, 
                              return_format=return_format)
    
    @function_timer
    def merge_tables(self, table1: str, table2: str, join_type: str = 'inner',
                     on_column: str = 'datetime') -> Optional[pd.DataFrame]:
        """
        关联合并两个表
        
        Args:
            table1: 第一个表名
            table2: 第二个表名
            join_type: 连接类型 ('inner', 'left', 'right', 'outer')
            on_column: 连接列
            
        Returns:
            合并结果
        """
        self._ensure_connection()
        
        try:
            df1 = self.query_data(table1)
            df2 = self.query_data(table2)
            
            if df1 is None or df2 is None:
                self.logger.error("无法获取表数据进行合并")
                return None
            
            # 执行合并
            if join_type == 'inner':
                result = pd.merge(df1, df2, left_index=True, right_index=True, how='inner')
            elif join_type == 'left':
                result = pd.merge(df1, df2, left_index=True, right_index=True, how='left')
            elif join_type == 'right':
                result = pd.merge(df1, df2, left_index=True, right_index=True, how='right')
            elif join_type == 'outer':
                result = pd.merge(df1, df2, left_index=True, right_index=True, how='outer')
            else:
                self.logger.error(f"不支持的连接类型: {join_type}")
                return None
            
            self.logger.info(f"成功合并表 {table1} 和 {table2}，结果形状: {result.shape}")
            return result
            
        except Exception as e:
            self.logger.error(f"合并表失败: {str(e)}")
            return None
    
    def list_tables(self) -> List[str]:
        """
        列出所有表
        
        Returns:
            表名列表
        """
        self._ensure_connection()
        
        try:
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            
            tables = [row['table_name'] for row in self.cursor.fetchall()]
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
            """, (table_name,))
            
            columns = self.cursor.fetchall()
            
            # 获取行数
            self.cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
            row_count = self.cursor.fetchone()['row_count']
            
            return {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count
            }
            
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


if __name__ == "__main__":
    # 使用示例
    with PostgreSQLManager() as db:
        # 列出所有表
        tables = db.list_tables()
        print("现有表:", tables)