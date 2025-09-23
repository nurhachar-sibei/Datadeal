"""
高级PostgreSQL数据管理系统
扩展功能：批量更新、数据验证、性能优化等

Author: Nurhachar
Date: 2025
"""

import pandas as pd
import numpy as np
from typing import Union, List, Dict, Optional, Any, Tuple
from pathlib import Path
import json
from postgres_manager import PostgreSQLManager, function_timer
import logging

class AdvancedPostgreSQLManager(PostgreSQLManager):
    """
    高级PostgreSQL数据管理类
    继承基础管理类，添加更多高级功能
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
    
    @function_timer
    def batch_insert_files(self, file_directory: str, table_prefix: str = "", 
                          file_pattern: str = "*.csv", overwrite: bool = False) -> Dict[str, bool]:
        """
        批量导入文件夹中的文件
        
        Args:
            file_directory: 文件目录
            table_prefix: 表名前缀
            file_pattern: 文件匹配模式
            overwrite: 是否覆盖已存在的表
            
        Returns:
            Dict[str, bool]: 每个文件的导入结果
        """
        from datetime import datetime
        
        results = {}
        file_dir = Path(file_directory)
        
        if not file_dir.exists():
            self.logger.error(f"目录不存在: {file_directory}")
            return results
        
        files = list(file_dir.glob(file_pattern))
        batch_start_time = datetime.now()
        self.logger.info(f"批量导入开始: 发现 {len(files)} 个匹配文件")
        
        for i, file_path in enumerate(files, 1):
            table_name = f"{table_prefix}{file_path.stem.lower()}"
            file_start_time = datetime.now()
            
            self.logger.info(f"[{i}/{len(files)}] 开始处理文件: {file_path.name} -> 表: {table_name}")
            
            try:
                success = self.create_table(table_name, str(file_path), overwrite=overwrite)
                results[str(file_path)] = success
                
                if success:
                    info = self.get_table_info(table_name)
                    file_end_time = datetime.now()
                    duration = (file_end_time - file_start_time).total_seconds()
                    
                    self.logger.info(f"[{i}/{len(files)}] ✓ 文件 {file_path.name} 导入成功: "
                                   f"{info.get('row_count', 0)} 行数据, 耗时 {duration:.2f} 秒")
                else:
                    self.logger.warning(f"[{i}/{len(files)}] ✗ 文件 {file_path.name} 导入失败")
                    
            except Exception as e:
                self.logger.error(f"[{i}/{len(files)}] ✗ 处理文件 {file_path.name} 时出错: {str(e)}")
                results[str(file_path)] = False
        
        # 批量导入完成统计
        batch_end_time = datetime.now()
        total_duration = (batch_end_time - batch_start_time).total_seconds()
        success_count = sum(1 for success in results.values() if success)
        
        self.logger.info(f"批量导入完成: {success_count}/{len(files)} 个文件成功, "
                        f"总耗时 {total_duration:.2f} 秒, "
                        f"平均速度 {len(files)/total_duration:.2f} 个/秒")
        
        return results

    @function_timer
    def batch_insert_data(self, data_sources: List[Union[str, pd.DataFrame, dict]], 
                         table_names: List[str], overwrite: bool = False) -> Dict[str, bool]:
        """
        批量导入多个数据源（文件路径、DataFrame或字典）
        
        Args:
            data_sources: 数据源列表，支持文件路径、DataFrame或字典
            table_names: 对应的表名列表
            overwrite: 是否覆盖已存在的表
            
        Returns:
            Dict[str, bool]: 每个数据源的导入结果
        """
        from datetime import datetime
        
        results = {}
        
        if len(data_sources) != len(table_names):
            self.logger.error("数据源数量与表名数量不匹配")
            return results
        
        batch_start_time = datetime.now()
        self.logger.info(f"批量导入开始: 共 {len(data_sources)} 个数据源")
        
        for i, (data_source, table_name) in enumerate(zip(data_sources, table_names), 1):
            source_start_time = datetime.now()
            
            # 确定数据源类型和描述
            if isinstance(data_source, str):
                source_desc = f"文件: {Path(data_source).name}"
            elif isinstance(data_source, pd.DataFrame):
                source_desc = f"DataFrame: {data_source.shape}"
            elif isinstance(data_source, dict):
                source_desc = f"字典: {len(data_source)} 键"
            else:
                source_desc = f"未知类型: {type(data_source)}"
            
            self.logger.info(f"[{i}/{len(data_sources)}] 开始处理 {source_desc} -> 表: {table_name}")
            
            try:
                success = self.create_table(table_name, data_source, overwrite=overwrite)
                results[f"{source_desc}"] = success
                
                if success:
                    info = self.get_table_info(table_name)
                    source_end_time = datetime.now()
                    duration = (source_end_time - source_start_time).total_seconds()
                    
                    self.logger.info(f"[{i}/{len(data_sources)}] ✓ {source_desc} 导入成功: "
                                   f"{info.get('row_count', 0)} 行数据, 耗时 {duration:.2f} 秒")
                else:
                    self.logger.warning(f"[{i}/{len(data_sources)}] ✗ {source_desc} 导入失败")
                    
            except Exception as e:
                self.logger.error(f"[{i}/{len(data_sources)}] ✗ 处理 {source_desc} 时出错: {str(e)}")
                results[f"{source_desc}"] = False
        
        # 批量导入完成统计
        batch_end_time = datetime.now()
        total_duration = (batch_end_time - batch_start_time).total_seconds()
        success_count = sum(1 for success in results.values() if success)
        
        self.logger.info(f"批量导入完成: {success_count}/{len(data_sources)} 个数据源成功, "
                        f"总耗时 {total_duration:.2f} 秒, "
                        f"平均速度 {len(data_sources)/total_duration:.2f} 个/秒")
        
        return results
        
        files = list(file_dir.glob(file_pattern))
        batch_start_time = datetime.now()
        self.logger.info(f"批量导入开始: 发现 {len(files)} 个匹配文件")
        
        for i, file_path in enumerate(files, 1):
            table_name = f"{table_prefix}{file_path.stem.lower()}"
            file_start_time = datetime.now()
            
            self.logger.info(f"[{i}/{len(files)}] 开始处理文件: {file_path.name} -> 表: {table_name}")
            
            try:
                success = self.create_table(table_name, str(file_path), overwrite=overwrite)
                results[str(file_path)] = success
                
                if success:
                    info = self.get_table_info(table_name)
                    file_end_time = datetime.now()
                    duration = (file_end_time - file_start_time).total_seconds()
                    
                    self.logger.info(f"[{i}/{len(files)}] ✓ 文件 {file_path.name} 导入成功: "
                                   f"{info.get('row_count', 0)} 行数据, 耗时 {duration:.2f} 秒")
                else:
                    self.logger.warning(f"[{i}/{len(files)}] ✗ 文件 {file_path.name} 导入失败")
                    
            except Exception as e:
                self.logger.error(f"[{i}/{len(files)}] ✗ 处理文件 {file_path.name} 时出错: {str(e)}")
                results[str(file_path)] = False
        
        # 批量导入完成统计
        batch_end_time = datetime.now()
        total_duration = (batch_end_time - batch_start_time).total_seconds()
        success_count = sum(results.values())
        
        self.logger.info(f"批量导入完成: {success_count}/{len(files)} 个文件成功, "
                        f"总耗时 {total_duration:.2f} 秒")
        
        # 详细统计信息
        if success_count > 0:
            total_rows = 0
            for file_path, success in results.items():
                if success:
                    try:
                        table_name = f"{table_prefix}{Path(file_path).stem.lower()}"
                        info = self.get_table_info(table_name)
                        total_rows += info.get('row_count', 0)
                    except:
                        pass
            
            if total_rows > 0:
                self.logger.info(f"批量导入统计: 总计导入 {total_rows:,} 行数据, "
                               f"平均速度 {total_rows/total_duration:.0f} 行/秒")
        
        return results
    
    @function_timer
    def update_data_incremental(self, table_name: str, new_data: Union[str, pd.DataFrame],
                               date_column: str = 'datetime', 
                               conflict_strategy: str = 'update') -> bool:
        """
        增量更新数据
        
        Args:
            table_name: 表名
            new_data: 新数据
            date_column: 日期列名
            conflict_strategy: 冲突处理策略 ('update', 'ignore', 'error')
            
        Returns:
            bool: 更新是否成功
        """
        self._ensure_connection()
        
        try:
            # 加载新数据
            df = self._load_data(new_data)
            if df is None:
                return False
            
            # 获取表中最新日期
            self.cursor.execute(f"SELECT MAX({date_column}) as max_date FROM {table_name}")
            result = self.cursor.fetchone()
            max_date = result['max_date'] if result['max_date'] else pd.Timestamp.min
            
            self.logger.info(f"表 {table_name} 中最新日期: {max_date}")
            
            # 过滤出新于最新日期的数据
            if isinstance(df.index, pd.DatetimeIndex):
                new_df = df[df.index > max_date]
            else:
                # 如果不是DatetimeIndex，尝试转换
                df.index = pd.to_datetime(df.index)
                new_df = df[df.index > max_date]
            
            if len(new_df) == 0:
                self.logger.info("没有新数据需要更新")
                return True
            
            self.logger.info(f"发现 {len(new_df)} 行新数据")
            
            # 根据冲突策略处理数据
            if conflict_strategy == 'update':
                self._insert_dataframe(table_name, new_df)
            elif conflict_strategy == 'ignore':
                self._insert_dataframe_ignore_conflicts(table_name, new_df)
            elif conflict_strategy == 'error':
                self._insert_dataframe_error_on_conflict(table_name, new_df)
            else:
                self.logger.error(f"不支持的冲突策略: {conflict_strategy}")
                return False
            
            self.conn.commit()
            self.logger.info(f"成功增量更新 {len(new_df)} 行数据")
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"增量更新失败: {str(e)}")
            return False
    
    def _insert_dataframe_ignore_conflicts(self, table_name: str, df: pd.DataFrame):
        """
        插入数据，忽略冲突
        适配长表格格式：datetime, code, metric, value
        """
        # 获取metric名称（从表名中提取，去掉schema前缀）
        metric_name = table_name.split('.')[-1]
        
        data_tuples = []
        for datetime_idx, row in df.iterrows():
            for code in row.index:
                value = row[code]
                if not pd.isna(value):
                    data_tuples.append((datetime_idx, code, metric_name, float(value)))
        
        if not data_tuples:
            self.logger.warning(f"没有有效数据插入到表 {table_name}")
            return
        
        insert_sql = """
            INSERT INTO {} (datetime, code, metric, value) 
            VALUES %s
            ON CONFLICT (datetime, code, metric) DO NOTHING
        """.format(table_name)
        
        import psycopg2.extras
        psycopg2.extras.execute_values(
            self.cursor, insert_sql, data_tuples, page_size=1000
        )
        
        self.logger.info(f"插入数据到表 {table_name}，忽略冲突，共 {len(data_tuples)} 条记录")
    
    def _insert_dataframe_error_on_conflict(self, table_name: str, df: pd.DataFrame):
        """
        插入数据，遇到冲突时报错
        适配长表格格式：datetime, code, metric, value
        """
        # 获取metric名称（从表名中提取，去掉schema前缀）
        metric_name = table_name.split('.')[-1]
        
        data_tuples = []
        for datetime_idx, row in df.iterrows():
            for code in row.index:
                value = row[code]
                if not pd.isna(value):
                    data_tuples.append((datetime_idx, code, metric_name, float(value)))
        
        if not data_tuples:
            self.logger.warning(f"没有有效数据插入到表 {table_name}")
            return
        
        insert_sql = """
            INSERT INTO {} (datetime, code, metric, value) 
            VALUES %s
        """.format(table_name)
        
        import psycopg2.extras
        psycopg2.extras.execute_values(
            self.cursor, insert_sql, data_tuples, page_size=1000
        )
        
        self.logger.info(f"插入数据到表 {table_name}，遇到冲突将报错，共 {len(data_tuples)} 条记录")
    
    @function_timer
    def validate_data_quality(self, table_name: str, 
                             checks: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        数据质量验证
        
        Args:
            table_name: 表名
            checks: 检查配置
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        self._ensure_connection()
        
        default_checks = {
            'null_percentage': 0.1,  # 空值比例阈值
            'duplicate_rows': True,   # 检查重复行
            'date_continuity': True,  # 检查日期连续性
            'outlier_detection': True # 异常值检测
        }
        
        if checks:
            default_checks.update(checks)
        
        results = {
            'table_name': table_name,
            'total_rows': 0,
            'issues': [],
            'warnings': [],
            'summary': {}
        }
        
        try:
            # 获取基本信息
            info = self.get_table_info(table_name)
            results['total_rows'] = info.get('row_count', 0)
            
            if results['total_rows'] == 0:
                results['issues'].append("表为空")
                return results
            
            # 检查空值比例
            if 'null_percentage' in default_checks:
                null_stats = self._check_null_values(table_name, default_checks['null_percentage'])
                results['summary']['null_values'] = null_stats
                
                for col, pct in null_stats.items():
                    if pct > default_checks['null_percentage']:
                        results['warnings'].append(f"列 {col} 空值比例过高: {pct:.2%}")
            
            # 检查重复行
            if default_checks.get('duplicate_rows'):
                dup_count = self._check_duplicate_rows(table_name)
                results['summary']['duplicate_rows'] = dup_count
                
                if dup_count > 0:
                    results['warnings'].append(f"发现 {dup_count} 行重复数据")
            
            # 检查日期连续性
            if default_checks.get('date_continuity'):
                date_gaps = self._check_date_continuity(table_name)
                results['summary']['date_gaps'] = len(date_gaps)
                
                if date_gaps:
                    results['warnings'].append(f"发现 {len(date_gaps)} 个日期间隙")
            
            self.logger.info(f"数据质量验证完成: {table_name}")
            
        except Exception as e:
            results['issues'].append(f"验证过程出错: {str(e)}")
            self.logger.error(f"数据质量验证失败: {str(e)}")
        
        return results
    
    def _check_null_values(self, table_name: str, threshold: float) -> Dict[str, float]:
        """检查空值比例"""
        self.cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
        total_rows = self.cursor.fetchone()['total']
        
        # 获取所有列
        self.cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name != 'datetime'
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = [row['column_name'] for row in self.cursor.fetchall()]
        null_stats = {}
        
        for col in columns:
            self.cursor.execute(f'SELECT COUNT(*) as null_count FROM {table_name} WHERE "{col}" IS NULL')
            null_count = self.cursor.fetchone()['null_count']
            null_stats[col] = null_count / total_rows if total_rows > 0 else 0
        
        return null_stats
    
    def _check_duplicate_rows(self, table_name: str) -> int:
        """检查重复行"""
        self.cursor.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT datetime) as duplicate_count 
            FROM {table_name}
        """)
        return self.cursor.fetchone()['duplicate_count']
    
    def _check_date_continuity(self, table_name: str) -> List[Tuple[str, str]]:
        """检查日期连续性"""
        self.cursor.execute(f"""
            WITH date_series AS (
                SELECT datetime,
                       LAG(datetime) OVER (ORDER BY datetime) as prev_date
                FROM {table_name}
                ORDER BY datetime
            )
            SELECT prev_date, datetime
            FROM date_series
            WHERE datetime - prev_date > INTERVAL '1 day'
            AND prev_date IS NOT NULL
        """)
        
        gaps = [(row['prev_date'], row['datetime']) for row in self.cursor.fetchall()]
        return gaps
    
    @function_timer
    def optimize_table(self, table_name: str, operations: List[str] = None) -> bool:
        """
        优化表性能
        
        Args:
            table_name: 表名
            operations: 优化操作列表
            
        Returns:
            bool: 优化是否成功
        """
        if operations is None:
            operations = ['analyze', 'vacuum', 'reindex']
        
        self._ensure_connection()
        
        try:
            for operation in operations:
                if operation == 'analyze':
                    self.cursor.execute(f"ANALYZE {table_name}")
                    self.logger.info(f"已分析表 {table_name}")
                
                elif operation == 'vacuum':
                    # VACUUM需要在自动提交模式下执行
                    old_autocommit = self.conn.autocommit
                    self.conn.autocommit = True
                    self.cursor.execute(f"VACUUM {table_name}")
                    self.conn.autocommit = old_autocommit
                    self.logger.info(f"已清理表 {table_name}")
                
                elif operation == 'reindex':
                    self.cursor.execute(f"REINDEX TABLE {table_name}")
                    self.logger.info(f"已重建索引 {table_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"优化表 {table_name} 失败: {str(e)}")
            return False
    
    @function_timer
    def export_data(self, table_name: str, output_path: str,
                   format: str = 'csv', **kwargs) -> bool:
        """
        导出数据到文件
        
        Args:
            table_name: 表名
            output_path: 输出路径
            format: 输出格式 ('csv', 'excel', 'json', 'parquet')
            **kwargs: 其他参数
            
        Returns:
            bool: 导出是否成功
        """
        try:
            df = self.query_data(table_name, **kwargs)
            if df is None:
                self.logger.error("无法获取数据进行导出")
                return False
            
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            if format.lower() == 'csv':
                df.to_csv(output_file)
            elif format.lower() == 'excel':
                df.to_excel(output_file)
            elif format.lower() == 'json':
                df.to_json(output_file, orient='index', date_format='iso')
            elif format.lower() == 'parquet':
                df.to_parquet(output_file)
            else:
                self.logger.error(f"不支持的导出格式: {format}")
                return False
            
            self.logger.info(f"成功导出数据到 {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"导出数据失败: {str(e)}")
            return False
    
    @function_timer
    def create_index(self, table_name: str, columns: List[str], 
                    index_name: str = None, unique: bool = False) -> bool:
        """
        创建索引
        
        Args:
            table_name: 表名
            columns: 列名列表
            index_name: 索引名
            unique: 是否唯一索引
            
        Returns:
            bool: 创建是否成功
        """
        self._ensure_connection()
        
        try:
            if index_name is None:
                index_name = f"idx_{table_name}_{'_'.join(columns)}"
            
            unique_str = "UNIQUE" if unique else ""
            columns_str = ', '.join([f'"{col}"' for col in columns])
            
            create_sql = f"""
                CREATE {unique_str} INDEX IF NOT EXISTS {index_name}
                ON {table_name} ({columns_str})
            """
            
            self.cursor.execute(create_sql)
            self.conn.commit()
            
            self.logger.info(f"成功创建索引 {index_name}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"创建索引失败: {str(e)}")
            return False
    
    def get_data_statistics(self, table_name: str, columns: List[str] = None) -> Dict[str, Any]:
        """
        获取数据统计信息
        
        Args:
            table_name: 表名
            columns: 列名列表，None表示所有数值列
            
        Returns:
            Dict[str, Any]: 统计信息
        """
        self._ensure_connection()
        
        try:
            # 如果没有指定列，获取所有数值列
            if columns is None:
                self.cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    AND data_type IN ('double precision', 'numeric', 'integer', 'real')
                    ORDER BY ordinal_position
                """, (table_name,))
                columns = [row['column_name'] for row in self.cursor.fetchall()]
            
            if not columns:
                return {}
            
            stats = {}
            for col in columns:
                self.cursor.execute(f"""
                    SELECT 
                        COUNT("{col}") as count,
                        AVG("{col}") as mean,
                        STDDEV("{col}") as std,
                        MIN("{col}") as min,
                        MAX("{col}") as max,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col}") as q25,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{col}") as median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col}") as q75
                    FROM {table_name}
                    WHERE "{col}" IS NOT NULL
                """)
                
                result = self.cursor.fetchone()
                stats[col] = dict(result) if result else {}
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取统计信息失败: {str(e)}")
            return {}


if __name__ == "__main__":
    # 使用示例
    with AdvancedPostgreSQLManager() as db:
        print("高级PostgreSQL数据管理系统测试")
        
        # 批量导入示例
        results = db.batch_insert_files("sample_data", "factor_", "*.csv")
        print(f"批量导入结果: {results}")
        
        # 数据质量验证示例
        if db.list_tables():
            table_name = db.list_tables()[0]
            quality_report = db.validate_data_quality(table_name)
            print(f"数据质量报告: {quality_report}")