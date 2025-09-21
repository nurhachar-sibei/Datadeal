"""
PostgreSQL数据管理系统综合测试
测试所有功能：创建、查询、更新、删除、时间切片、合并等

Author: Nurhachar
Date: 2025
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timedelta
import sys
import os

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_log.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def test_basic_operations():
    """测试基本操作"""
    logger.info("=" * 50)
    logger.info("开始测试基本操作")
    logger.info("=" * 50)
    
    with PostgreSQLManager() as db:
        # 1. 测试连接
        logger.info("1. 测试数据库连接")
        tables = db.list_tables()
        logger.info(f"当前数据库中的表: {tables}")
        
        # 2. 测试创建表格
        logger.info("2. 测试创建表格")
        sample_file = Path("sample_data/Price_Close.csv")
        if sample_file.exists():
            success = db.create_table("test_price_close", str(sample_file), overwrite=True)
            logger.info(f"创建表格结果: {success}")
            
            if success:
                info = db.get_table_info("test_price_close")
                logger.info(f"表格信息: {info}")
        else:
            logger.warning(f"样本文件不存在: {sample_file}")
        
        # 3. 测试数据查询
        logger.info("3. 测试数据查询")
        if "test_price_close" in db.list_tables():
            # 查询前10行
            df = db.query_data("test_price_close", limit=10)
            if df is not None:
                logger.info(f"查询结果形状: {df.shape}")
                logger.info(f"列名: {list(df.columns)}")
                logger.info(f"日期范围: {df.index.min()} 到 {df.index.max()}")
            
            # 测试条件查询
            df_filtered = db.query_data(
                "test_price_close", 
                start_date="2020-01-01", 
                end_date="2020-01-31",
                columns=["000001.SZ", "000002.SZ"]
            )
            if df_filtered is not None:
                logger.info(f"条件查询结果形状: {df_filtered.shape}")
        
        # 4. 测试时间切片
        logger.info("4. 测试时间切片")
        if "test_price_close" in db.list_tables():
            df_slice = db.time_slice("test_price_close", "2020-01-01", "2020-01-10")
            if df_slice is not None:
                logger.info(f"时间切片结果形状: {df_slice.shape}")
        
        # 5. 测试数据删除
        logger.info("5. 测试数据删除")
        if "test_price_close" in db.list_tables():
            # 删除指定日期范围的数据
            success = db.delete_data("test_price_close", 
                                   start_date="2020-01-01", 
                                   end_date="2020-01-05")
            logger.info(f"删除数据结果: {success}")
            
            if success:
                info = db.get_table_info("test_price_close")
                logger.info(f"删除后表格信息: {info}")

def test_advanced_operations():
    """测试高级操作"""
    logger.info("=" * 50)
    logger.info("开始测试高级操作")
    logger.info("=" * 50)
    
    with AdvancedPostgreSQLManager() as db:
        # 1. 测试批量导入
        logger.info("1. 测试批量导入")
        sample_dir = Path("sample_data")
        if sample_dir.exists():
            results = db.batch_insert_files(
                str(sample_dir), 
                table_prefix="factor_", 
                file_pattern="*.csv",
                overwrite=True
            )
            logger.info(f"批量导入结果: {results}")
            
            # 显示所有表
            tables = db.list_tables()
            logger.info(f"导入后的表列表: {tables}")
        
        # 2. 测试数据质量验证
        logger.info("2. 测试数据质量验证")
        tables = db.list_tables()
        if tables:
            table_name = tables[0]
            quality_report = db.validate_data_quality(table_name)
            logger.info(f"数据质量报告 ({table_name}):")
            logger.info(f"  总行数: {quality_report['total_rows']}")
            logger.info(f"  问题: {quality_report['issues']}")
            logger.info(f"  警告: {quality_report['warnings']}")
            logger.info(f"  摘要: {quality_report['summary']}")
        
        # 3. 测试增量更新
        logger.info("3. 测试增量更新")
        if tables and len(tables) > 0:
            table_name = tables[0]
            
            # 创建一些新数据进行增量更新
            df_original = db.query_data(table_name, limit=100)
            if df_original is not None and len(df_original) > 0:
                # 创建未来日期的数据
                last_date = df_original.index.max()
                new_dates = pd.date_range(
                    start=last_date + timedelta(days=1), 
                    periods=5, 
                    freq='D'
                )
                
                # 生成随机数据
                new_data = pd.DataFrame(
                    np.random.randn(len(new_dates), len(df_original.columns)),
                    index=new_dates,
                    columns=df_original.columns
                )
                
                # 执行增量更新
                success = db.update_data_incremental(
                    table_name, 
                    new_data, 
                    conflict_strategy='update'
                )
                logger.info(f"增量更新结果: {success}")
                
                if success:
                    info = db.get_table_info(table_name)
                    logger.info(f"更新后表格信息: {info}")
        
        # 4. 测试表合并
        logger.info("4. 测试表合并")
        if len(tables) >= 2:
            table1, table2 = tables[0], tables[1]
            
            # 获取两个表的数据进行合并测试
            df1 = db.query_data(table1, limit=50)
            df2 = db.query_data(table2, limit=50)
            
            if df1 is not None and df2 is not None:
                merged_df = db.merge_tables(
                    table1, table2, 
                    join_type='inner',
                    suffix=('_1', '_2')
                )
                
                if merged_df is not None:
                    logger.info(f"表合并结果形状: {merged_df.shape}")
                    logger.info(f"合并后列数: {len(merged_df.columns)}")
        
        # 5. 测试数据统计
        logger.info("5. 测试数据统计")
        if tables:
            table_name = tables[0]
            stats = db.get_data_statistics(table_name)
            if stats:
                logger.info(f"数据统计信息 ({table_name}):")
                for col, stat in list(stats.items())[:3]:  # 只显示前3列的统计
                    logger.info(f"  {col}: 均值={stat.get('mean', 'N/A'):.4f}, "
                              f"标准差={stat.get('std', 'N/A'):.4f}")
        
        # 6. 测试表优化
        logger.info("6. 测试表优化")
        if tables:
            table_name = tables[0]
            success = db.optimize_table(table_name, ['analyze'])
            logger.info(f"表优化结果: {success}")

def test_data_export():
    """测试数据导出"""
    logger.info("=" * 50)
    logger.info("开始测试数据导出")
    logger.info("=" * 50)
    
    with AdvancedPostgreSQLManager() as db:
        tables = db.list_tables()
        if tables:
            table_name = tables[0]
            
            # 测试导出为CSV
            export_dir = Path("exports")
            export_dir.mkdir(exist_ok=True)
            
            success = db.export_data(
                table_name, 
                f"exports/{table_name}_export.csv",
                format='csv',
                limit=100
            )
            logger.info(f"CSV导出结果: {success}")
            
            # 测试导出为Excel
            success = db.export_data(
                table_name, 
                f"exports/{table_name}_export.xlsx",
                format='excel',
                limit=100
            )
            logger.info(f"Excel导出结果: {success}")

def test_performance():
    """测试性能"""
    logger.info("=" * 50)
    logger.info("开始测试性能")
    logger.info("=" * 50)
    
    with PostgreSQLManager() as db:
        tables = db.list_tables()
        if tables:
            table_name = tables[0]
            
            # 测试大量数据查询性能
            import time
            
            start_time = time.time()
            df = db.query_data(table_name)
            end_time = time.time()
            
            if df is not None:
                logger.info(f"查询 {len(df)} 行数据耗时: {end_time - start_time:.2f} 秒")
                logger.info(f"数据大小: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

def cleanup_test_data():
    """清理测试数据"""
    logger.info("=" * 50)
    logger.info("清理测试数据")
    logger.info("=" * 50)
    
    with PostgreSQLManager() as db:
        tables = db.list_tables()
        test_tables = [t for t in tables if t.startswith(('test_', 'factor_'))]
        
        for table in test_tables:
            try:
                db.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                db.conn.commit()
                logger.info(f"删除测试表: {table}")
            except Exception as e:
                logger.error(f"删除表 {table} 失败: {str(e)}")

def main():
    """主测试函数"""
    logger.info("开始PostgreSQL数据管理系统综合测试")
    
    try:
        # 运行所有测试
        test_basic_operations()
        test_advanced_operations()
        test_data_export()
        test_performance()
        
        logger.info("=" * 50)
        logger.info("所有测试完成!")
        logger.info("=" * 50)
        
        # 询问是否清理测试数据
        response = input("是否清理测试数据? (y/n): ")
        if response.lower() == 'y':
            cleanup_test_data()
        
    except Exception as e:
        logger.error(f"测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()