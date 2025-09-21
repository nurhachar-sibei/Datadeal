"""
PostgreSQL数据管理系统测试示例
演示如何使用系统进行数据管理操作

Author: Nurhachar
Date: 2025
"""

import sys
import os
from pathlib import Path

# 添加当前目录到Python路径
sys.path.append(str(Path(__file__).parent))

from postgres_manager import PostgreSQLManager
import pandas as pd
import numpy as np

def test_basic_operations():
    """测试基本操作"""
    print("=" * 50)
    print("开始测试PostgreSQL数据管理系统")
    print("=" * 50)
    
    # 创建数据库管理器实例
    with PostgreSQLManager() as db:
        
        # 1. 列出现有表
        print("\n1. 列出现有表:")
        tables = db.list_tables()
        print(f"现有表: {tables}")
        
        # 2. 从CSV文件创建表
        print("\n2. 从CSV文件创建表:")
        csv_file = "sample_data/Price_Close.csv"
        if os.path.exists(csv_file):
            success = db.create_table("price_close", csv_file, overwrite=True)
            print(f"创建表 price_close: {'成功' if success else '失败'}")
        else:
            print(f"CSV文件不存在: {csv_file}")
        
        # 3. 查询数据
        print("\n3. 查询数据:")
        df = db.query_data("price_close", 
                          start_date="2020-01-01", 
                          end_date="2020-01-10",
                          codes=["000001.SZ", "600000.SH"])
        if df is not None:
            print(f"查询结果形状: {df.shape}")
            print("前5行数据:")
            print(df.head())
        
        # 4. 时间切片
        print("\n4. 时间切片测试:")
        slice_df = db.time_slice("price_close", "2020-01-05", "2020-01-08")
        if slice_df is not None:
            print(f"时间切片结果形状: {slice_df.shape}")
        
        # 5. 获取表信息
        print("\n5. 获取表信息:")
        table_info = db.get_table_info("price_close")
        if table_info:
            print(f"表名: {table_info['table_name']}")
            print(f"行数: {table_info['row_count']}")
            print(f"列数: {len(table_info['columns'])}")

def test_multiple_files():
    """测试多个文件导入"""
    print("\n" + "=" * 50)
    print("测试多个文件导入")
    print("=" * 50)
    
    with PostgreSQLManager() as db:
        
        # 获取sample_data目录下的所有CSV文件
        sample_dir = Path("sample_data")
        if sample_dir.exists():
            csv_files = list(sample_dir.glob("*.csv"))
            print(f"发现 {len(csv_files)} 个CSV文件")
            
            for csv_file in csv_files[:3]:  # 只测试前3个文件
                table_name = csv_file.stem.lower()
                print(f"\n导入文件: {csv_file.name} -> 表: {table_name}")
                
                success = db.create_table(table_name, str(csv_file), overwrite=True)
                if success:
                    # 查询一些基本信息
                    info = db.get_table_info(table_name)
                    print(f"  - 行数: {info.get('row_count', 0)}")
                    print(f"  - 列数: {len(info.get('columns', []))}")
                else:
                    print(f"  - 导入失败")

def test_data_operations():
    """测试数据操作"""
    print("\n" + "=" * 50)
    print("测试数据操作")
    print("=" * 50)
    
    with PostgreSQLManager() as db:
        
        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        test_data = pd.DataFrame({
            'stock_a': np.random.randn(10) * 100 + 1000,
            'stock_b': np.random.randn(10) * 50 + 500,
            'stock_c': np.random.randn(10) * 200 + 2000
        }, index=dates)
        
        print("创建测试数据:")
        print(test_data.head())
        
        # 创建表并插入数据
        success = db.create_table("test_stocks", test_data, overwrite=True)
        print(f"\n创建测试表: {'成功' if success else '失败'}")
        
        if success:
            # 查询全部数据
            all_data = db.query_data("test_stocks")
            print(f"\n查询全部数据形状: {all_data.shape}")
            
            # 查询特定股票
            specific_stocks = db.query_data("test_stocks", codes=["stock_a", "stock_c"])
            print(f"查询特定股票形状: {specific_stocks.shape}")
            
            # 时间范围查询
            time_range = db.query_data("test_stocks", 
                                     start_date="2024-01-03", 
                                     end_date="2024-01-07")
            print(f"时间范围查询形状: {time_range.shape}")
            
            # 返回numpy格式
            numpy_data = db.query_data("test_stocks", return_format='numpy')
            print(f"Numpy格式数据形状: {numpy_data.shape}")

def test_table_merge():
    """测试表合并功能"""
    print("\n" + "=" * 50)
    print("测试表合并功能")
    print("=" * 50)
    
    with PostgreSQLManager() as db:
        
        # 检查是否有足够的表进行合并测试
        tables = db.list_tables()
        if len(tables) >= 2:
            table1, table2 = tables[0], tables[1]
            print(f"合并表: {table1} 和 {table2}")
            
            merged_df = db.merge_tables(table1, table2, join_type='inner')
            if merged_df is not None:
                print(f"合并结果形状: {merged_df.shape}")
                print("合并结果前5行:")
                print(merged_df.head())
            else:
                print("合并失败")
        else:
            print("没有足够的表进行合并测试")

if __name__ == "__main__":
    try:
        # 切换到正确的工作目录
        os.chdir(Path(__file__).parent)
        
        # 运行测试
        test_basic_operations()
        test_multiple_files()
        test_data_operations()
        test_table_merge()
        
        print("\n" + "=" * 50)
        print("所有测试完成!")
        print("=" * 50)
        
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()