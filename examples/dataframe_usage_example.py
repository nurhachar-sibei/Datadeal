#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataFrame和字典数据源使用示例
演示如何使用pandas DataFrame和字典作为数据源
包括创建、导入、查询和分析的完整流程
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

def create_dataframe_examples():
    """创建各种DataFrame示例"""
    print("=" * 60)
    print("1. 创建DataFrame数据源示例")
    print("=" * 60)
    
    # 1.1 标准时间序列DataFrame
    print("\n1.1 标准时间序列DataFrame")
    print("-" * 40)
    
    dates = pd.date_range('2020-01-01', periods=100, freq='D')
    stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
    
    # 创建股价数据
    price_df = pd.DataFrame(
        np.random.uniform(10.0, 100.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    print(f"✓ 股价DataFrame: {price_df.shape}")
    print("前5行数据:")
    print(price_df.head())
    
    # 1.2 基本面指标DataFrame
    print("\n1.2 基本面指标DataFrame")
    print("-" * 40)
    
    # 创建PB比率数据
    pb_df = pd.DataFrame(
        np.random.uniform(0.5, 5.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    print(f"✓ PB比率DataFrame: {pb_df.shape}")
    
    # 1.3 技术指标DataFrame
    print("\n1.3 技术指标DataFrame")
    print("-" * 40)
    
    # 创建RSI指标数据
    rsi_df = pd.DataFrame(
        np.random.uniform(20.0, 80.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    print(f"✓ RSI指标DataFrame: {rsi_df.shape}")
    
    # 1.4 自定义列名DataFrame
    print("\n1.4 自定义列名DataFrame")
    print("-" * 40)
    
    custom_df = pd.DataFrame({
        'stock_a': np.random.randn(50),
        'stock_b': np.random.randn(50),
        'stock_c': np.random.randn(50)
    }, index=pd.date_range('2023-01-01', periods=50))
    
    print(f"✓ 自定义DataFrame: {custom_df.shape}")
    print("列名:", custom_df.columns.tolist())
    
    return {
        'price': price_df,
        'pb_ratio': pb_df,
        'rsi': rsi_df,
        'custom': custom_df
    }

def create_dict_examples():
    """创建各种字典数据源示例"""
    print("\n" + "=" * 60)
    print("2. 创建字典数据源示例")
    print("=" * 60)
    
    # 2.1 基础字典数据
    print("\n2.1 基础字典数据")
    print("-" * 40)
    
    basic_dict = {
        'datetime': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05'],
        '000001.SZ': [12.34, 12.56, 12.78, 13.01, 13.23],
        '000002.SZ': [23.45, 23.67, 23.89, 24.12, 24.34],
        '600000.SH': [34.56, 34.78, 35.01, 35.23, 35.45]
    }
    print(f"✓ 基础字典: {len(basic_dict['datetime'])} 行, {len(basic_dict)-1} 个股票")
    print("数据预览:", {k: v[:2] for k, v in basic_dict.items()})
    
    # 2.2 大数据量字典
    print("\n2.2 大数据量字典")
    print("-" * 40)
    
    dates_list = pd.date_range('2021-01-01', periods=365, freq='D').strftime('%Y-%m-%d').tolist()
    large_dict = {
        'datetime': dates_list,
        '000001.SZ': np.random.uniform(10, 50, 365).tolist(),
        '000002.SZ': np.random.uniform(20, 60, 365).tolist(),
        '600000.SH': np.random.uniform(30, 70, 365).tolist()
    }
    print(f"✓ 大数据量字典: {len(large_dict['datetime'])} 行")
    
    # 2.3 混合数据类型字典
    print("\n2.3 混合数据类型字典")
    print("-" * 40)
    
    mixed_dict = {
        'datetime': ['2022-01-01', '2022-01-02', '2022-01-03'],
        'price': [45.67, 46.78, 47.89],
        'volume': [1000000, 1200000, 1100000],
        'pe_ratio': [15.5, 16.2, 15.8]
    }
    print(f"✓ 混合类型字典: {len(mixed_dict['datetime'])} 行")
    print("数据类型:", {k: type(v[0]).__name__ for k, v in mixed_dict.items() if k != 'datetime'})
    
    return {
        'basic': basic_dict,
        'large': large_dict,
        'mixed': mixed_dict
    }

def demonstrate_dataframe_operations():
    """演示DataFrame操作"""
    print("\n" + "=" * 60)
    print("3. DataFrame数据源操作演示")
    print("=" * 60)
    
    dataframes = create_dataframe_examples()
    
    with PostgreSQLManager() as db:
        print("\n3.1 从DataFrame创建表")
        print("-" * 40)
        
        for name, df in dataframes.items():
            table_name = f"df_{name}"
            success = db.create_table(table_name, df, overwrite=True)
            if success:
                info = db.get_table_info(table_name)
                print(f"✓ 表 {table_name}: {info['row_count']} 行, {info['column_count']} 列")
            else:
                print(f"✗ 表 {table_name} 创建失败")
        
        print("\n3.2 DataFrame数据查询")
        print("-" * 40)
        
        # 查询DataFrame创建的表
        df_result = db.query_data("df_price", limit=5)
        print(f"✓ 查询结果: {df_result.shape}")
        print("数据预览:")
        print(df_result)
        
        print("\n3.3 DataFrame数据插入")
        print("-" * 40)
        
        # 创建新的DataFrame数据
        new_dates = pd.date_range('2020-04-10', periods=5, freq='D')
        new_df = pd.DataFrame(
            np.random.uniform(10.0, 100.0, (5, 4)),
            index=new_dates,
            columns=['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
        )
        
        # 插入新数据
        success = db.insert_data("df_price", new_df)
        if success:
            # 验证插入
            latest_data = db.query_data("df_price", start_date='2020-04-10')
            print(f"✓ 新数据插入成功: {latest_data.shape}")
        
        print("\n3.4 DataFrame数据类型处理")
        print("-" * 40)
        
        # 创建包含不同数据类型的DataFrame
        mixed_df = pd.DataFrame({
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'str_col': ['10.5', '20.5', '30.5', '40.5', '50.5']  # 字符串形式的数字
        }, index=pd.date_range('2023-01-01', periods=5))
        
        success = db.create_table("df_mixed_types", mixed_df, overwrite=True)
        if success:
            result = db.query_data("df_mixed_types")
            print(f"✓ 混合类型DataFrame处理成功: {result.shape}")
            print("数据类型:", result.dtypes.to_dict())

def demonstrate_dict_operations():
    """演示字典操作"""
    print("\n" + "=" * 60)
    print("4. 字典数据源操作演示")
    print("=" * 60)
    
    dict_data = create_dict_examples()
    
    with PostgreSQLManager() as db:
        print("\n4.1 从字典创建表")
        print("-" * 40)
        
        for name, data_dict in dict_data.items():
            table_name = f"dict_{name}"
            success = db.create_table(table_name, data_dict, overwrite=True)
            if success:
                info = db.get_table_info(table_name)
                print(f"✓ 表 {table_name}: {info['row_count']} 行, {info['column_count']} 列")
            else:
                print(f"✗ 表 {table_name} 创建失败")
        
        print("\n4.2 字典数据查询")
        print("-" * 40)
        
        # 查询字典创建的表
        dict_result = db.query_data("dict_basic", limit=3)
        print(f"✓ 查询结果: {dict_result.shape}")
        print("数据预览:")
        print(dict_result)
        
        print("\n4.3 字典数据插入")
        print("-" * 40)
        
        # 创建新的字典数据
        new_dict = {
            'datetime': ['2020-01-06', '2020-01-07'],
            '000001.SZ': [13.45, 13.67],
            '000002.SZ': [24.56, 24.78],
            '600000.SH': [35.67, 35.89]
        }
        
        # 插入新数据
        success = db.insert_data("dict_basic", new_dict)
        if success:
            # 验证插入
            all_data = db.query_data("dict_basic")
            print(f"✓ 字典数据插入成功: {all_data.shape}")
            print("最新数据:")
            print(all_data.tail(2))

def demonstrate_batch_mixed_operations():
    """演示批量混合数据源操作"""
    print("\n" + "=" * 60)
    print("5. 批量混合数据源操作演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n5.1 准备混合数据源")
        print("-" * 40)
        
        # DataFrame数据源
        df_source = pd.DataFrame({
            '000001.SZ': np.random.randn(10),
            '000002.SZ': np.random.randn(10)
        }, index=pd.date_range('2023-01-01', periods=10))
        
        # 字典数据源
        dict_source = {
            'datetime': ['2023-02-01', '2023-02-02', '2023-02-03'],
            '000001.SZ': [1.23, 1.45, 1.67],
            '000002.SZ': [2.34, 2.56, 2.78]
        }
        
        # CSV文件路径（如果存在）
        csv_source = "sample_data/Fundamental_PB_Ratio.csv"
        
        print("✓ DataFrame数据源准备完成")
        print("✓ 字典数据源准备完成")
        
        print("\n5.2 批量导入混合数据源")
        print("-" * 40)
        
        # 准备数据源列表
        data_sources = [df_source, dict_source]
        table_names = ["batch_df", "batch_dict"]
        
        # 如果CSV文件存在，添加到批量导入中
        if os.path.exists(csv_source):
            data_sources.append(csv_source)
            table_names.append("batch_csv")
            print("✓ 添加CSV文件到批量导入")
        
        # 执行批量导入
        results = db.batch_insert_data(data_sources, table_names, overwrite=True)
        
        print(f"✓ 批量导入结果: {results}")
        
        # 验证导入结果
        for table_name in table_names:
            if results.get(table_name, False):
                info = db.get_table_info(table_name)
                print(f"  - {table_name}: {info['row_count']} 行, {info['column_count']} 列")
        
        print("\n5.3 查询批量导入的数据")
        print("-" * 40)
        
        for table_name in table_names:
            if results.get(table_name, False):
                data = db.query_data(table_name, limit=3)
                print(f"\n表 {table_name} (前3行):")
                print(data)

def demonstrate_data_conversion():
    """演示数据转换功能"""
    print("\n" + "=" * 60)
    print("6. 数据转换功能演示")
    print("=" * 60)
    
    print("\n6.1 DataFrame与字典互转")
    print("-" * 40)
    
    # 创建DataFrame
    original_df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    }, index=pd.date_range('2020-01-01', periods=3))
    
    print("原始DataFrame:")
    print(original_df)
    
    # DataFrame转字典
    df_to_dict = {
        'datetime': original_df.index.strftime('%Y-%m-%d').tolist(),
        **{col: original_df[col].tolist() for col in original_df.columns}
    }
    
    print("\n转换为字典:")
    print(df_to_dict)
    
    # 字典转DataFrame
    dict_to_df = pd.DataFrame(df_to_dict)
    dict_to_df['datetime'] = pd.to_datetime(dict_to_df['datetime'])
    dict_to_df.set_index('datetime', inplace=True)
    
    print("\n字典转回DataFrame:")
    print(dict_to_df)
    
    print("\n6.2 数据类型自动转换")
    print("-" * 40)
    
    with PostgreSQLManager() as db:
        # 测试字符串数字的自动转换
        string_numbers_dict = {
            'datetime': ['2020-01-01', '2020-01-02'],
            'numeric_str': ['123.45', '678.90'],  # 字符串形式的数字
            'pure_numeric': [111.22, 333.44]      # 纯数字
        }
        
        success = db.create_table("string_conversion_test", string_numbers_dict, overwrite=True)
        if success:
            result = db.query_data("string_conversion_test")
            print("✓ 字符串数字自动转换成功")
            print("数据类型:", result.dtypes.to_dict())
            print("数据内容:")
            print(result)

def cleanup_examples():
    """清理示例数据"""
    print("\n" + "=" * 60)
    print("7. 清理示例数据")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        tables = db.list_tables()
        example_tables = [t for t in tables if t.startswith('df_') or 
                         t.startswith('dict_') or t.startswith('batch_') or
                         t == 'string_conversion_test']
        
        print(f"发现 {len(example_tables)} 个示例表")
        
        for table in example_tables:
            success = db.drop_table(table)
            if success:
                print(f"✓ 删除表: {table}")
            else:
                print(f"✗ 删除失败: {table}")

def main():
    """主函数"""
    print("PostgreSQL数据管理系统 - DataFrame和字典数据源使用示例")
    print("=" * 80)
    
    try:
        # 1. DataFrame操作演示
        demonstrate_dataframe_operations()
        
        # 2. 字典操作演示
        demonstrate_dict_operations()
        
        # 3. 批量混合操作演示
        demonstrate_batch_mixed_operations()
        
        # 4. 数据转换演示
        demonstrate_data_conversion()
        
        # 5. 清理示例数据
        cleanup_examples()
        
        print("\n" + "=" * 80)
        print("✅ DataFrame和字典数据源示例演示完成！")
        print("=" * 80)
        
        print("\n📋 总结:")
        print("1. ✓ DataFrame数据源：支持各种pandas DataFrame格式")
        print("2. ✓ 字典数据源：支持标准Python字典格式")
        print("3. ✓ 批量导入：支持混合数据源类型的批量操作")
        print("4. ✓ 自动转换：系统自动处理数据类型转换")
        print("5. ✓ 灵活操作：创建、查询、插入、更新等完整功能")
        
    except Exception as e:
        print(f"\n❌ 示例演示过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()