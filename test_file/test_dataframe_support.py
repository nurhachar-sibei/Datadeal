#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试DataFrame和字典数据源支持功能
"""

import pandas as pd
import numpy as np
from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

def test_dataframe_support():
    """测试DataFrame数据源支持"""
    print("=" * 60)
    print("测试DataFrame和字典数据源支持")
    print("=" * 60)
    
    # 创建数据库管理器
    manager = PostgreSQLManager()
    advanced_manager = AdvancedPostgreSQLManager()
    
    try:
        # 1. 测试DataFrame数据源
        print("\n1. 测试DataFrame数据源")
        dates = pd.date_range('2020-01-01', periods=10, freq='D')
        df_data = pd.DataFrame({
            'stock_a': np.random.randn(10),
            'stock_b': np.random.randn(10),
            'stock_c': np.random.randn(10)
        }, index=dates)
        
        print(f"创建测试DataFrame，形状: {df_data.shape}")
        success = manager.create_table("test_dataframe_source", df_data, overwrite=True)
        if success:
            print("✓ DataFrame数据源导入成功")
        else:
            print("✗ DataFrame数据源导入失败")
        
        # 2. 测试字典数据源
        print("\n2. 测试字典数据源")
        dict_data = {
            'date': pd.date_range('2020-02-01', periods=5, freq='D'),
            'value_x': [1.1, 2.2, 3.3, 4.4, 5.5],
            'value_y': [10, 20, 30, 40, 50],
            'value_z': ['A', 'B', 'C', 'D', 'E']
        }
        
        print(f"创建测试字典，包含 {len(dict_data)} 个键")
        # 需要先转换为DataFrame并设置索引
        dict_df = pd.DataFrame(dict_data)
        dict_df.set_index('date', inplace=True)
        
        success = manager.create_table("test_dict_source", dict_df, overwrite=True)
        if success:
            print("✓ 字典数据源导入成功")
        else:
            print("✗ 字典数据源导入失败")
        
        # 3. 测试批量导入混合数据源
        print("\n3. 测试批量导入混合数据源")
        
        # 创建多个不同类型的数据源
        df1 = pd.DataFrame({
            'col1': np.random.randn(8),
            'col2': np.random.randn(8)
        }, index=pd.date_range('2020-03-01', periods=8, freq='D'))
        
        df2 = pd.DataFrame({
            'metric_a': [100, 200, 300, 150, 250],
            'metric_b': [0.1, 0.2, 0.3, 0.15, 0.25]
        }, index=pd.date_range('2020-04-01', periods=5, freq='D'))
        
        # 混合数据源列表
        data_sources = [df1, df2]
        table_names = ['test_batch_df1', 'test_batch_df2']
        
        results = advanced_manager.batch_insert_data(data_sources, table_names, overwrite=True)
        
        success_count = sum(1 for success in results.values() if success)
        print(f"批量导入结果: {success_count}/{len(data_sources)} 个数据源成功")
        
        # 4. 验证数据
        print("\n4. 验证导入的数据")
        
        tables_to_check = ['test_dataframe_source', 'test_dict_source', 'test_batch_df1', 'test_batch_df2']
        
        for table_name in tables_to_check:
            result = manager.query_data(table_name, limit=3)
            if result is not None:
                print(f"✓ 表 {table_name}: {result.shape[0]} 行数据")
            else:
                print(f"✗ 表 {table_name}: 查询失败")
        
        # 5. 测试insert_data方法的DataFrame支持
        print("\n5. 测试insert_data方法的DataFrame支持")
        
        # 创建新数据
        new_df = pd.DataFrame({
            'stock_a': [0.5, 0.6],
            'stock_b': [1.5, 1.6],
            'stock_c': [2.5, 2.6]
        }, index=pd.date_range('2020-01-11', periods=2, freq='D'))
        
        success = manager.insert_data("test_dataframe_source", new_df)
        if success:
            print("✓ DataFrame增量数据插入成功")
            # 验证数据
            result = manager.query_data("test_dataframe_source")
            if result is not None:
                print(f"  更新后表数据: {result.shape[0]} 行")
        else:
            print("✗ DataFrame增量数据插入失败")
        
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 关闭连接
        manager.close()
        advanced_manager.close()
        print("\n测试完成")

if __name__ == "__main__":
    test_dataframe_support()