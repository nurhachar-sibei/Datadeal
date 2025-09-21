#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整工作流程示例
演示PostgreSQL数据管理系统的完整使用流程
包括数据导入、查询、分析和导出的完整工作流程
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

def create_sample_data():
    """创建示例数据"""
    print("=" * 60)
    print("1. 创建示例数据")
    print("=" * 60)
    
    # 创建时间序列
    dates = pd.date_range('2020-01-01', '2022-12-31', freq='D')
    stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '600519.SH']
    
    # 创建基本面数据 - PB比率
    pb_data = pd.DataFrame(
        np.random.uniform(0.5, 5.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    
    # 创建基本面数据 - PE比率
    pe_data = pd.DataFrame(
        np.random.uniform(5.0, 50.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    
    # 创建价格数据
    price_data = pd.DataFrame(
        np.random.uniform(10.0, 100.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    
    # 创建技术指标数据
    rsi_data = pd.DataFrame(
        np.random.uniform(20.0, 80.0, (len(dates), len(stock_codes))),
        index=dates,
        columns=stock_codes
    )
    
    print(f"✓ 创建了 {len(dates)} 天的数据")
    print(f"✓ 包含 {len(stock_codes)} 只股票")
    print(f"✓ 数据类型: PB比率、PE比率、价格、RSI指标")
    
    return {
        'pb_ratio': pb_data,
        'pe_ratio': pe_data,
        'price_close': price_data,
        'rsi': rsi_data
    }

def demonstrate_basic_operations():
    """演示基础操作"""
    print("\n" + "=" * 60)
    print("2. 基础数据管理操作")
    print("=" * 60)
    
    # 创建示例数据
    sample_data = create_sample_data()
    
    with PostgreSQLManager() as db:
        print("\n2.1 从DataFrame创建表")
        print("-" * 40)
        
        # 从DataFrame创建表
        for factor_name, data in sample_data.items():
            table_name = f"demo_{factor_name}"
            success = db.create_table(table_name, data, overwrite=True)
            if success:
                info = db.get_table_info(table_name)
                print(f"✓ 表 {table_name}: {info['row_count']} 行, {info['column_count']} 列")
            else:
                print(f"✗ 表 {table_name} 创建失败")
        
        print("\n2.2 数据查询操作")
        print("-" * 40)
        
        # 基础查询
        df_all = db.query_data("demo_pb_ratio", limit=5)
        print(f"✓ 全部数据查询 (前5行): {df_all.shape}")
        print(df_all.head())
        
        # 时间切片查询
        df_2020 = db.query_data("demo_pb_ratio", 
                                start_date='2020-01-01', 
                                end_date='2020-12-31')
        print(f"\n✓ 2020年数据: {df_2020.shape}")
        
        # 列筛选查询
        df_selected = db.query_data("demo_pb_ratio", 
                                   columns=['000001.SZ', '600000.SH'])
        print(f"✓ 选定列查询: {df_selected.shape}")
        
        # 组合查询
        df_complex = db.query_data("demo_pb_ratio",
                                  start_date='2021-01-01',
                                  end_date='2021-06-30',
                                  columns=['000001.SZ', '000002.SZ'],
                                  limit=10)
        print(f"✓ 复合查询: {df_complex.shape}")
        
        print("\n2.3 数据插入操作")
        print("-" * 40)
        
        # 创建新数据
        new_dates = pd.date_range('2023-01-01', periods=5, freq='D')
        new_data = pd.DataFrame(
            np.random.uniform(0.5, 5.0, (5, len(sample_data['pb_ratio'].columns))),
            index=new_dates,
            columns=sample_data['pb_ratio'].columns
        )
        
        # 插入新数据
        success = db.insert_data("demo_pb_ratio", new_data)
        if success:
            # 验证插入
            latest_data = db.query_data("demo_pb_ratio", start_date='2023-01-01')
            print(f"✓ 新数据插入成功: {latest_data.shape}")
        
        print("\n2.4 表管理操作")
        print("-" * 40)
        
        # 列出所有表
        tables = db.list_tables()
        demo_tables = [t for t in tables if t.startswith('demo_')]
        print(f"✓ 演示表数量: {len(demo_tables)}")
        
        # 获取表详细信息
        for table in demo_tables[:2]:  # 只显示前2个表的信息
            info = db.get_table_info(table)
            print(f"  - {table}: {info}")

def demonstrate_advanced_operations():
    """演示高级操作"""
    print("\n" + "=" * 60)
    print("3. 高级数据管理操作")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n3.1 批量数据导入")
        print("-" * 40)
        
        # 准备混合数据源
        # DataFrame数据源
        df_data = pd.DataFrame({
            '000001.SZ': np.random.randn(10),
            '000002.SZ': np.random.randn(10)
        }, index=pd.date_range('2023-02-01', periods=10))
        
        # 字典数据源
        dict_data = {
            'datetime': ['2023-03-01', '2023-03-02', '2023-03-03'],
            '000001.SZ': [1.23, 1.45, 1.67],
            '000002.SZ': [2.34, 2.56, 2.78]
        }
        
        # 批量导入
        data_sources = [df_data, dict_data]
        table_names = ["batch_df_data", "batch_dict_data"]
        
        results = db.batch_insert_data(data_sources, table_names, overwrite=True)
        print(f"✓ 批量导入结果: {results}")
        
        print("\n3.2 多因子查询")
        print("-" * 40)
        
        # 检查可用的因子表
        tables = db.list_tables()
        factor_tables = [t for t in tables if t.startswith('demo_')]
        print(f"✓ 可用因子表: {factor_tables}")
        
        if len(factor_tables) >= 2:
            # 执行多因子查询
            multi_factor_data = db.query_data_multifactor(
                start_date='2020-01-01',
                end_date='2020-01-31',
                codes=['000001.SZ', '000002.SZ']
            )
            print(f"✓ 多因子查询结果: {multi_factor_data.shape}")
            print(f"✓ 包含列: {multi_factor_data.columns.tolist()}")
            print("\n前5行数据:")
            print(multi_factor_data.head())
        
        print("\n3.3 数据统计分析")
        print("-" * 40)
        
        # 获取数据统计
        if factor_tables:
            table_name = factor_tables[0]
            stats = db.get_data_statistics(table_name)
            print(f"✓ 表 {table_name} 统计信息:")
            for key, value in stats.items():
                print(f"  - {key}: {value}")
        
        print("\n3.4 数据质量验证")
        print("-" * 40)
        
        # 数据质量检查
        if factor_tables:
            table_name = factor_tables[0]
            quality_report = db.validate_data_quality(table_name)
            print(f"✓ 表 {table_name} 数据质量报告:")
            for key, value in quality_report.items():
                print(f"  - {key}: {value}")

def demonstrate_data_export():
    """演示数据导出"""
    print("\n" + "=" * 60)
    print("4. 数据导出操作")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        tables = db.list_tables()
        demo_tables = [t for t in tables if t.startswith('demo_')]
        
        if demo_tables:
            table_name = demo_tables[0]
            
            print(f"\n4.1 导出表 {table_name}")
            print("-" * 40)
            
            # 导出全部数据
            output_file = f"exported_{table_name}.csv"
            success = db.export_data(table_name, output_file)
            if success:
                print(f"✓ 全部数据导出成功: {output_file}")
            
            # 导出部分数据
            partial_file = f"partial_{table_name}.csv"
            success = db.export_data(
                table_name, 
                partial_file,
                start_date='2020-01-01',
                end_date='2020-12-31',
                limit=100
            )
            if success:
                print(f"✓ 部分数据导出成功: {partial_file}")
            
            # 导出指定列
            columns_file = f"columns_{table_name}.csv"
            success = db.export_data(
                table_name,
                columns_file,
                columns=['000001.SZ', '000002.SZ']
            )
            if success:
                print(f"✓ 指定列导出成功: {columns_file}")

def demonstrate_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("5. 错误处理演示")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        print("\n5.1 处理不存在的表")
        print("-" * 40)
        
        # 查询不存在的表
        try:
            result = db.query_data("non_existent_table")
        except Exception as e:
            print(f"✓ 捕获错误: {str(e)}")
        
        print("\n5.2 处理无效的数据格式")
        print("-" * 40)
        
        # 尝试插入无效数据
        try:
            invalid_data = pd.DataFrame({
                'invalid_column': ['not_a_number', 'also_not_a_number']
            })
            success = db.create_table("test_invalid", invalid_data)
            if not success:
                print("✓ 系统正确拒绝了无效数据")
        except Exception as e:
            print(f"✓ 捕获数据格式错误: {str(e)}")
        
        print("\n5.3 处理重复表名")
        print("-" * 40)
        
        # 创建测试表
        test_data = pd.DataFrame({
            '000001.SZ': [1.0, 2.0, 3.0]
        }, index=pd.date_range('2020-01-01', periods=3))
        
        # 第一次创建
        success1 = db.create_table("duplicate_test", test_data, overwrite=False)
        print(f"✓ 首次创建表: {success1}")
        
        # 第二次创建（不覆盖）
        success2 = db.create_table("duplicate_test", test_data, overwrite=False)
        print(f"✓ 重复创建（不覆盖）: {success2}")
        
        # 第三次创建（覆盖）
        success3 = db.create_table("duplicate_test", test_data, overwrite=True)
        print(f"✓ 重复创建（覆盖）: {success3}")

def cleanup_demo_data():
    """清理演示数据"""
    print("\n" + "=" * 60)
    print("6. 清理演示数据")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        tables = db.list_tables()
        demo_tables = [t for t in tables if t.startswith('demo_') or 
                      t.startswith('batch_') or t == 'duplicate_test']
        
        print(f"发现 {len(demo_tables)} 个演示表")
        
        for table in demo_tables:
            success = db.drop_table(table)
            if success:
                print(f"✓ 删除表: {table}")
            else:
                print(f"✗ 删除失败: {table}")
        
        # 清理导出的文件
        export_files = [f for f in os.listdir('.') if f.startswith('exported_') or 
                       f.startswith('partial_') or f.startswith('columns_')]
        
        for file in export_files:
            try:
                os.remove(file)
                print(f"✓ 删除文件: {file}")
            except:
                print(f"✗ 删除文件失败: {file}")

def main():
    """主函数"""
    print("PostgreSQL数据管理系统 - 完整工作流程演示")
    print("=" * 80)
    
    try:
        # 1. 基础操作演示
        demonstrate_basic_operations()
        
        # 2. 高级操作演示
        demonstrate_advanced_operations()
        
        # 3. 数据导出演示
        demonstrate_data_export()
        
        # 4. 错误处理演示
        demonstrate_error_handling()
        
        # 5. 清理演示数据
        cleanup_demo_data()
        
        print("\n" + "=" * 80)
        print("✅ 完整工作流程演示完成！")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ 演示过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()