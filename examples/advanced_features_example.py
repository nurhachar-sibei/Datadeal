#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高级功能示例
演示PostgreSQL数据管理系统的高级功能
包括批量操作、性能优化、错误处理、数据验证等
"""

import pandas as pd
import numpy as np
import time
import os
import sys
from datetime import datetime, timedelta
import logging

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def demonstrate_batch_operations():
    """演示批量操作功能"""
    print("=" * 60)
    print("1. 批量操作功能演示")
    print("=" * 60)
    
    print("\n1.1 准备批量数据源")
    print("-" * 40)
    
    # 准备多种数据源
    data_sources = []
    table_names = []
    
    # 1. DataFrame数据源
    df1 = pd.DataFrame({
        '000001.SZ': np.random.randn(50),
        '000002.SZ': np.random.randn(50)
    }, index=pd.date_range('2023-01-01', periods=50))
    data_sources.append(df1)
    table_names.append("batch_df1")
    
    # 2. 字典数据源
    dict1 = {
        'datetime': pd.date_range('2023-02-01', periods=30).strftime('%Y-%m-%d').tolist(),
        '600000.SH': np.random.uniform(10, 50, 30).tolist(),
        '600036.SH': np.random.uniform(20, 60, 30).tolist()
    }
    data_sources.append(dict1)
    table_names.append("batch_dict1")
    
    # 3. 更多DataFrame
    df2 = pd.DataFrame({
        '000858.SZ': np.random.randn(40),
        '002415.SZ': np.random.randn(40)
    }, index=pd.date_range('2023-03-01', periods=40))
    data_sources.append(df2)
    table_names.append("batch_df2")
    
    print(f"✓ 准备了 {len(data_sources)} 个数据源")
    
    print("\n1.2 执行批量导入")
    print("-" * 40)
    
    with AdvancedPostgreSQLManager() as db:
        start_time = time.time()
        
        # 执行批量导入
        results = db.batch_insert_data(
            data_sources=data_sources,
            table_names=table_names,
            overwrite=True
        )
        
        end_time = time.time()
        
        print(f"✓ 批量导入完成，耗时: {end_time - start_time:.2f} 秒")
        print("导入结果:")
        
        for table_name, success in results.items():
            if success:
                info = db.get_table_info(table_name)
                print(f"  ✓ {table_name}: {info['row_count']} 行, {info['column_count']} 列")
            else:
                print(f"  ✗ {table_name}: 导入失败")
    
    print("\n1.3 批量查询操作")
    print("-" * 40)
    
    with AdvancedPostgreSQLManager() as db:
        # 批量查询多个表
        successful_tables = [name for name, success in results.items() if success]
        
        start_time = time.time()
        
        query_results = {}
        for table_name in successful_tables:
            data = db.query_data(table_name, limit=10)
            if data is not None:
                query_results[table_name] = data
        
        end_time = time.time()
        
        print(f"✓ 批量查询完成，耗时: {end_time - start_time:.2f} 秒")
        print(f"✓ 成功查询 {len(query_results)} 个表")
        
        for table_name, data in query_results.items():
            print(f"  {table_name}: {data.shape}")

def demonstrate_performance_optimization():
    """演示性能优化功能"""
    print("\n" + "=" * 60)
    print("2. 性能优化功能演示")
    print("=" * 60)
    
    print("\n2.1 大数据量处理测试")
    print("-" * 40)
    
    # 创建大数据量测试数据
    large_data_sizes = [1000, 5000, 10000]
    stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
    
    with PostgreSQLManager() as db:
        performance_results = {}
        
        for size in large_data_sizes:
            print(f"\n测试数据量: {size} 行")
            
            # 创建测试数据
            test_data = pd.DataFrame(
                np.random.randn(size, len(stock_codes)),
                columns=stock_codes,
                index=pd.date_range('2020-01-01', periods=size, freq='D')
            )
            
            table_name = f"perf_test_{size}"
            
            # 测试创建表的性能
            start_time = time.time()
            success = db.create_table(table_name, test_data, overwrite=True)
            create_time = time.time() - start_time
            
            if success:
                # 测试查询性能
                start_time = time.time()
                query_result = db.query_data(table_name, limit=1000)
                query_time = time.time() - start_time
                
                # 测试插入性能
                new_data = pd.DataFrame(
                    np.random.randn(100, len(stock_codes)),
                    columns=stock_codes,
                    index=pd.date_range('2025-01-01', periods=100, freq='D')
                )
                
                start_time = time.time()
                insert_success = db.insert_data(table_name, new_data)
                insert_time = time.time() - start_time
                
                performance_results[size] = {
                    'create_time': create_time,
                    'query_time': query_time,
                    'insert_time': insert_time,
                    'success': True
                }
                
                print(f"  ✓ 创建: {create_time:.3f}s, 查询: {query_time:.3f}s, 插入: {insert_time:.3f}s")
            else:
                performance_results[size] = {'success': False}
                print(f"  ✗ 创建表失败")
        
        print("\n2.2 性能统计分析")
        print("-" * 40)
        
        if performance_results:
            perf_df = pd.DataFrame({
                size: results for size, results in performance_results.items() 
                if results.get('success', False)
            }).T
            
            if not perf_df.empty:
                print("✓ 性能测试结果:")
                print(perf_df[['create_time', 'query_time', 'insert_time']].round(3))
                
                # 计算性能指标
                print("\n✓ 性能指标分析:")
                print(f"  平均创建时间: {perf_df['create_time'].mean():.3f}s")
                print(f"  平均查询时间: {perf_df['query_time'].mean():.3f}s")
                print(f"  平均插入时间: {perf_df['insert_time'].mean():.3f}s")

def demonstrate_error_handling():
    """演示错误处理功能"""
    print("\n" + "=" * 60)
    print("3. 错误处理功能演示")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        print("\n3.1 数据类型错误处理")
        print("-" * 40)
        
        # 测试不兼容的数据类型
        try:
            invalid_data = {
                'datetime': ['2023-01-01', '2023-01-02'],
                'mixed_col': ['text', 123]  # 混合类型
            }
            
            success = db.create_table("error_test_mixed", invalid_data, overwrite=True)
            if success:
                print("✓ 混合类型数据处理成功（系统自动转换）")
                result = db.query_data("error_test_mixed")
                print(f"  数据类型: {result.dtypes.to_dict()}")
            else:
                print("✗ 混合类型数据处理失败")
                
        except Exception as e:
            print(f"✗ 数据类型错误: {str(e)}")
        
        print("\n3.2 空数据处理")
        print("-" * 40)
        
        # 测试空数据
        try:
            empty_data = pd.DataFrame()
            success = db.create_table("error_test_empty", empty_data, overwrite=True)
            if success:
                print("✓ 空数据处理成功")
            else:
                print("✗ 空数据处理失败（预期行为）")
        except Exception as e:
            print(f"✓ 空数据错误处理: {str(e)}")
        
        print("\n3.3 重复表名处理")
        print("-" * 40)
        
        # 测试重复表名
        try:
            test_data = pd.DataFrame({'A': [1, 2, 3]}, index=pd.date_range('2023-01-01', periods=3))
            
            # 第一次创建
            success1 = db.create_table("duplicate_test", test_data, overwrite=False)
            print(f"✓ 首次创建表: {success1}")
            
            # 第二次创建（不覆盖）
            success2 = db.create_table("duplicate_test", test_data, overwrite=False)
            print(f"✓ 重复创建（不覆盖）: {success2}")
            
            # 第三次创建（覆盖）
            success3 = db.create_table("duplicate_test", test_data, overwrite=True)
            print(f"✓ 重复创建（覆盖）: {success3}")
            
        except Exception as e:
            print(f"✗ 重复表名处理错误: {str(e)}")
        
        print("\n3.4 无效查询处理")
        print("-" * 40)
        
        # 测试查询不存在的表
        try:
            result = db.query_data("nonexistent_table")
            if result is None:
                print("✓ 不存在表的查询正确返回None")
            else:
                print("✗ 不存在表的查询返回了数据")
        except Exception as e:
            print(f"✓ 无效查询错误处理: {str(e)}")
        
        # 测试无效日期范围
        try:
            if db.table_exists("duplicate_test"):
                result = db.query_data("duplicate_test", start_date="2025-01-01", end_date="2024-01-01")
                if result is None or result.empty:
                    print("✓ 无效日期范围查询正确处理")
                else:
                    print(f"✓ 无效日期范围查询返回: {result.shape}")
        except Exception as e:
            print(f"✓ 无效日期范围错误处理: {str(e)}")

def demonstrate_data_validation():
    """演示数据验证功能"""
    print("\n" + "=" * 60)
    print("4. 数据验证功能演示")
    print("=" * 60)
    
    print("\n4.1 数据完整性验证")
    print("-" * 40)
    
    with PostgreSQLManager() as db:
        # 创建测试数据
        validation_data = pd.DataFrame({
            '000001.SZ': [10.5, 11.2, np.nan, 12.8, 13.1],  # 包含NaN
            '000002.SZ': [20.1, 21.5, 22.3, 23.0, 24.2]
        }, index=pd.date_range('2023-01-01', periods=5))
        
        success = db.create_table("validation_test", validation_data, overwrite=True)
        
        if success:
            # 查询数据并验证
            result = db.query_data("validation_test")
            
            print(f"✓ 数据形状: {result.shape}")
            print(f"✓ 缺失值统计:")
            print(result.isnull().sum())
            
            # 数据类型验证
            print(f"✓ 数据类型:")
            print(result.dtypes)
            
            # 数值范围验证
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            print(f"✓ 数值范围:")
            for col in numeric_cols:
                print(f"  {col}: [{result[col].min():.2f}, {result[col].max():.2f}]")
    
    print("\n4.2 时间序列验证")
    print("-" * 40)
    
    with PostgreSQLManager() as db:
        if db.table_exists("validation_test"):
            result = db.query_data("validation_test")
            
            # 时间索引验证
            if 'datetime' in result.columns:
                datetime_col = pd.to_datetime(result['datetime'])
                
                print(f"✓ 时间范围: {datetime_col.min()} 到 {datetime_col.max()}")
                print(f"✓ 时间点数量: {len(datetime_col)}")
                
                # 检查时间序列的连续性
                time_diffs = datetime_col.diff().dropna()
                if len(time_diffs.unique()) == 1:
                    print(f"✓ 时间序列连续，间隔: {time_diffs.iloc[0]}")
                else:
                    print(f"✓ 时间序列不规则，间隔范围: {time_diffs.min()} 到 {time_diffs.max()}")
    
    print("\n4.3 数据质量评估")
    print("-" * 40)
    
    with PostgreSQLManager() as db:
        if db.table_exists("validation_test"):
            result = db.query_data("validation_test")
            
            # 数据质量指标
            total_cells = result.shape[0] * result.shape[1]
            missing_cells = result.isnull().sum().sum()
            completeness = (total_cells - missing_cells) / total_cells
            
            print(f"✓ 数据完整性: {completeness:.2%}")
            
            # 数值列的统计特征
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                stats = result[numeric_cols].describe()
                print(f"✓ 数值统计特征:")
                print(stats.round(3))
                
                # 异常值检测（简单的3σ规则）
                for col in numeric_cols:
                    mean_val = result[col].mean()
                    std_val = result[col].std()
                    outliers = result[(result[col] < mean_val - 3*std_val) | 
                                    (result[col] > mean_val + 3*std_val)]
                    if len(outliers) > 0:
                        print(f"  {col}: 发现 {len(outliers)} 个异常值")
                    else:
                        print(f"  {col}: 无异常值")

def demonstrate_advanced_queries():
    """演示高级查询功能"""
    print("\n" + "=" * 60)
    print("5. 高级查询功能演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n5.1 准备查询测试数据")
        print("-" * 40)
        
        # 创建多个相关表
        stock_codes = ['000001.SZ', '000002.SZ', '600000.SH']
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        
        # 价格表
        price_data = pd.DataFrame(
            np.random.uniform(10, 100, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        db.create_table("adv_query_prices", price_data, overwrite=True)
        
        # 成交量表
        volume_data = pd.DataFrame(
            np.random.uniform(1000000, 10000000, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        db.create_table("adv_query_volume", volume_data, overwrite=True)
        
        print("✓ 查询测试数据准备完成")
        
        print("\n5.2 条件查询测试")
        print("-" * 40)
        
        # 按日期范围查询
        date_result = db.query_data(
            "adv_query_prices",
            start_date="2023-02-01",
            end_date="2023-02-28"
        )
        print(f"✓ 日期范围查询: {date_result.shape}")
        
        # 限制结果数量
        limit_result = db.query_data("adv_query_prices", limit=10)
        print(f"✓ 限制数量查询: {limit_result.shape}")
        
        # 组合条件查询
        combo_result = db.query_data(
            "adv_query_prices",
            start_date="2023-03-01",
            end_date="2023-03-31",
            limit=20
        )
        print(f"✓ 组合条件查询: {combo_result.shape}")
        
        print("\n5.3 多表联合查询")
        print("-" * 40)
        
        # 多因子查询
        multifactor_result = db.query_data_multifactor(
            table_names=["adv_query_prices", "adv_query_volume"],
            stock_codes=["000001.SZ", "000002.SZ"],
            start_date="2023-01-01",
            end_date="2023-01-31"
        )
        
        if multifactor_result is not None:
            print(f"✓ 多因子查询: {multifactor_result.shape}")
            print("✓ 查询列:", multifactor_result.columns.tolist())
        else:
            print("✗ 多因子查询失败")
        
        print("\n5.4 聚合查询功能")
        print("-" * 40)
        
        # 获取数据进行聚合分析
        monthly_data = db.query_data(
            "adv_query_prices",
            start_date="2023-01-01",
            end_date="2023-12-31"
        )
        
        if monthly_data is not None:
            # 转换datetime列
            monthly_data['datetime'] = pd.to_datetime(monthly_data['datetime'])
            monthly_data.set_index('datetime', inplace=True)
            
            # 月度聚合
            monthly_avg = monthly_data.resample('M').mean()
            print(f"✓ 月度平均价格: {monthly_avg.shape}")
            print("前3个月数据:")
            print(monthly_avg.head(3).round(2))
            
            # 季度聚合
            quarterly_avg = monthly_data.resample('Q').mean()
            print(f"✓ 季度平均价格: {quarterly_avg.shape}")

def demonstrate_backup_restore():
    """演示备份恢复功能"""
    print("\n" + "=" * 60)
    print("6. 备份恢复功能演示")
    print("=" * 60)
    
    print("\n6.1 数据导出功能")
    print("-" * 40)
    
    with PostgreSQLManager() as db:
        # 确保有数据可以导出
        if db.table_exists("validation_test"):
            # 导出到CSV
            export_path = "backup_test_export.csv"
            success = db.export_to_csv("validation_test", export_path)
            
            if success and os.path.exists(export_path):
                print(f"✓ 数据导出成功: {export_path}")
                
                # 验证导出文件
                exported_data = pd.read_csv(export_path)
                print(f"✓ 导出数据验证: {exported_data.shape}")
                
                # 清理导出文件
                os.remove(export_path)
                print("✓ 清理导出文件")
            else:
                print("✗ 数据导出失败")
        
        print("\n6.2 表结构信息")
        print("-" * 40)
        
        # 获取表信息
        tables = db.list_tables()
        test_tables = [t for t in tables if 'test' in t or 'batch' in t or 'adv_query' in t]
        
        print(f"✓ 发现测试表: {len(test_tables)} 个")
        
        for table in test_tables[:5]:  # 只显示前5个
            info = db.get_table_info(table)
            print(f"  {table}: {info['row_count']} 行, {info['column_count']} 列")

def cleanup_advanced_examples():
    """清理高级功能示例数据"""
    print("\n" + "=" * 60)
    print("7. 清理高级功能示例数据")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        tables = db.list_tables()
        cleanup_tables = [t for t in tables if any(prefix in t for prefix in [
            'batch_', 'perf_test_', 'error_test_', 'validation_test', 
            'duplicate_test', 'adv_query_'
        ])]
        
        print(f"发现 {len(cleanup_tables)} 个需要清理的表")
        
        for table in cleanup_tables:
            success = db.drop_table(table)
            if success:
                print(f"✓ 删除表: {table}")
            else:
                print(f"✗ 删除失败: {table}")

def main():
    """主函数"""
    print("PostgreSQL数据管理系统 - 高级功能示例")
    print("=" * 80)
    
    try:
        # 1. 批量操作演示
        demonstrate_batch_operations()
        
        # 2. 性能优化演示
        demonstrate_performance_optimization()
        
        # 3. 错误处理演示
        demonstrate_error_handling()
        
        # 4. 数据验证演示
        demonstrate_data_validation()
        
        # 5. 高级查询演示
        demonstrate_advanced_queries()
        
        # 6. 备份恢复演示
        demonstrate_backup_restore()
        
        # 7. 清理示例数据
        cleanup_advanced_examples()
        
        print("\n" + "=" * 80)
        print("✅ 高级功能示例演示完成！")
        print("=" * 80)
        
        print("\n📋 高级功能总结:")
        print("1. ✓ 批量操作：多数据源批量导入和查询")
        print("2. ✓ 性能优化：大数据量处理性能测试")
        print("3. ✓ 错误处理：各种异常情况的优雅处理")
        print("4. ✓ 数据验证：完整性、质量和异常值检测")
        print("5. ✓ 高级查询：条件查询、多表联合、聚合分析")
        print("6. ✓ 备份恢复：数据导出和表结构管理")
        print("7. ✓ 资源管理：自动清理和内存优化")
        
        print("\n💡 使用建议:")
        print("- 大数据量操作时建议使用批量功能")
        print("- 生产环境中启用完整的错误处理")
        print("- 定期进行数据质量验证")
        print("- 合理使用查询条件以提高性能")
        
    except Exception as e:
        print(f"\n❌ 高级功能演示过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()