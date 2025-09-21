"""
PostgreSQL数据管理系统使用示例
演示如何使用PostgreSQLManager进行数据管理操作
"""

import pandas as pd
import numpy as np
from pathlib import Path
from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

def demo_basic_operations():
    """演示基本操作"""
    print("=" * 60)
    print("PostgreSQL数据管理系统 - 基本操作演示")
    print("=" * 60)
    
    # 1. 创建数据库管理器实例
    print("\n1. 连接数据库")
    with PostgreSQLManager() as db:
        print("✓ 数据库连接成功")
        
        # 2. 创建表格
        print("\n2. 创建表格")
        sample_file = Path("sample_data/Fundamental_PB_Ratio.csv")
        if sample_file.exists():
            table_name = "demo_pb_ratio"
            success = db.create_table(table_name, str(sample_file), overwrite=True)
            if success:
                print(f"✓ 成功创建表: {table_name}")
                
                # 3. 查询数据
                print("\n3. 查询数据")
                df = db.query_data(table_name, limit=10)
                if df is not None:
                    print(f"✓ 查询成功，数据形状: {df.shape}")
                    print(f"  - 日期范围: {df.index.min()} 到 {df.index.max()}")
                    print(f"  - 列数: {len(df.columns)}")
                
                # 4. 时间切片查询
                print("\n4. 时间切片查询")
                df_slice = db.query_data(
                    table_name, 
                    start_date='2020-01-01', 
                    end_date='2020-01-31'
                )
                if df_slice is not None:
                    print(f"✓ 时间切片查询成功，数据形状: {df_slice.shape}")
                
                # 5. 获取表信息
                print("\n5. 表信息")
                info = db.get_table_info(table_name)
                print(f"✓ 表信息: 行数={info.get('row_count', 0)}, 列数={info.get('column_count', 0)}")
                
                # 6. 演示完成
                print("\n6. 演示完成")
                print(f"✓ 表 {table_name} 保留在数据库中供后续使用")
        else:
            print("✗ 样本数据文件不存在")

def demo_advanced_operations():
    """演示高级操作"""
    print("\n" + "=" * 60)
    print("PostgreSQL数据管理系统 - 高级操作演示")
    print("=" * 60)
    
    # 使用高级管理器
    with AdvancedPostgreSQLManager() as db:
        print("✓ 高级数据库管理器连接成功")
        
        # 1. 批量导入多个文件
        print("\n1. 批量导入文件")
        sample_dir = Path("sample_data")
        csv_files = list(sample_dir.glob("*.csv"))[:3]  # 只处理前3个文件
        
        if csv_files:
            table_names = []
            for csv_file in csv_files:
                table_name = f"demo_{csv_file.stem.lower()}"
                success = db.create_table(table_name, str(csv_file), overwrite=True)
                if success:
                    table_names.append(table_name)
                    print(f"✓ 导入文件: {csv_file.name} -> {table_name}")
            
            # 2. 数据统计
            print("\n2. 数据统计")
            for table_name in table_names:
                stats = db.get_data_statistics(table_name)
                print(f"✓ {table_name}: {stats}")
            
            # 3. 数据导出
            print("\n3. 数据导出")
            if table_names:
                export_file = f"export_{table_names[0]}.csv"
                success = db.export_data(table_names[0], export_file, limit=100)
                if success:
                    print(f"✓ 导出数据到: {export_file}")
            
            # 4. 演示多因子查询功能
            print("\n4. 演示多因子查询功能")
            if len(table_names) >= 2:
                multifactor_data = db.query_data_multifactor(
                    table_names=table_names[:2],
                    start_date='2020-01-01',
                    end_date='2020-01-05'
                )
                
                if multifactor_data is not None:
                    print(f"✓ 多因子查询结果形状: {multifactor_data.shape}")
            
            # 5. 演示多因子查询功能
            print("\n5. 演示多因子查询功能")
            if len(table_names) >= 2:
                multifactor_data = db.query_data_multifactor(
                    table_names=table_names[:2],
                    start_date='2020-01-01',
                    end_date='2020-01-05'
                )
                
                if multifactor_data is not None:
                    print(f"✓ 多因子查询结果形状: {multifactor_data.shape}")
            
            # 6. 演示完成
            print("\n6. 演示完成")
            for table_name in table_names:
                print(f"✓ 表 {table_name} 保留在数据库中供后续使用")
        else:
            print("✗ 没有找到样本数据文件")

def demo_data_operations():
    """演示数据操作"""
    print("\n" + "=" * 60)
    print("PostgreSQL数据管理系统 - 数据操作演示")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        # 1. 创建测试数据
        print("\n1. 创建测试数据")
        dates = pd.date_range('2020-01-01', periods=10, freq='D')
        test_data = pd.DataFrame({
            'stock_a': np.random.randn(10),
            'stock_b': np.random.randn(10),
            'stock_c': np.random.randn(10)
        }, index=dates)
        
        table_name = "demo_test_data"
        success = db.create_table(table_name, test_data, overwrite=True)
        if success:
            print(f"✓ 创建测试表: {table_name}")
            print(f"  数据形状: {test_data.shape}")
        
        # 2. 插入新数据
        print("\n2. 插入新数据")
        new_dates = pd.date_range('2020-01-11', periods=5, freq='D')
        new_data = pd.DataFrame({
            'stock_a': np.random.randn(5),
            'stock_b': np.random.randn(5),
            'stock_c': np.random.randn(5)
        }, index=new_dates)
        
        success = db.insert_data(table_name, new_data)
        if success:
            print(f"✓ 插入新数据成功")
        
        # 3. 查询合并后的数据
        print("\n3. 查询合并数据")
        all_data = db.query_data(table_name)
        if all_data is not None:
            print(f"✓ 合并后数据形状: {all_data.shape}")
            print(f"  日期范围: {all_data.index.min()} 到 {all_data.index.max()}")
        
        # 4. 条件查询
        print("\n4. 条件查询")
        filtered_data = db.query_data(
            table_name,
            start_date='2020-01-05',
            end_date='2020-01-10',
            columns=['stock_a', 'stock_b']
        )
        if filtered_data is not None:
            print(f"✓ 条件查询成功，数据形状: {filtered_data.shape}")
        
        # 5. 演示完成
        print("\n5. 演示多因子查询功能")
        # 查询多个因子的数据
        factor_tables = ['demo_fundamental_pb_ratio', 'demo_fundamental_pe_ratio']
        multifactor_data = db.query_data_multifactor(
            table_names=factor_tables,
            start_date='2020-01-01',
            end_date='2020-01-05',
            codes=['000001.SZ', '000002.SZ']
        )
        
        if multifactor_data is not None:
            print(f"多因子查询结果形状: {multifactor_data.shape}")
            print("前5行数据:")
            print(multifactor_data.head())
        
        print("\n6. 演示完成")
        print(f"✓ 表 {table_name} 保留在数据库中供后续使用")

def main():
    """主函数"""
    print("PostgreSQL数据管理系统演示")
    print("作者: AI Assistant")
    print("版本: 1.0")
    print("=" * 60)
    
    try:
        # 基本操作演示
        demo_basic_operations()
        
        # 高级操作演示
        demo_advanced_operations()
        
        # 数据操作演示
        demo_data_operations()
        
        print("\n" + "=" * 60)
        print("✓ 所有演示完成!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 演示过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()