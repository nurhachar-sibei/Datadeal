"""
简单测试脚本 - 测试PostgreSQL数据管理系统基本功能

Author: Nurhachar
Date: 2025
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
import os

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from postgres_manager import PostgreSQLManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_connection():
    """测试数据库连接"""
    print("=" * 50)
    print("测试数据库连接")
    print("=" * 50)
    
    try:
        with PostgreSQLManager() as db:
            print("✓ 数据库连接成功")
            tables = db.list_tables()
            print(f"✓ 当前表数量: {len(tables)}")
            return True
    except Exception as e:
        print(f"✗ 数据库连接失败: {str(e)}")
        return False

def test_create_table():
    """测试创建表格"""
    print("=" * 50)
    print("测试创建表格")
    print("=" * 50)
    
    try:
        with PostgreSQLManager() as db:
            # 检查sample_data目录
            sample_dir = Path("sample_data")
            if not sample_dir.exists():
                print(f"✗ 样本数据目录不存在: {sample_dir}")
                return False
            
            # 查找CSV文件
            csv_files = list(sample_dir.glob("*.csv"))
            if not csv_files:
                print("✗ 没有找到CSV文件")
                return False
            
            print(f"✓ 找到 {len(csv_files)} 个CSV文件")
            
            # 测试创建第一个表
            test_file = csv_files[0]
            table_name = f"test_{test_file.stem.lower()}"
            
            print(f"正在创建表: {table_name}")
            success = db.create_table(table_name, str(test_file), overwrite=True)
            
            if success:
                print(f"✓ 成功创建表: {table_name}")
                
                # 获取表信息
                info = db.get_table_info(table_name)
                print(f"  - 行数: {info.get('row_count', 0)}")
                print(f"  - 列数: {info.get('column_count', 0)}")
                
                return True
            else:
                print(f"✗ 创建表失败: {table_name}")
                return False
                
    except Exception as e:
        print(f"✗ 创建表过程出错: {str(e)}")
        return False

def test_query_data():
    """测试数据查询"""
    print("=" * 50)
    print("测试数据查询")
    print("=" * 50)
    
    try:
        with PostgreSQLManager() as db:
            tables = db.list_tables()
            test_tables = [t for t in tables if t.startswith('test_')]
            
            if not test_tables:
                print("✗ 没有找到测试表")
                return False
            
            table_name = test_tables[0]
            print(f"查询表: {table_name}")
            
            # 查询前10行
            df = db.query_data(table_name, limit=10)
            if df is not None:
                print(f"✓ 查询成功")
                print(f"  - 数据形状: {df.shape}")
                print(f"  - 列名: {list(df.columns)[:5]}...")  # 只显示前5列
                print(f"  - 索引类型: {type(df.index)}")
                
                if hasattr(df.index, 'min') and hasattr(df.index, 'max'):
                    print(f"  - 日期范围: {df.index.min()} 到 {df.index.max()}")
                
                return True
            else:
                print("✗ 查询失败")
                return False
                
    except Exception as e:
        print(f"✗ 查询过程出错: {str(e)}")
        return False

def test_time_slice():
    """测试时间切片"""
    print("=" * 50)
    print("测试时间切片")
    print("=" * 50)
    
    try:
        with PostgreSQLManager() as db:
            tables = db.list_tables()
            test_tables = [t for t in tables if t.startswith('test_')]
            
            if not test_tables:
                print("✗ 没有找到测试表")
                return False
            
            table_name = test_tables[0]
            print(f"时间切片表: {table_name}")
            
            # 获取数据的日期范围
            df_full = db.query_data(table_name, limit=100)
            if df_full is None or len(df_full) == 0:
                print("✗ 无法获取数据")
                return False
            
            # 选择一个日期范围进行切片
            start_date = df_full.index.min()
            end_date = df_full.index.min() + pd.Timedelta(days=10)
            
            print(f"切片日期范围: {start_date} 到 {end_date}")
            
            df_slice = db.time_slice(table_name, str(start_date.date()), str(end_date.date()))
            
            if df_slice is not None:
                print(f"✓ 时间切片成功")
                print(f"  - 切片数据形状: {df_slice.shape}")
                print(f"  - 实际日期范围: {df_slice.index.min()} 到 {df_slice.index.max()}")
                return True
            else:
                print("✗ 时间切片失败")
                return False
                
    except Exception as e:
        print(f"✗ 时间切片过程出错: {str(e)}")
        return False

def test_insert_data():
    """测试数据插入"""
    print("=" * 50)
    print("测试数据插入")
    print("=" * 50)
    
    try:
        with PostgreSQLManager() as db:
            tables = db.list_tables()
            test_tables = [t for t in tables if t.startswith('test_')]
            
            if not test_tables:
                print("✗ 没有找到测试表")
                return False
            
            table_name = test_tables[0]
            print(f"插入数据到表: {table_name}")
            
            # 获取现有数据结构
            df_existing = db.query_data(table_name, limit=5)
            if df_existing is None:
                print("✗ 无法获取现有数据结构")
                return False
            
            # 创建新数据
            last_date = df_existing.index.max()
            new_date = last_date + pd.Timedelta(days=1)
            
            # 创建一行新数据
            new_data = pd.DataFrame(
                [np.random.randn(len(df_existing.columns))],
                index=[new_date],
                columns=df_existing.columns
            )
            
            print(f"插入新数据日期: {new_date}")
            
            # 插入数据
            success = db.insert_data(table_name, new_data)
            
            if success:
                print("✓ 数据插入成功")
                
                # 验证插入
                df_verify = db.query_data(table_name, 
                                        start_date=str(new_date.date()),
                                        end_date=str(new_date.date()))
                
                if df_verify is not None and len(df_verify) > 0:
                    print("✓ 插入数据验证成功")
                    return True
                else:
                    print("✗ 插入数据验证失败")
                    return False
            else:
                print("✗ 数据插入失败")
                return False
                
    except Exception as e:
        print(f"✗ 数据插入过程出错: {str(e)}")
        return False

def cleanup():
    """清理测试数据"""
    print("=" * 50)
    print("清理测试数据")
    print("=" * 50)
    
    try:
        with PostgreSQLManager() as db:
            tables = db.list_tables()
            test_tables = [t for t in tables if t.startswith('test_')]
            
            for table in test_tables:
                try:
                    db.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    db.conn.commit()
                    print(f"✓ 删除表: {table}")
                except Exception as e:
                    print(f"✗ 删除表 {table} 失败: {str(e)}")
                    
    except Exception as e:
        print(f"✗ 清理过程出错: {str(e)}")

def main():
    """主测试函数"""
    print("PostgreSQL数据管理系统 - 简单功能测试")
    print("=" * 50)
    
    tests = [
        ("数据库连接", test_connection),
        ("创建表格", test_create_table),
        ("数据查询", test_query_data),
        ("时间切片", test_time_slice),
        ("数据插入", test_insert_data),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"✗ 测试 {test_name} 出现异常: {str(e)}")
            results[test_name] = False
        
        print()  # 空行分隔
    
    # 显示测试结果摘要
    print("=" * 50)
    print("测试结果摘要")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n总计: {passed}/{total} 个测试通过")
    
    # 询问是否清理
    if passed > 0:
        response = input("\n是否清理测试数据? (y/n): ")
        if response.lower() == 'y':
            cleanup()

if __name__ == "__main__":
    main()