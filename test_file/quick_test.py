"""
EasyManager 快速测试
简单验证基本功能是否正常
"""

import pandas as pd
from easy_manager import EasyManager

def quick_test():
    """快速测试基本功能"""
    
    print("=" * 60)
    print("EasyManager 快速测试")
    print("=" * 60)
    
    try:
        # 创建管理器
        with EasyManager(database='test_data_base') as em:
            print("\n✓ 数据库连接成功")
            
            # 创建测试数据
            print("\n创建测试数据...")
            test_data = {
                'datetime': pd.date_range('2020-01-01', periods=10),
                'stock_A': [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0],
                'stock_B': [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0],
                'stock_C': [3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0]
            }
            df = pd.DataFrame(test_data)
            df = df.set_index('datetime')
            print(f"测试数据形状: {df.shape}")
            print(f"\n{df.head()}")
            
            # 测试1: 创建表
            print("\n" + "-" * 60)
            print("测试1: 创建表")
            print("-" * 60)
            success = em.create_table('quick_test_table', df, overwrite=True)
            if success:
                print("✓ 创建表成功")
            else:
                print("✗ 创建表失败")
                return
            
            # 测试2: 列出表
            print("\n" + "-" * 60)
            print("测试2: 列出所有表")
            print("-" * 60)
            tables = em.list_tables()
            print(f"找到 {len(tables)} 个表")
            if 'quick_test_table' in tables:
                print("✓ 找到测试表")
            
            # 测试3: 获取表信息
            print("\n" + "-" * 60)
            print("测试3: 获取表信息")
            print("-" * 60)
            info = em.get_table_info('quick_test_table')
            print(f"表名: {info['table_name']}")
            print(f"行数: {info['row_count']}")
            print(f"列数: {len(info['columns'])}")
            if info['row_count'] == 10:
                print("✓ 数据行数正确")
            
            # 测试4: 导入表
            print("\n" + "-" * 60)
            print("测试4: 从数据库导入表")
            print("-" * 60)
            df_loaded = em.load_table('quick_test_table')
            if df_loaded is not None:
                print(f"✓ 导入成功，形状: {df_loaded.shape}")
                print(f"\n{df_loaded.head()}")
            else:
                print("✗ 导入失败")
            
            # 测试5: 插入重复数据（应该被过滤）
            print("\n" + "-" * 60)
            print("测试5: 插入重复数据（测试去重）")
            print("-" * 60)
            success = em.insert_data('quick_test_table', df.head(5), deduplicate=True)
            if success:
                info = em.get_table_info('quick_test_table')
                if info['row_count'] == 10:
                    print("✓ 去重成功，行数保持不变")
                else:
                    print(f"✗ 去重失败，行数变为 {info['row_count']}")
            
            # 测试6: 插入新数据
            print("\n" + "-" * 60)
            print("测试6: 插入新数据")
            print("-" * 60)
            new_data = {
                'datetime': pd.date_range('2020-01-11', periods=5),
                'stock_A': [2.1, 2.2, 2.3, 2.4, 2.5],
                'stock_B': [3.1, 3.2, 3.3, 3.4, 3.5],
                'stock_C': [4.1, 4.2, 4.3, 4.4, 4.5]
            }
            df_new = pd.DataFrame(new_data)
            df_new = df_new.set_index('datetime')
            
            success = em.insert_data('quick_test_table', df_new, deduplicate=True)
            if success:
                info = em.get_table_info('quick_test_table')
                if info['row_count'] == 15:
                    print("✓ 插入新数据成功，行数: 10 -> 15")
                else:
                    print(f"行数: {info['row_count']}")
            
            # 测试7: 删除表
            print("\n" + "-" * 60)
            print("测试7: 删除表")
            print("-" * 60)
            success = em.drop_table('quick_test_table')
            if success:
                print("✓ 删除表成功")
            else:
                print("✗ 删除表失败")
            
            # 验证删除
            tables = em.list_tables()
            if 'quick_test_table' not in tables:
                print("✓ 验证成功：表已被删除")
            else:
                print("✗ 验证失败：表仍然存在")
            
        print("\n" + "=" * 60)
        print("所有测试完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    quick_test()

