"""
EasyManager 测试脚本
测试所有核心功能：创建表、插入数据（去重）、删除表、导入表
"""

import pandas as pd
from easy_manager import EasyManager

def test_easy_manager():
    """测试EasyManager的所有功能"""
    
    print("=" * 80)
    print("开始测试 EasyManager")
    print("=" * 80)
    
    # 创建管理器实例
    with EasyManager(database='test_data_base') as em:
        
        # 1. 列出现有表
        print("\n【步骤1】列出现有表")
        tables = em.list_tables()
        print(f"现有表: {tables}")
        
        # 2. 加载示例数据
        print("\n【步骤2】加载示例数据")
        df_pb = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
        print(f"PB Ratio数据形状: {df_pb.shape}")
        print(f"前5行数据:\n{df_pb.head()}")
        
        # 3. 创建表并插入数据
        print("\n【步骤3】创建表 test_pb_ratio 并插入数据")
        success = em.create_table('test_pb_ratio', df_pb, overwrite=True)
        if success:
            print("✓ 表创建成功")
        else:
            print("✗ 表创建失败")
        
        # 4. 获取表信息
        print("\n【步骤4】获取表信息")
        info = em.get_table_info('test_pb_ratio')
        print(f"表名: {info.get('table_name')}")
        print(f"行数: {info.get('row_count')}")
        print(f"列数: {len(info.get('columns', []))}")
        
        # 5. 从数据库导入表
        print("\n【步骤5】从数据库导入表")
        df_loaded = em.load_table('test_pb_ratio', limit=10)
        if df_loaded is not None:
            print(f"✓ 成功导入数据，形状: {df_loaded.shape}")
            print(f"前5行:\n{df_loaded.head()}")
        
        # 6. 测试插入重复数据（应该被过滤）
        print("\n【步骤6】测试插入重复数据（应该被过滤）")
        df_duplicate = df_pb.head(100)  # 取前100行，这些都是重复的
        success = em.insert_data('test_pb_ratio', df_duplicate, deduplicate=True)
        if success:
            print("✓ 重复数据已被过滤")
        
        # 7. 测试插入新数据
        print("\n【步骤7】测试插入新数据")
        # 加载另一个文件的部分数据作为"新数据"
        df_pe = pd.read_csv('sample_data/Fundamental_PE_Ratio.csv', index_col=0)
        df_new = df_pe.head(50)  # 取PE ratio的前50行作为新数据
        
        # 创建一个新表来测试
        print("  创建新表 test_pe_ratio")
        em.create_table('test_pe_ratio', df_pe.head(100), overwrite=True)
        
        print("  插入额外的新数据")
        success = em.insert_data('test_pe_ratio', df_pe.iloc[100:150], deduplicate=True)
        if success:
            print("✓ 新数据插入成功")
        
        # 验证数据量
        info = em.get_table_info('test_pe_ratio')
        print(f"  表 test_pe_ratio 现有行数: {info.get('row_count')}")
        
        # 8. 列出所有表
        print("\n【步骤8】列出所有表")
        tables = em.list_tables()
        print(f"数据库中的表: {tables}")
        
        # 9. 删除测试表
        print("\n【步骤9】删除测试表")
        print("  删除 test_pb_ratio...")
        em.drop_table('test_pb_ratio')
        
        print("  删除 test_pe_ratio...")
        em.drop_table('test_pe_ratio')
        
        # 10. 验证删除
        print("\n【步骤10】验证删除")
        tables = em.list_tables()
        print(f"剩余的表: {tables}")
        
        # 11. 创建多个表的综合测试
        print("\n【步骤11】综合测试 - 创建多个表")
        
        # 创建Price_Close表
        df_price = pd.read_csv('sample_data/Price_Close.csv', index_col=0)
        em.create_table('test_price_close', df_price.head(200), overwrite=True)
        print("✓ 创建表 test_price_close")
        
        # 创建Technical_RSI表
        df_rsi = pd.read_csv('sample_data/Technical_RSI.csv', index_col=0)
        em.create_table('test_technical_rsi', df_rsi.head(200), overwrite=True)
        print("✓ 创建表 test_technical_rsi")
        
        # 列出所有表
        tables = em.list_tables()
        print(f"\n当前数据库中的表: {tables}")
        
        # 导入并显示数据
        print("\n【步骤12】导入表并显示统计信息")
        for table in ['test_price_close', 'test_technical_rsi']:
            df = em.load_table(table, limit=5)
            if df is not None:
                print(f"\n表 {table}:")
                print(f"  形状: {df.shape}")
                print(f"  列: {df.columns.tolist()}")
        
        # 清理测试数据
        print("\n【步骤13】清理测试数据")
        em.drop_table('test_price_close')
        em.drop_table('test_technical_rsi')
        print("✓ 测试数据已清理")
    
    print("\n" + "=" * 80)
    print("测试完成！")
    print("=" * 80)


def demo_basic_usage():
    """演示基本使用方法"""
    
    print("\n" + "=" * 80)
    print("基本使用示例")
    print("=" * 80)
    
    # 创建EasyManager实例
    with EasyManager(database='test_data_base') as em:
        
        # 读取CSV文件
        df = pd.read_csv('sample_data/Fundamental_ROE.csv', index_col=0)
        print(f"\n读取数据: {df.shape}")
        
        # 创建表
        print("\n创建表 demo_roe...")
        em.create_table('demo_roe', df, overwrite=True)
        
        # 导入表
        print("\n从数据库导入表...")
        df_loaded = em.load_table('demo_roe')
        print(f"导入成功: {df_loaded.shape}")
        
        # 获取表信息
        info = em.get_table_info('demo_roe')
        print(f"\n表信息:")
        print(f"  表名: {info['table_name']}")
        print(f"  行数: {info['row_count']}")
        print(f"  列数: {len(info['columns'])}")
        
        # 删除表
        print("\n删除表 demo_roe...")
        em.drop_table('demo_roe')
        print("完成！")


if __name__ == "__main__":
    # 运行完整测试
    test_easy_manager()
    
    # 运行基本使用示例
    demo_basic_usage()

