"""
使用 sample_data 文件夹中的真实数据测试 EasyManager
"""

import pandas as pd
from easy_manager import EasyManager
import os

def test_with_sample_data():
    """使用示例数据测试"""
    
    
    print("=" * 80)
    print("使用真实样本数据测试 EasyManager")
    print("=" * 80)
    
    # 检查 sample_data 文件夹是否存在
    if not os.path.exists('sample_data'):
        print("错误: sample_data 文件夹不存在")
        return
    
    try:
        with EasyManager(database='test_data_base') as em:
            
            # 1. 测试 Fundamental_PB_Ratio
            print("\n" + "=" * 80)
            print("测试1: Fundamental PB Ratio 数据")
            print("=" * 80)
            
            df_pb = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
            print(f"数据形状: {df_pb.shape}")
            print(f"日期范围: {df_pb.index[0]} 到 {df_pb.index[-1]}")
            print(f"股票数量: {len(df_pb.columns)}")
            print(f"\n前3行数据:")
            print(df_pb.head(3))
            
            print("\n创建表 sample_pb_ratio...")
            em.create_table('sample_pb_ratio', df_pb, overwrite=True)
            
            info = em.get_table_info('sample_pb_ratio')
            print(f"✓ 表创建成功: {info['row_count']} 行, {len(info['columns'])} 列")
            
            # 2. 测试 Price_Close
            print("\n" + "=" * 80)
            print("测试2: Price Close 数据")
            print("=" * 80)
            
            df_price = pd.read_csv('sample_data/Price_Close.csv', index_col=0)
            print(f"数据形状: {df_price.shape}")
            print(f"日期范围: {df_price.index[0]} 到 {df_price.index[-1]}")
            print(f"股票数量: {len(df_price.columns)}")
            
            print("\n创建表 sample_price_close...")
            em.create_table('sample_price_close', df_price, overwrite=True)
            
            info = em.get_table_info('sample_price_close')
            print(f"✓ 表创建成功: {info['row_count']} 行, {len(info['columns'])} 列")
            
            # 3. 测试 Technical_RSI
            print("\n" + "=" * 80)
            print("测试3: Technical RSI 数据")
            print("=" * 80)
            
            df_rsi = pd.read_csv('sample_data/Technical_RSI.csv', index_col=0)
            print(f"数据形状: {df_rsi.shape}")
            print(f"日期范围: {df_rsi.index[0]} 到 {df_rsi.index[-1]}")
            
            print("\n创建表 sample_technical_rsi...")
            em.create_table('sample_technical_rsi', df_rsi, overwrite=True)
            
            info = em.get_table_info('sample_technical_rsi')
            print(f"✓ 表创建成功: {info['row_count']} 行, {len(info['columns'])} 列")
            
            # 4. 列出所有表
            print("\n" + "=" * 80)
            print("当前数据库中的所有表")
            print("=" * 80)
            
            tables = em.list_tables()
            for i, table in enumerate(tables, 1):
                info = em.get_table_info(table)
                print(f"{i}. {table}: {info['row_count']} 行, {len(info['columns'])} 列")
            
            # 5. 测试数据导入和查询
            print("\n" + "=" * 80)
            print("测试数据导入")
            print("=" * 80)
            
            print("\n从数据库导入 sample_pb_ratio 表（前10行）...")
            df_loaded = em.load_table('sample_pb_ratio', limit=10)
            if df_loaded is not None:
                print(f"✓ 导入成功，形状: {df_loaded.shape}")
                print("\n数据预览:")
                print(df_loaded)
            
            # 6. 测试增量插入
            print("\n" + "=" * 80)
            print("测试增量插入数据")
            print("=" * 80)
            
            # 取 PB Ratio 的一部分新数据
            df_new_data = df_pb.iloc[-50:]  # 最后50行作为"新数据"
            print(f"准备插入 {len(df_new_data)} 行新数据...")
            
            # 记录插入前的行数
            info_before = em.get_table_info('sample_pb_ratio')
            print(f"插入前行数: {info_before['row_count']}")
            
            # 插入数据（去重）
            em.insert_data('sample_pb_ratio', df_new_data, deduplicate=True)
            
            # 检查插入后的行数
            info_after = em.get_table_info('sample_pb_ratio')
            print(f"插入后行数: {info_after['row_count']}")
            
            if info_after['row_count'] == info_before['row_count']:
                print("✓ 去重功能正常：没有插入重复数据")
            else:
                print(f"插入了 {info_after['row_count'] - info_before['row_count']} 行新数据")
            
            # 7. 对比原始数据和导入的数据
            print("\n" + "=" * 80)
            print("验证数据完整性")
            print("=" * 80)
            
            print("\n导入完整的 sample_price_close 表...")
            df_loaded_full = em.load_table('sample_price_close')
            
            if df_loaded_full is not None:
                print(f"原始数据形状: {df_price.shape}")
                print(f"导入数据形状: {df_loaded_full.shape}")
                
                # 检查行数（加上datetime列）
                expected_cols = len(df_price.columns) + 1  # +1 for datetime column
                if len(df_loaded_full.columns) == expected_cols:
                    print("✓ 列数匹配")
                else:
                    print(f"列数不匹配: 期望 {expected_cols}, 实际 {len(df_loaded_full.columns)}")
                
                if len(df_loaded_full) == len(df_price):
                    print("✓ 行数匹配")
                else:
                    print(f"行数不匹配: 期望 {len(df_price)}, 实际 {len(df_loaded_full)}")
            
            # 8. 清理测试数据
            print("\n" + "=" * 80)
            print("清理测试数据")
            print("=" * 80)
            
            response = input("\n是否删除测试表? (y/n): ").strip().lower()
            if response == 'y':
                print("\n删除测试表...")
                em.drop_table('sample_pb_ratio')
                em.drop_table('sample_price_close')
                em.drop_table('sample_technical_rsi')
                print("✓ 测试表已删除")
            else:
                print("保留测试表")
                print("\n如需手动删除，可运行:")
                print("  em.drop_table('sample_pb_ratio')")
                print("  em.drop_table('sample_price_close')")
                print("  em.drop_table('sample_technical_rsi')")
        
        print("\n" + "=" * 80)
        print("测试完成！")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


def show_sample_data_info():
    """显示 sample_data 文件夹中的文件信息"""
    
    print("\n" + "=" * 80)
    print("Sample Data 文件夹内容")
    print("=" * 80)
    
    if not os.path.exists('sample_data'):
        print("错误: sample_data 文件夹不存在")
        return
    
    csv_files = [f for f in os.listdir('sample_data') if f.endswith('.csv')]
    
    for i, filename in enumerate(sorted(csv_files), 1):
        filepath = os.path.join('sample_data', filename)
        try:
            df = pd.read_csv(filepath, index_col=0)
            print(f"\n{i}. {filename}")
            print(f"   形状: {df.shape} (行数 x 列数)")
            print(f"   日期范围: {df.index[0]} 到 {df.index[-1]}")
            print(f"   股票数量: {len(df.columns)}")
        except Exception as e:
            print(f"\n{i}. {filename}")
            print(f"   错误: {str(e)}")


if __name__ == "__main__":
    # 显示示例数据信息
    show_sample_data_info()
    
    print("\n")
    input("按 Enter 继续测试...")
    
    # 运行测试
    test_with_sample_data()

