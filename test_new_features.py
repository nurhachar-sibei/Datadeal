"""
测试 EasyManager 的新功能
1. add_columns - 添加新列
2. insert_data 的三种模式：skip、update、append
"""

import pandas as pd
import numpy as np
from easy_manager import EasyManager

def test_add_columns():
    """测试添加新列功能"""
    
    print("=" * 80)
    print("测试1: 添加新列功能 (add_columns)")
    print("=" * 80)
    
    with EasyManager() as em:
        # 创建初始数据
        print("\n【步骤1】创建初始表")
        initial_data = {
            'datetime': pd.date_range('2020-01-01', periods=10),
            'stock_A': np.random.randn(10),
            'stock_B': np.random.randn(10)
        }
        df_initial = pd.DataFrame(initial_data)
        df_initial = df_initial.set_index('datetime')
        
        print(f"初始数据形状: {df_initial.shape}")
        print(f"初始列: {df_initial.columns.tolist()}")
        print(df_initial.head(3))
        
        # 创建表
        em.create_table('test_add_columns', df_initial, overwrite=True)
        
        # 创建包含新列的数据
        print("\n【步骤2】准备添加新列")
        new_data = {
            'datetime': pd.date_range('2020-01-01', periods=10),
            'stock_A': np.random.randn(10),  # 已存在的列
            'stock_C': np.random.randn(10),  # 新列
            'stock_D': np.random.randn(10)   # 新列
        }
        df_new = pd.DataFrame(new_data)
        df_new = df_new.set_index('datetime')
        
        print(f"新数据形状: {df_new.shape}")
        print(f"新数据列: {df_new.columns.tolist()}")
        print(f"新列: stock_C, stock_D")
        
        # 添加新列
        print("\n【步骤3】添加新列到表中")
        em.add_columns('test_add_columns', df_new, merge_on_index=True)
        
        # 验证结果
        print("\n【步骤4】验证结果")
        df_result = em.load_table('test_add_columns')
        print(f"结果数据形状: {df_result.shape}")
        print(f"结果列: {df_result.columns.tolist()}")
        print("\n前5行数据:")
        print(df_result.head())
        
        # 获取表信息
        info = em.get_table_info('test_add_columns')
        print(f"\n表信息: {info['row_count']} 行, {len(info['columns'])} 列")
        
        # 清理
        em.drop_table('test_add_columns')
        print("\n✓ 测试完成，表已清理")


def test_insert_modes():
    """测试insert_data的三种模式"""
    
    print("\n" + "=" * 80)
    print("测试2: insert_data 的三种模式")
    print("=" * 80)
    
    with EasyManager() as em:
        # 准备初始数据
        print("\n【步骤1】创建初始表")
        initial_data = {
            'datetime': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05'],
            'stock_A': [1.0, 2.0, 3.0, 4.0, 5.0],
            'stock_B': [10.0, 20.0, 30.0, 40.0, 50.0]
        }
        df_initial = pd.DataFrame(initial_data)
        df_initial['datetime'] = pd.to_datetime(df_initial['datetime'])
        df_initial = df_initial.set_index('datetime')
        
        print("初始数据:")
        print(df_initial)
        
        em.create_table('test_insert_modes', df_initial, overwrite=True)
        
        # 测试模式1: skip（忽略重复索引）
        print("\n" + "-" * 80)
        print("【测试 skip 模式】忽略重复索引")
        print("-" * 80)
        
        skip_data = {
            'datetime': ['2020-01-03', '2020-01-04', '2020-01-06', '2020-01-07'],  # 前两个索引重复
            'stock_A': [333.0, 444.0, 6.0, 7.0],  # 注意：值不同但索引重复
            'stock_B': [3333.0, 4444.0, 60.0, 70.0]  # 也会被跳过
        }
        df_skip = pd.DataFrame(skip_data)
        df_skip['datetime'] = pd.to_datetime(df_skip['datetime'])
        df_skip = df_skip.set_index('datetime')
        
        print("尝试插入的数据（包含重复索引 01-03 和 01-04）:")
        print(df_skip)
        print("\n说明：虽然值不同，但因为索引（datetime）重复，前两行会被跳过")
        
        em.insert_data('test_insert_modes', df_skip, mode='skip')
        
        result = em.load_table('test_insert_modes')
        print("\n结果（应该只添加了 01-06 和 01-07，01-03 和 01-04 因索引重复被跳过）:")
        print(result.sort_values('datetime'))
        
        # 测试模式2: update（覆盖重复）
        print("\n" + "-" * 80)
        print("【测试 update 模式】覆盖重复数据")
        print("-" * 80)
        
        update_data = {
            'datetime': ['2020-01-03', '2020-01-04', '2020-01-08'],
            'stock_A': [333.0, 444.0, 8.0],      # 更新前两行
            'stock_B': [3333.0, 4444.0, 80.0]    # 更新前两行
        }
        df_update = pd.DataFrame(update_data)
        df_update['datetime'] = pd.to_datetime(df_update['datetime'])
        df_update = df_update.set_index('datetime')
        
        print("尝试插入/更新的数据:")
        print(df_update)
        
        em.insert_data('test_insert_modes', df_update, mode='update')
        
        result = em.load_table('test_insert_modes')
        print("\n结果（01-03和01-04应该被更新，01-08被插入）:")
        print(result.sort_values('datetime'))
        
        # 测试模式3: append（直接追加）
        print("\n" + "-" * 80)
        print("【测试 append 模式】直接追加，不检查重复")
        print("-" * 80)
        
        append_data = {
            'datetime': ['2020-01-09', '2020-01-09'],  # 故意重复
            'stock_A': [9.0, 9.9],
            'stock_B': [90.0, 99.0]
        }
        df_append = pd.DataFrame(append_data)
        df_append['datetime'] = pd.to_datetime(df_append['datetime'])
        df_append = df_append.set_index('datetime')
        
        print("尝试追加的数据（包含重复日期）:")
        print(df_append)
        
        em.insert_data('test_insert_modes', df_append, mode='append')
        
        result = em.load_table('test_insert_modes')
        print("\n结果（两行都被插入，即使日期重复）:")
        print(result.sort_values('datetime'))
        
        # 清理
        em.drop_table('test_insert_modes')
        print("\n✓ 测试完成，表已清理")


def test_with_sample_data():
    """使用真实样本数据测试新功能"""
    
    print("\n" + "=" * 80)
    print("测试3: 使用真实样本数据")
    print("=" * 80)
    
    with EasyManager() as em:
        # 加载样本数据
        print("\n【步骤1】加载 PB Ratio 数据")
        df_pb = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
        print(f"PB Ratio 数据: {df_pb.shape}")
        print(f"前3列: {df_pb.columns[:3].tolist()}")
        
        # 创建表，只使用前5列
        print("\n【步骤2】创建表（使用前5列）")
        df_initial = df_pb.iloc[:, :5]
        print(f"初始数据: {df_initial.shape}")
        em.create_table('test_sample_data', df_initial, overwrite=True)
        
        # 添加更多列
        print("\n【步骤3】添加新列（第6-10列）")
        df_new_cols = df_pb.iloc[:, 5:10]
        print(f"新列数据: {df_new_cols.shape}")
        print(f"新列: {df_new_cols.columns.tolist()}")
        
        em.add_columns('test_sample_data', df_new_cols, merge_on_index=True)
        
        # 验证
        result = em.load_table('test_sample_data')
        print(f"\n结果: {result.shape}")
        print(f"总列数: {len(result.columns)}")
        
        # 测试 update 模式
        print("\n【步骤4】测试 update 模式（修改前100行数据）")
        df_update = df_pb.head(100).copy()
        df_update = df_update * 2  # 所有值乘以2
        
        em.insert_data('test_sample_data', df_update, mode='update')
        
        result = em.load_table('test_sample_data', limit=5)
        print("\n更新后的前5行:")
        print(result)
        
        # 清理
        em.drop_table('test_sample_data')
        print("\n✓ 测试完成，表已清理")


def comprehensive_example():
    """综合示例：演示完整工作流"""
    
    print("\n" + "=" * 80)
    print("综合示例：完整工作流演示")
    print("=" * 80)
    
    with EasyManager() as em:
        print("\n场景：股票数据管理系统")
        print("-" * 80)
        
        # Day 1: 创建初始表
        print("\n【Day 1】创建初始股票价格表")
        day1_data = {
            'date': pd.date_range('2020-01-01', periods=5),
            'AAPL': [100, 101, 102, 103, 104],
            'GOOGL': [1000, 1010, 1020, 1030, 1040]
        }
        df_day1 = pd.DataFrame(day1_data).set_index('date')
        print(df_day1)
        
        em.create_table('stock_prices', df_day1, overwrite=True)
        print("✓ 表已创建")
        
        # Day 2: 添加新股票列
        print("\n【Day 2】添加新股票 MSFT 和 TSLA")
        day2_data = {
            'date': pd.date_range('2020-01-01', periods=5),
            'MSFT': [200, 202, 204, 206, 208],
            'TSLA': [500, 510, 520, 530, 540]
        }
        df_day2 = pd.DataFrame(day2_data).set_index('date')
        print(df_day2)
        
        em.add_columns('stock_prices', df_day2, merge_on_index=True)
        print("✓ 新列已添加")
        
        # Day 3: 插入新日期数据（skip模式）
        print("\n【Day 3】插入新日期数据（2020-01-06 到 2020-01-08）")
        day3_data = {
            'date': pd.date_range('2020-01-06', periods=3),
            'AAPL': [105, 106, 107],
            'GOOGL': [1050, 1060, 1070],
            'MSFT': [210, 212, 214],
            'TSLA': [550, 560, 570]
        }
        df_day3 = pd.DataFrame(day3_data).set_index('date')
        print(df_day3)
        
        em.insert_data('stock_prices', df_day3, mode='skip')
        print("✓ 新数据已插入")
        
        # Day 4: 修正错误数据（update模式）
        print("\n【Day 4】发现 2020-01-03 的数据有误，需要更新")
        day4_data = {
            'date': pd.to_datetime(['2020-01-03']),
            'AAPL': [102.5],       # 修正后的值
            'GOOGL': [1025],       # 修正后的值
            'MSFT': [205],         # 修正后的值
            'TSLA': [525]          # 修正后的值
        }
        df_day4 = pd.DataFrame(day4_data).set_index('date')
        print(df_day4)
        
        em.insert_data('stock_prices', df_day4, mode='update')
        print("✓ 数据已更新")
        
        # 查看最终结果
        print("\n【最终结果】当前表的所有数据")
        final_result = em.load_table('stock_prices')
        print(final_result.sort_values('date'))
        
        info = em.get_table_info('stock_prices')
        print(f"\n表统计: {info['row_count']} 行, {len(info['columns'])} 列")
        
        # 清理
        em.drop_table('stock_prices')
        print("\n✓ 示例完成，表已清理")


if __name__ == "__main__":
    # 运行所有测试
    try:
        # 测试1: 添加新列
        test_add_columns()
        
        # 测试2: 三种插入模式
        test_insert_modes()
        
        # 测试3: 使用真实样本数据
        print("\n是否运行样本数据测试？（需要 sample_data 文件夹）")
        response = input("输入 y 继续，其他键跳过: ").strip().lower()
        if response == 'y':
            test_with_sample_data()
        
        # 综合示例
        comprehensive_example()
        
        print("\n" + "=" * 80)
        print("所有测试完成！")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n测试失败: {str(e)}")
        import traceback
        traceback.print_exc()

