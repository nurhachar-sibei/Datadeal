"""
测试 LongManager 类
演示长格式数据管理的完整功能
"""

from easy_manager import LongManager
import pandas as pd

print("=" * 80)
print("LongManager 功能测试")
print("=" * 80)

with LongManager(time_col='datetime', entity_col='company') as lm:
    
    # ============================================================================
    # 测试 1: 显示帮助信息
    # ============================================================================
    
    print("\n【测试 1】显示帮助信息")
    print("-" * 80)
    LongManager.help()
    
    # ============================================================================
    # 测试 2: 创建表（使用测试样本数据）
    # ============================================================================
    
    print("\n【测试 2】创建长格式数据表")
    print("-" * 80)
    
    # 读取测试数据
    df = pd.read_csv('long_data/test_sample.csv')
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    print(f"原始数据形状: {df.shape}")
    print(f"列顺序: {list(df.columns)}")
    print("\n数据预览:")
    print(df.head(10))
    
    # 创建表
    success = lm.create_table('test_long_table', df, overwrite=True)
    
    if success:
        print("\n[成功] 表创建完成！")
        
        # 查看表信息
        lm.get_table_info('test_long_table', print_info=True)
    else:
        print("\n[失败] 表创建失败")
    
    # ============================================================================
    # 测试 3: 插入数据 - skip 模式
    # ============================================================================
    
    print("\n【测试 3】插入数据 - skip 模式（忽略重复的时间-公司组合）")
    print("-" * 80)
    
    # 创建包含部分重复键的数据
    new_data = {
        'datetime': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02', 
                                    '2024-03-31', '2024-04-01']),
        'company': ['AAPL', 'GOOGL', 'AAPL', 'AAPL', 'AAPL'],
        'factor_A': [100, 200, 300, 400, 500],
        'factor_B': [10, 20, 30, 40, 50],
        'factor_C': [0.1, 0.2, 0.3, 0.4, 0.5]
    }
    df_new = pd.DataFrame(new_data)
    
    print("尝试插入的数据:")
    print(df_new)
    print("\n说明: 前3行的（时间，公司）组合已存在，应该被跳过")
    
    lm.insert_data('test_long_table', df_new, mode='skip')
    
    # 查看结果
    result = lm.load_table('test_long_table')
    print(f"\n插入后表的行数: {len(result)}")
    print("表中 AAPL 的所有记录:")
    print(result[result['company'] == 'AAPL'].sort_values('datetime'))
    
    # ============================================================================
    # 测试 4: 插入数据 - update 模式
    # ============================================================================
    
    print("\n【测试 4】插入数据 - update 模式（更新重复的时间-公司组合）")
    print("-" * 80)
    
    # 创建更新数据
    update_data = {
        'datetime': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-04-02']),
        'company': ['AAPL', 'AAPL', 'AAPL'],
        'factor_A': [999, 888, 777],  # 新值
        'factor_B': [99, 88, 77],     # 新值
        'factor_C': [0.99, 0.88, 0.77]  # 新值
    }
    df_update = pd.DataFrame(update_data)
    
    print("尝试更新/插入的数据:")
    print(df_update)
    print("\n说明: 前2行更新已有数据，第3行插入新数据")
    
    lm.insert_data('test_long_table', df_update, mode='update')
    
    # 查看结果
    result = lm.load_table('test_long_table')
    print(f"\n更新后表的行数: {len(result)}")
    print("表中 AAPL 的所有记录:")
    print(result[result['company'] == 'AAPL'].sort_values('datetime'))
    
    # ============================================================================
    # 测试 5: 添加新列
    # ============================================================================
    
    print("\n【测试 5】添加新因子列")
    print("-" * 80)
    
    # 创建新因子数据
    new_factor_data = {
        'datetime': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02', 
                                    '2024-01-02', '2024-01-03']),
        'company': ['AAPL', 'GOOGL', 'AAPL', 'GOOGL', 'AAPL'],
        'factor_D': [11.1, 22.2, 33.3, 44.4, 55.5],
        'factor_E': [111, 222, 333, 444, 555]
    }
    df_new_factor = pd.DataFrame(new_factor_data)
    
    print("新因子数据:")
    print(df_new_factor.head())
    
    lm.add_columns('test_long_table', df_new_factor, merge_on_keys=True)
    
    # 查看结果
    info = lm.get_table_info('test_long_table')
    print(f"\n添加列后的总列数: {info['column_count']}")
    print(f"所有列名: {[col['column_name'] for col in info['columns']]}")
    
    result = lm.load_table('test_long_table')
    print("\n表数据预览（前10行）:")
    print(result.head(10))
    print("GOOGL的数据")
    print(result[result['company'] == 'GOOGL'].sort_values('datetime'))
    
    # ============================================================================
    # 测试 6: 使用更大的数据集
    # ============================================================================
    
    print("\n【测试 6】使用更大的数据集（技术指标）")
    print("-" * 80)
    
    # 读取技术指标数据（部分）
    df_tech = pd.read_csv('long_data/technical_factors.csv', nrows=1000)
    df_tech['datetime'] = pd.to_datetime(df_tech['datetime'])
    
    print(f"数据集信息:")
    print(f"  行数: {len(df_tech):,}")
    print(f"  列数: {len(df_tech.columns)}")
    print(f"  时间范围: {df_tech['datetime'].min()} 到 {df_tech['datetime'].max()}")
    print(f"  公司数: {df_tech['company'].nunique()}")
    print(f"  列名: {list(df_tech.columns)}")
    
    # 创建表
    lm.create_table('tech_indicators', df_tech, overwrite=True)
    
    # 查看表信息
    lm.get_table_info('tech_indicators', print_info=True)
    
    # ============================================================================
    # 测试 7: 查询和筛选
    # ============================================================================
    
    print("\n【测试 7】数据查询示例")
    print("-" * 80)
    
    # 加载数据
    df_loaded = lm.load_table('tech_indicators')
    
    print(f"总记录数: {len(df_loaded):,}")
    
    # 按公司分组统计
    print("\n各公司的记录数:")
    company_counts = df_loaded['company'].value_counts()
    print(company_counts.head(10))
    
    # 按日期分组统计
    print("\n数据的时间分布（前10个日期）:")
    date_counts = df_loaded['datetime'].value_counts().sort_index()
    print(date_counts.head(10))
    
    # 查看特定公司的数据
    company_sample = df_loaded['company'].iloc[0]
    company_data = df_loaded[df_loaded['company'] == company_sample].sort_values('datetime')
    print(f"\n公司 {company_sample} 的数据（前5条）:")
    print(company_data.head())
    
    # ============================================================================
    # 测试 8: 列出所有表
    # ============================================================================
    
    print("\n【测试 8】列出所有长格式表")
    print("-" * 80)
    
    lm.list_tables(pattern='%long%', verbose=True, print_table=True)
    lm.list_tables(pattern='tech%', verbose=True, print_table=True)

print("\n" + "=" * 80)
print("[完成] 所有测试完成！")
print("=" * 80)

print("\n总结:")
print("  - LongManager 专门处理长格式（Panel Data）数据")
print("  - 使用（时间，公司）作为复合键")
print("  - 支持 skip、update、append 三种插入模式")
print("  - 自动管理列顺序和索引")
print("  - 继承 EasyManager 的所有功能")
print("=" * 80 + "\n")

