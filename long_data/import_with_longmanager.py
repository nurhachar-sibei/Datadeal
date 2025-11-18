"""
使用 LongManager 导入长格式数据示例
LongManager 专门处理面板数据（Panel Data）
"""

from easy_manager import LongManager
import pandas as pd

print("=" * 80)
print("使用 LongManager 导入长格式数据")
print("=" * 80)

with LongManager(time_col='datetime', entity_col='company') as lm:
    
    # 查看帮助
    print("\n提示：可以使用 LongManager.help() 查看完整帮助")
    
    # ============================================================================
    # 示例 1: 导入完整因子数据
    # ============================================================================
    
    print("\n【示例 1】导入完整因子数据集")
    print("-" * 80)
    
    # 读取数据
    df_full = pd.read_csv('long_data/full_factors.csv')
    df_full['datetime'] = pd.to_datetime(df_full['datetime'])
    
    print(f"数据信息:")
    print(f"  - 总记录数: {len(df_full):,}")
    print(f"  - 时间范围: {df_full['datetime'].min()} 到 {df_full['datetime'].max()}")
    print(f"  - 公司数量: {df_full['company'].nunique()}")
    print(f"  - 因子数量: {len(df_full.columns) - 2}")
    
    # 创建表
    print("\n正在创建表...")
    lm.create_table('full_factors_panel', df_full, overwrite=True)
    
    print("[完成] 表 'full_factors_panel' 创建成功")
    
    # ============================================================================
    # 示例 2: 导入基本面因子
    # ============================================================================
    
    print("\n【示例 2】导入基本面因子数据集")
    print("-" * 80)
    
    df_fund = pd.read_csv('long_data/fundamental_factors.csv')
    df_fund['datetime'] = pd.to_datetime(df_fund['datetime'])
    
    print(f"数据信息:")
    print(f"  - 总记录数: {len(df_fund):,}")
    print(f"  - 时间范围: {df_fund['datetime'].min()} 到 {df_fund['datetime'].max()}")
    print(f"  - 公司数量: {df_fund['company'].nunique()}")
    print(f"  - 因子数量: {len(df_fund.columns) - 2}")
    
    lm.create_table('fundamental_panel', df_fund, overwrite=True)
    print("[完成] 表 'fundamental_panel' 创建成功")
    
    # ============================================================================
    # 示例 3: 导入技术指标
    # ============================================================================
    
    print("\n【示例 3】导入技术指标数据集")
    print("-" * 80)
    
    df_tech = pd.read_csv('long_data/technical_factors.csv')
    df_tech['datetime'] = pd.to_datetime(df_tech['datetime'])
    
    print(f"数据信息:")
    print(f"  - 总记录数: {len(df_tech):,}")
    print(f"  - 时间范围: {df_tech['datetime'].min()} 到 {df_tech['datetime'].max()}")
    print(f"  - 公司数量: {df_tech['company'].nunique()}")
    print(f"  - 因子数量: {len(df_tech.columns) - 2}")
    
    lm.create_table('technical_panel', df_tech, overwrite=True)
    print("[完成] 表 'technical_panel' 创建成功")
    
    # ============================================================================
    # 示例 4: 合并因子（添加新列）
    # ============================================================================
    
    print("\n【示例 4】向表中添加新因子列")
    print("-" * 80)
    
    # 假设我们想把技术指标添加到基本面数据中
    print("将技术指标合并到基本面数据表...")
    
    # 注意：这需要两个数据集有相同的（时间，公司）组合
    # 这里仅演示概念，实际使用时需要确保数据匹配
    
    # 选择部分技术指标
    tech_subset = df_tech[['datetime', 'company', 'rsi', 'macd']].copy()
    
    # 添加到基本面表
    lm.add_columns('fundamental_panel', tech_subset, merge_on_keys=True)
    
    print("[完成] 技术指标已添加到基本面数据表")
    
    # ============================================================================
    # 示例 5: 增量更新（模拟每日数据更新）
    # ============================================================================
    
    print("\n【示例 5】增量更新示例")
    print("-" * 80)
    
    # 模拟新一天的数据
    new_day_data = df_tech[df_tech['datetime'] == df_tech['datetime'].max()].copy()
    
    # 修改日期为第二天
    new_day_data['datetime'] = new_day_data['datetime'] + pd.Timedelta(days=1)
    
    print(f"新数据: {len(new_day_data)} 条记录，日期: {new_day_data['datetime'].iloc[0]}")
    
    # 使用 skip 模式插入（避免重复）
    lm.insert_data('technical_panel', new_day_data, mode='skip')
    
    print("[完成] 增量数据已插入")
    
    # ============================================================================
    # 示例 6: 数据修正（使用 update 模式）
    # ============================================================================
    
    print("\n【示例 6】数据修正示例（update 模式）")
    print("-" * 80)
    
    # 假设发现某些数据有误，需要修正
    # 选择一些数据进行修正
    correction_data = df_tech.head(10).copy()
    
    # 修改值（模拟修正）
    for col in ['rsi', 'macd', 'volatility']:
        if col in correction_data.columns:
            correction_data[col] = correction_data[col] * 1.1  # 修正为原值的 1.1 倍
    
    print(f"修正数据: {len(correction_data)} 条记录")
    
    # 使用 update 模式更新
    lm.insert_data('technical_panel', correction_data, mode='update')
    
    print("[完成] 数据已修正")
    
    # ============================================================================
    # 查看导入结果
    # ============================================================================
    
    print("\n【导入结果汇总】")
    print("=" * 80)
    
    # 列出所有导入的表
    lm.list_tables(pattern='%panel%', verbose=True, print_table=True)
    
    # 查看某个表的详细信息
    print("\n完整因子表的详细信息:")
    lm.get_table_info('full_factors_panel', print_info=True)

print("\n" + "=" * 80)
print("[完成] 所有数据导入完成！")
print("=" * 80)

print("\n使用建议:")
print("  1. 使用 LongManager 而非 EasyManager 处理长格式数据")
print("  2. 确保数据有 'datetime' 和 'company' 列")
print("  3. 时间列必须是 pd.to_datetime() 转换后的格式")
print("  4. 使用 skip 模式进行增量更新")
print("  5. 使用 update 模式进行数据修正")
print("  6. 使用 add_columns 添加新因子")
print("=" * 80 + "\n")

