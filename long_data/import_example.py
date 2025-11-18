
'''
长格式数据导入 EasyManager 示例
'''

from easy_manager import EasyManager
import pandas as pd

print("=" * 80)
print("长格式数据导入示例")
print("=" * 80)

with EasyManager() as em:
    
    # 示例1: 导入基本面因子（月频数据）
    print("\n【1】导入基本面因子数据")
    print("-" * 80)
    
    df_fund = pd.read_csv('long_data/fundamental_factors.csv')
    df_fund['datetime'] = pd.to_datetime(df_fund['datetime'])
    df_fund = df_fund.set_index('datetime')
    
    em.create_table('fundamental_factors', df_fund, overwrite=True)
    print(f"✓ 已导入 {len(df_fund)} 条基本面数据")
    
    # 示例2: 导入技术指标（日频数据）
    print("\n【2】导入技术指标数据")
    print("-" * 80)
    
    df_tech = pd.read_csv('long_data/technical_factors.csv')
    df_tech['datetime'] = pd.to_datetime(df_tech['datetime'])
    df_tech = df_tech.set_index('datetime')
    
    em.create_table('technical_factors', df_tech, overwrite=True)
    print(f"✓ 已导入 {len(df_tech)} 条技术指标数据")
    
    # 示例3: 查看导入的表
    print("\n【3】查看导入的表")
    print("-" * 80)
    
    em.list_tables(pattern='%factors%', verbose=True, print_table=True)
    
    # 示例4: 查看表详细信息
    print("\n【4】查看表详细信息")
    print("-" * 80)
    
    em.get_table_info('fundamental_factors', print_info=True)

print("\n" + "=" * 80)
print("导入完成！")
print("=" * 80)
