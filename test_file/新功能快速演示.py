"""
新功能快速演示 - 5分钟了解新功能
运行此脚本体验 EasyManager 的新功能
"""

import pandas as pd
import numpy as np
from easy_manager import EasyManager

print("=" * 80)
print("EasyManager 新功能快速演示")
print("=" * 80)

# 创建管理器
with EasyManager() as em:
    
    # ========== 演示 1: 添加新列 ==========
    print("\n" + "▶" * 40)
    print("演示 1: add_columns - 添加新列")
    print("▶" * 40)
    
    # 创建初始表（2列）
    print("\n1️⃣ 创建初始表（2列）")
    data_initial = {
        'date': pd.date_range('2020-01-01', periods=5),
        'stock_A': [100, 101, 102, 103, 104],
        'stock_B': [200, 201, 202, 203, 204]
    }
    df_initial = pd.DataFrame(data_initial).set_index('date')
    print(df_initial)
    
    em.create_table('demo_table', df_initial, overwrite=True)
    print("\n✓ 表已创建，包含 2 列")
    
    # 添加新列
    print("\n2️⃣ 添加新列 stock_C 和 stock_D")
    data_new = {
        'date': pd.date_range('2020-01-01', periods=5),
        'stock_C': [300, 301, 302, 303, 304],
        'stock_D': [400, 401, 402, 403, 404]
    }
    df_new = pd.DataFrame(data_new).set_index('date')
    
    em.add_columns('demo_table', df_new)
    print("\n✓ 新列已添加")
    
    # 查看结果
    print("\n3️⃣ 查看结果（现在有 4 列）")
    result = em.load_table('demo_table')
    print(result)
    
    # ========== 演示 2: insert_data 的三种模式 ==========
    print("\n" + "▶" * 40)
    print("演示 2: insert_data - 三种模式")
    print("▶" * 40)
    
    # 准备新表
    print("\n准备演示表...")
    demo_data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'score': [85, 90, 95]
    }
    df_demo = pd.DataFrame(demo_data).set_index('id')
    em.create_table('mode_demo', df_demo, overwrite=True)
    print("初始数据:")
    print(df_demo)
    
    # 模式 1: skip - 忽略重复索引
    print("\n" + "-" * 40)
    print("模式 1: skip - 忽略重复索引")
    print("-" * 40)
    
    skip_data = {
        'id': [2, 3, 4],  # id=2,3 的索引重复
        'name': ['Bob_new', 'Charlie_new', 'David'],  # 注意：即使值不同
        'score': [92, 97, 88]  # 也会因为索引重复被跳过
    }
    df_skip = pd.DataFrame(skip_data).set_index('id')
    print("\n尝试插入（包含重复索引 id=2,3）:")
    print(df_skip)
    print("\n说明：即使 name 和 score 的值不同，")
    print("      也会因为索引 id=2,3 重复而被跳过")
    
    em.insert_data('mode_demo', df_skip, mode='skip')
    
    result = em.load_table('mode_demo')
    print("\n结果（只有 id=4 被插入，id=2,3 因索引重复被忽略）:")
    print(result.sort_index())
    
    # 模式 2: update - 覆盖重复
    print("\n" + "-" * 40)
    print("模式 2: update - 覆盖重复数据")
    print("-" * 40)
    
    update_data = {
        'id': [1, 2, 5],  # 1,2 更新, 5 插入
        'name': ['Alice_updated', 'Bob_updated', 'Eve'],
        'score': [100, 100, 92]
    }
    df_update = pd.DataFrame(update_data).set_index('id')
    print("\n尝试插入/更新:")
    print(df_update)
    
    em.insert_data('mode_demo', df_update, mode='update')
    
    result = em.load_table('mode_demo')
    print("\n结果（id=1,2 被更新，id=5 被插入）:")
    print(result.sort_index())
    
    # 模式 3: append - 直接追加
    print("\n" + "-" * 40)
    print("模式 3: append - 直接追加（不检查重复）")
    print("-" * 40)
    
    append_data = {
        'id': [6, 7],
        'name': ['Frank', 'Grace'],
        'score': [87, 93]
    }
    df_append = pd.DataFrame(append_data).set_index('id')
    print("\n追加数据:")
    print(df_append)
    
    em.insert_data('mode_demo', df_append, mode='append')
    
    result = em.load_table('mode_demo')
    print("\n最终结果:")
    print(result.sort_index())
    
    # ========== 总结 ==========
    print("\n" + "=" * 80)
    print("新功能总结")
    print("=" * 80)
    
    print("\n✅ add_columns - 添加新列")
    print("   用法: em.add_columns('table', df_with_new_columns)")
    print("   场景: 扩展表结构，添加新的数据维度")
    
    print("\n✅ insert_data (mode='skip') - 忽略重复索引")
    print("   用法: em.insert_data('table', df, mode='skip')")
    print("   场景: 增量更新，避免索引重复（基于索引判断）")
    print("   注意: 需要 DataFrame 有索引列")
    
    print("\n✅ insert_data (mode='update') - 覆盖重复")
    print("   用法: em.insert_data('table', df, mode='update')")
    print("   场景: 数据修正，更新已有记录")
    
    print("\n✅ insert_data (mode='append') - 直接追加")
    print("   用法: em.insert_data('table', df, mode='append')")
    print("   场景: 快速批量导入，明确无重复")
    
    # 清理演示表
    print("\n" + "-" * 80)
    print("清理演示数据...")
    em.drop_table('demo_table')
    em.drop_table('mode_demo')
    print("✓ 演示完成！")

print("\n" + "=" * 80)
print("下一步:")
print("  1. 查看 '新功能说明.md' 了解详细用法")
print("  2. 运行 'test_new_features.py' 查看更多测试")
print("  3. 在你的数据上尝试这些新功能")
print("=" * 80)

