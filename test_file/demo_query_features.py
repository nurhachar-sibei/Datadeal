"""
演示 EasyManager v2.2 的查询功能增强
展示 list_tables 和 get_table_info 的新特性
"""

from easy_manager import EasyManager
import pandas as pd

print("=" * 80)
print("EasyManager v2.2 - 查询功能增强演示")
print("=" * 80)

with EasyManager() as em:
    
    # ============================================================================
    # 第一部分：list_tables 功能演示
    # ============================================================================
    
    print("\n" + "=" * 80)
    print("【1】list_tables 功能演示")
    print("=" * 80)
    
    # 1.1 简单模式：只显示表名
    print("\n1.1 简单模式（只返回表名）:")
    print("-" * 80)
    tables = em.list_tables()
    if tables:
        print("表名列表:", [t['table_name'] for t in tables[:5]])  # 只显示前5个
    else:
        print("数据库中暂无表")
    
    # 1.2 简单模式 + 美观打印
    print("\n1.2 简单模式 + 美观打印:")
    print("-" * 80)
    em.list_tables(print_table=True)
    
    # 1.3 详细模式：显示行数、列数、大小
    print("\n1.3 详细模式（显示行数、列数、大小）:")
    print("-" * 80)
    em.list_tables(verbose=True, print_table=True)
    
    # 1.4 按名称过滤：只显示特定模式的表
    print("\n1.4 按名称过滤（示例：查找所有以 'test' 开头的表）:")
    print("-" * 80)
    test_tables = em.list_tables(pattern='test%', verbose=True, print_table=True)
    
    if not test_tables:
        print("提示：如果没有匹配的表，可以尝试其他模式，如：")
        print("  - pattern='%data%'  - 包含 'data' 的表")
        print("  - pattern='stock%'  - 以 'stock' 开头的表")
        print("  - pattern='%_table' - 以 '_table' 结尾的表")
    
    # ============================================================================
    # 第二部分：get_table_info 功能演示
    # ============================================================================
    
    print("\n" + "=" * 80)
    print("【2】get_table_info 功能演示")
    print("=" * 80)
    
    # 获取第一个表进行演示
    tables = em.list_tables()
    if tables:
        demo_table = tables[0]['table_name']
        
        # 2.1 获取信息字典
        print(f"\n2.1 获取表信息（表名: {demo_table}）:")
        print("-" * 80)
        info = em.get_table_info(demo_table)
        
        print(f"基本信息:")
        print(f"  • 表名: {info['table_name']}")
        print(f"  • 行数: {info['row_count']:,}")
        print(f"  • 列数: {info['column_count']}")
        print(f"  • 大小: {info['size']}")
        print(f"  • 主键: {info['primary_keys']}")
        
        print(f"\n列名列表:")
        column_names = [col['column_name'] for col in info['columns']]
        print(f"  {', '.join(column_names[:10])}")  # 只显示前10个列名
        if len(column_names) > 10:
            print(f"  ... (共 {len(column_names)} 列)")
        
        # 2.2 美观打印完整信息
        print(f"\n2.2 美观打印完整信息:")
        print("-" * 80)
        em.get_table_info(demo_table, print_info=True)
        
        # 2.3 实用示例：检查表是否存在数据
        print("\n2.3 实用示例：检查表是否有数据")
        print("-" * 80)
        info = em.get_table_info(demo_table)
        if info and info['row_count'] > 0:
            print(f"✓ 表 '{demo_table}' 存在且包含 {info['row_count']:,} 行数据")
        else:
            print(f"✗ 表 '{demo_table}' 为空或不存在")
    else:
        print("\n数据库中暂无表，无法演示 get_table_info 功能")
        print("提示：先创建一些表后再运行此演示")
    
    # ============================================================================
    # 第三部分：实用技巧
    # ============================================================================
    
    print("\n" + "=" * 80)
    print("【3】实用技巧")
    print("=" * 80)
    
    # 3.1 找到最大的表
    print("\n3.1 找到行数最多的表:")
    print("-" * 80)
    tables = em.list_tables(verbose=True)
    if tables:
        largest = max(tables, key=lambda t: t['row_count'])
        print(f"最大的表: {largest['table_name']}")
        print(f"  • 行数: {largest['row_count']:,}")
        print(f"  • 列数: {largest['column_count']}")
        print(f"  • 大小: {largest['size']}")
    
    # 3.2 统计总数据量
    print("\n3.2 统计数据库总数据量:")
    print("-" * 80)
    tables = em.list_tables(verbose=True)
    if tables:
        total_rows = sum(t['row_count'] for t in tables)
        total_columns = sum(t['column_count'] for t in tables)
        print(f"总表数: {len(tables)}")
        print(f"总行数: {total_rows:,}")
        print(f"总列数: {total_columns:,}")
    
    # 3.3 按模式分组表
    print("\n3.3 按前缀分组表（示例）:")
    print("-" * 80)
    tables = em.list_tables()
    if tables:
        from collections import defaultdict
        groups = defaultdict(list)
        for table in tables:
            name = table['table_name']
            # 简单按下划线或表名前几个字符分组
            prefix = name.split('_')[0] if '_' in name else name[:5]
            groups[prefix].append(name)
        
        for prefix, table_list in list(groups.items())[:5]:  # 只显示前5组
            print(f"  '{prefix}' 组: {len(table_list)} 个表")
            for t in table_list[:3]:  # 每组最多显示3个
                print(f"    - {t}")
            if len(table_list) > 3:
                print(f"    ... 还有 {len(table_list) - 3} 个表")

print("\n" + "=" * 80)
print("演示完成！")
print("=" * 80)
print("\n提示：")
print("  • 使用 verbose=True 查看详细信息")
print("  • 使用 print_table=True 或 print_info=True 美观打印")
print("  • 使用 pattern 参数过滤表名")
print("  • 查看完整文档：EasyManager完整使用手册.md")
print("=" * 80 + "\n")

