"""
演示 EasyManager 的 help() 功能
"""

from easy_manager import EasyManager

print("=" * 80)
print("EasyManager help() 功能演示")
print("=" * 80)

print("\n方式 1：类方法调用（不需要连接数据库）")
print("-" * 80)
EasyManager.help()

print("\n\n方式 2：实例方法调用")
print("-" * 80)
with EasyManager() as em:
    # 也可以这样调用
    # em.help()
    
    print("\n在连接数据库后，你可以：")
    print("- 使用 em.help() 查看帮助")
    print("- 使用 em.list_tables() 查看所有表")
    print("- 使用各种方法进行数据操作")

print("\n" + "=" * 80)
print("提示：在 Python 交互式环境中，可以随时调用 EasyManager.help() 查看帮助")
print("=" * 80)

