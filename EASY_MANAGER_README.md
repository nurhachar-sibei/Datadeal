# EasyManager 使用说明

## 简介

`EasyManager` 是一个简易的 PostgreSQL 数据管理工具，专为快速数据存储和管理设计。

## 主要功能

1. **创建表格** - 从 pandas DataFrame 创建数据库表
2. **插入数据（自动去重）** - 向已存在的表插入新数据，自动过滤重复行
3. **删除表格** - 删除指定的数据库表
4. **导入表格** - 从数据库导入表格到 Python

## 安装依赖

```bash
pip install pandas psycopg2 numpy
```

## 快速开始

### 1. 基本使用

```python
from easy_manager import EasyManager
import pandas as pd

# 创建管理器实例
with EasyManager(database='test_data_base') as em:
    # 读取数据
    df = pd.read_csv('data.csv', index_col=0)
    
    # 创建表
    em.create_table('my_table', df)
    
    # 导入表
    df_loaded = em.load_table('my_table')
    
    # 删除表
    em.drop_table('my_table')
```

### 2. 创建表格

```python
with EasyManager(database='test_data_base') as em:
    # 从DataFrame创建表
    df = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
    
    # 创建新表（如果已存在会报错）
    em.create_table('pb_ratio_table', df)
    
    # 覆盖已存在的表
    em.create_table('pb_ratio_table', df, overwrite=True)
```

### 3. 插入数据（自动去重）

```python
with EasyManager(database='test_data_base') as em:
    # 插入新数据，自动过滤重复行
    new_df = pd.read_csv('new_data.csv', index_col=0)
    em.insert_data('pb_ratio_table', new_df, deduplicate=True)
    
    # 不去重直接插入
    em.insert_data('pb_ratio_table', new_df, deduplicate=False)
```

### 4. 导入表格

```python
with EasyManager(database='test_data_base') as em:
    # 导入整个表
    df = em.load_table('pb_ratio_table')
    
    # 只导入前100行
    df = em.load_table('pb_ratio_table', limit=100)
```

### 5. 删除表格

```python
with EasyManager(database='test_data_base') as em:
    # 删除表
    em.drop_table('pb_ratio_table')
```

### 6. 查看表信息

```python
with EasyManager(database='test_data_base') as em:
    # 列出所有表
    tables = em.list_tables()
    print(f"所有表: {tables}")
    
    # 获取表详细信息
    info = em.get_table_info('pb_ratio_table')
    print(f"表名: {info['table_name']}")
    print(f"行数: {info['row_count']}")
    print(f"列信息: {info['columns']}")
```

## 完整示例

```python
from easy_manager import EasyManager
import pandas as pd

# 使用默认配置连接数据库
with EasyManager(database='test_data_base') as em:
    # 1. 加载数据
    df_pb = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
    df_pe = pd.read_csv('sample_data/Fundamental_PE_Ratio.csv', index_col=0)
    
    # 2. 创建表
    print("创建 PB Ratio 表...")
    em.create_table('pb_ratio', df_pb, overwrite=True)
    
    print("创建 PE Ratio 表...")
    em.create_table('pe_ratio', df_pe, overwrite=True)
    
    # 3. 查看所有表
    tables = em.list_tables()
    print(f"数据库中的表: {tables}")
    
    # 4. 获取表信息
    info = em.get_table_info('pb_ratio')
    print(f"PB Ratio 表有 {info['row_count']} 行")
    
    # 5. 插入新数据（去重）
    new_data = df_pb.tail(100)  # 取最后100行
    em.insert_data('pb_ratio', new_data, deduplicate=True)
    
    # 6. 从数据库导入
    df_loaded = em.load_table('pb_ratio')
    print(f"导入的数据形状: {df_loaded.shape}")
    
    # 7. 清理（可选）
    # em.drop_table('pb_ratio')
    # em.drop_table('pe_ratio')
```

## 初始化参数

```python
EasyManager(
    database='test_data_base',  # 数据库名
    user='postgres',            # 用户名
    password='cbw88982449',     # 密码
    host='localhost',           # 主机地址
    port='5432'                 # 端口号
)
```

## 方法说明

### `create_table(table_name, dataframe, overwrite=False)`
创建新表并导入数据
- `table_name`: 表名
- `dataframe`: pandas DataFrame
- `overwrite`: 是否覆盖已存在的表

### `insert_data(table_name, dataframe, deduplicate=True)`
向表中插入数据
- `table_name`: 表名
- `dataframe`: pandas DataFrame
- `deduplicate`: 是否自动去重

### `drop_table(table_name)`
删除表
- `table_name`: 表名

### `load_table(table_name, limit=None)`
从数据库导入表
- `table_name`: 表名
- `limit`: 限制返回行数（可选）

### `list_tables(schema='public')`
列出所有表
- `schema`: 模式名（默认为public）

### `get_table_info(table_name)`
获取表信息
- `table_name`: 表名

## 注意事项

1. **数据类型推断**：自动根据 pandas DataFrame 的数据类型推断 PostgreSQL 类型
2. **列名处理**：特殊字符（如 `.`, `-`, 空格）会被替换为下划线 `_`
3. **去重机制**：使用完整行比较进行去重
4. **索引处理**：有名称的索引会被作为列保存到数据库

## 运行测试

```bash
python test_easy_manager.py
```

这将运行完整的功能测试，包括：
- 创建表
- 插入数据
- 数据去重
- 导入表
- 删除表
- 查看表信息

## 错误处理

所有操作都有完整的错误处理和日志记录，日志会保存到 `datadeal.log` 文件中。

## 与 postgres_manager.py 的区别

- `EasyManager`：简化版，适合快速操作宽表格数据
- `PostgreSQLManager`：完整版，支持长表格格式和多因子分析

