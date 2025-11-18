# Easy Manager 使用手册

**版本：v2.3**
**更新日期：2025-11-18**
**Python 3.7+ | PostgreSQL 10+**

---

## 📑 目录

### 第一部分：EasyManager（宽表管理器）

1. [简介](#简介)
2. [快速开始](#快速开始)
3. [获取帮助](#获取帮助)
4. [核心功能](#核心功能)
5. [新功能详解](#新功能详解)
6. [完整 API 参考](#完整-api-参考)
7. [使用场景示例](#使用场景示例)

### 第二部分：LongManager（长格式数据管理器）

8. [LongManager 简介](#longmanager-简介)
9. [LongManager 快速开始](#longmanager-快速开始)
10. [LongManager 核心功能](#longmanager-核心功能)
11. [LongManager API 参考](#longmanager-api-参考)
12. [LongManager 使用示例](#longmanager-使用示例)

### 附录

13. [性能优化](#性能优化)
14. [故障排除](#故障排除)
15. [更新日志](#更新日志)
16. [最佳实践](#最佳实践)

---

## 简介

**Easy Manager** 是一套简易的 PostgreSQL 数据管理工具，包含两个管理器：

### 📊 EasyManager - 宽表管理器

适用于**时间序列宽表数据**，使用时间作为索引。

✅ **创建表格** - 从 pandas DataFrame 创建数据库表
✅ **添加新列** - 智能扩展表结构，自动识别已存在的列
✅ **插入数据** - 三种模式处理重复数据（skip/update/append）
✅ **删除表格** - 安全删除指定的数据库表
✅ **导入表格** - 从数据库导入表格到 Python
✅ **表信息查询** - 列出表、查看表结构和统计信息

**典型数据格式：** 每行是一个时间点，列是不同的股票或因子

```
datetime    | stock_A | stock_B | stock_C | ...
------------|---------|---------|---------|----
2020-01-01  | 100.5   | 200.3   | 150.2   | ...
2020-01-02  | 101.2   | 199.8   | 151.0   | ...
```

### 📈 LongManager - 长格式数据管理器

适用于**Panel Data（面板数据/长格式数据）**，使用（时间，公司）作为复合键。

✅ **专门处理长格式** - 针对 Panel Data 优化
✅ **复合键去重** - 基于（时间，公司）判断重复
✅ **自动列顺序** - 时间列第一，公司列第二
✅ **高效索引** - 自动创建复合索引提高查询性能
✅ **继承 EasyManager** - 拥有所有基础功能

**典型数据格式：** 每行是一个公司在某时间点的观测

```
datetime    | company | factor_A | factor_B | ...
------------|---------|----------|----------|----
2020-01-01  | AAPL    | 25.3     | 0.15     | ...
2020-01-01  | GOOGL   | 28.7     | 0.18     | ...
2020-01-02  | AAPL    | 25.5     | 0.16     | ...
```

### 如何选择？

- 📊 **宽表数据** → 使用 `EasyManager`
- 📈 **长格式数据 / Panel Data** → 使用 `LongManager`

### 安装依赖

```bash
pip install pandas psycopg2 numpy
```

### 数据库配置

默认配置（可自定义）：

```python
database = 'test_data_base'
user = 'postgres'
password = 'cbw88982449'
host = 'localhost'
port = '5432'
```

---

## 快速开始

### 最简单的例子

```python
from easy_manager import EasyManager
import pandas as pd

# 创建管理器（使用默认配置）
with EasyManager() as em:
    # 1. 读取数据
    df = pd.read_csv('data.csv', index_col=0)
  
    # 2. 创建表
    em.create_table('my_table', df)
  
    # 3. 导入表
    df_loaded = em.load_table('my_table')
  
    # 4. 删除表
    em.drop_table('my_table')
```

### 自定义数据库连接

```python
with EasyManager(
    database='your_database',
    user='your_user',
    password='your_password',
    host='localhost',
    port='5432'
) as em:
    # 你的操作
    pass
```

---

## 获取帮助

### 使用 help() 函数

`EasyManager` 提供了内置的 `help()` 函数，可以快速查看所有功能和使用示例。

```python
from easy_manager import EasyManager

# 方式 1：类方法调用（推荐）
EasyManager.help()

# 方式 2：实例方法调用
with EasyManager() as em:
    em.help()
```

**help() 显示内容包括：**

- ✅ 所有核心功能列表
- ✅ 三种 insert_data 模式对比
- ✅ 完整的使用示例代码
- ✅ 注意事项和最佳实践
- ✅ 相关文档链接

**示例输出：**

```
╔════════════════════════════════════════════════════════════════════════════╗
║                      EasyManager 使用帮助 (v2.1)                           ║
╚════════════════════════════════════════════════════════════════════════════╝

📚 核心功能：
  1. create_table() - 创建表
  2. add_columns() - 添加新列 ⭐
  3. insert_data() - 插入数据（三种模式）⭐
  ...
```

**什么时候使用 help()：**

- 🔹 忘记方法名称或参数时
- 🔹 需要快速查看使用示例
- 🔹 了解三种插入模式的区别
- 🔹 在 Python 交互式环境中快速查询

---

## 核心功能

### 1. 创建表格

从 pandas DataFrame 创建数据库表。

```python
with EasyManager() as em:
    df = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
  
    # 创建新表
    em.create_table('pb_ratio_table', df)
  
    # 覆盖已存在的表
    em.create_table('pb_ratio_table', df, overwrite=True)
```

**特性：**

- 自动推断列的数据类型
- 支持索引列的保存
- 自动处理特殊字符（`.`, `-`, 空格 → `_`）

### 2. 插入数据（三种模式）

#### 模式 1: skip - 忽略重复索引（默认）

```python
# 只插入索引不重复的数据
em.insert_data('table', df, mode='skip')
```

**特点：**

- 基于索引列判断重复
- 索引重复的行会被跳过
- 需要 DataFrame 有索引列

#### 模式 2: update - 覆盖重复数据

```python
# 索引重复则更新，不重复则插入
em.insert_data('table', df, mode='update')
```

**特点：**

- 基于索引列匹配
- 重复的行会被更新
- 新的行会被插入
- 需要 DataFrame 有索引列

#### 模式 3: append - 直接追加

```python
# 直接插入，不检查重复
em.insert_data('table', df, mode='append')
```

**特点：**

- 不检查重复
- 性能最快
- 不需要索引列

### 3. 删除表格

```python
with EasyManager() as em:
    # 删除表
    em.drop_table('table_name')
```

### 4. 导入表格

```python
with EasyManager() as em:
    # 导入整个表
    df = em.load_table('table_name')
  
    # 只导入前100行
    df = em.load_table('table_name', limit=100)
```

### 5. 查看表信息

#### 列出所有表

```python
with EasyManager() as em:
    # 方式1：简单模式 - 只返回表名列表
    tables = em.list_tables()
    for table in tables:
        print(table['table_name'])
  
    # 方式2：美观打印
    em.list_tables(print_table=True)
  
    # 方式3：详细模式 - 显示行数、列数、大小
    tables = em.list_tables(verbose=True, print_table=True)
  
    # 方式4：按名称过滤（支持SQL LIKE语法）
    stock_tables = em.list_tables(pattern='stock%', verbose=True, print_table=True)
    # 匹配所有以'stock'开头的表
```

**详细模式输出示例：**

```
================================================================================
📋 数据库表列表 (共 3 个表)
================================================================================
序号   表名                             行数         列数     大小      
--------------------------------------------------------------------------------
1      stock_prices                     1,500        10      128 kB
2      fundamental_data                   500        25      256 kB
3      technical_indicators             2,000        15      192 kB
================================================================================
📊 总行数: 4,000
================================================================================
```

#### 获取表详细信息

```python
with EasyManager() as em:
    # 方式1：获取信息字典
    info = em.get_table_info('my_table')
    print(f"表名: {info['table_name']}")
    print(f"行数: {info['row_count']}")
    print(f"列数: {info['column_count']}")
    print(f"大小: {info['size']}")
    print(f"主键: {info['primary_keys']}")
    print(f"列信息: {info['columns']}")
    print(f"索引: {info['indexes']}")
  
    # 方式2：美观打印所有信息
    em.get_table_info('my_table', print_info=True)
```

**美观打印输出示例：**

```
================================================================================
📊 表信息: stock_prices
================================================================================

📈 基本统计:
  • 总行数: 1,500
  • 总列数: 10
  • 表大小: 128 kB

🔑 主键:
  • datetime

📋 列详情:
序号   列名                      类型                 可空     默认值        
--------------------------------------------------------------------------------
1      datetime 🔑               timestamp            ✗        -
2      stock_A                  double precision     ✓        -
3      stock_B                  double precision     ✓        -
4      stock_C                  double precision     ✓        -
...

🗂️  索引 (共 1 个):
  1. stock_prices_pkey
     CREATE UNIQUE INDEX stock_prices_pkey ON stock_prices USING btree...
================================================================================
```

---

## 新功能详解

### add_columns - 添加新列

向已存在的表中智能添加新列。

#### 功能特性

- ✅ 自动识别新列（屏蔽已存在的列）
- ✅ 修改表结构添加新列
- ✅ 按索引自动合并数据
- ✅ 完整的错误处理

#### 方法签名

```python
def add_columns(self, table_name: str, dataframe: pd.DataFrame, 
                merge_on_index: bool = True) -> bool
```

#### 使用示例

**示例 1：基本使用**

```python
with EasyManager() as em:
    # 创建初始表（2列）
    initial_data = {
        'date': pd.date_range('2020-01-01', periods=5),
        'stock_A': [1.0, 2.0, 3.0, 4.0, 5.0],
        'stock_B': [10.0, 20.0, 30.0, 40.0, 50.0]
    }
    df_initial = pd.DataFrame(initial_data).set_index('date')
    em.create_table('stocks', df_initial)
  
    # 添加新列（2列）
    new_data = {
        'date': pd.date_range('2020-01-01', periods=5),
        'stock_C': [100.0, 200.0, 300.0, 400.0, 500.0],
        'stock_D': [1000.0, 2000.0, 3000.0, 4000.0, 5000.0]
    }
    df_new = pd.DataFrame(new_data).set_index('date')
  
    em.add_columns('stocks', df_new)
    # 结果：表现在有 4 列了
```

**示例 2：自动屏蔽已存在的列**

```python
with EasyManager() as em:
    # 新数据包含已存在的列和新列
    new_data = {
        'date': pd.date_range('2020-01-01', periods=5),
        'stock_A': [999, 999, 999, 999, 999],  # 已存在，会被忽略
        'stock_E': [5.0, 6.0, 7.0, 8.0, 9.0],  # 新列，会被添加
        'stock_F': [50.0, 60.0, 70.0, 80.0, 90.0]  # 新列，会被添加
    }
    df_new = pd.DataFrame(new_data).set_index('date')
  
    # 只有 stock_E 和 stock_F 会被添加
    em.add_columns('stocks', df_new)
```

**示例 3：逐步构建宽表**

```python
with EasyManager() as em:
    # 步骤1：创建基础价格表
    df_price = pd.read_csv('sample_data/Price_Close.csv', index_col=0)
    em.create_table('stock_data', df_price)
  
    # 步骤2：添加 PB Ratio 因子
    df_pb = pd.read_csv('sample_data/Fundamental_PB_Ratio.csv', index_col=0)
    em.add_columns('stock_data', df_pb)
  
    # 步骤3：添加 PE Ratio 因子
    df_pe = pd.read_csv('sample_data/Fundamental_PE_Ratio.csv', index_col=0)
    em.add_columns('stock_data', df_pe)
  
    # 步骤4：添加技术指标
    df_rsi = pd.read_csv('sample_data/Technical_RSI.csv', index_col=0)
    em.add_columns('stock_data', df_rsi)
  
    # 结果：一个包含所有因子的宽表
```

### insert_data - 三种模式详解

#### 模式对比表

| 模式             | 检查方式 | 更新数据 | 插入数据 | 需要索引 | 性能 | 适用场景               |
| ---------------- | -------- | -------- | -------- | -------- | ---- | ---------------------- |
| **skip**   | 基于索引 | ❌       | ✅       | ✅       | 中   | 增量更新，避免索引重复 |
| **update** | 基于索引 | ✅       | ✅       | ✅       | 慢   | 数据修正，UPSERT       |
| **append** | 不检查   | ❌       | ✅       | ❌       | 快   | 快速批量插入           |

#### skip 模式详解

**使用场景：**

- 每日增量数据导入
- 避免重复记录
- 保护现有数据

**示例：**

```python
with EasyManager() as em:
    # 表中已有 2020-01-01 到 2020-01-05 的数据
  
    # 新数据包含 01-03 到 01-07（前3个索引重复）
    new_data = {
        'date': pd.date_range('2020-01-03', periods=5),
        'value': [300, 400, 500, 6, 7]  # 即使值不同
    }
    df = pd.DataFrame(new_data).set_index('date')
  
    # 使用 skip 模式
    em.insert_data('my_table', df, mode='skip')
  
    # 结果：只有 01-06 和 01-07 被插入
    # 01-03, 01-04, 01-05 因索引重复被跳过
```

#### update 模式详解

**使用场景：**

- 修正错误数据
- 数据回填
- UPSERT 操作

**示例：**

```python
with EasyManager() as em:
    # 表中已有 2020-01-01 到 2020-01-05 的数据
  
    # 发现 01-03 和 01-04 的数据有误
    corrected_data = {
        'date': pd.to_datetime(['2020-01-03', '2020-01-04', '2020-01-08']),
        'value': [102.5, 103.5, 8.0]  # 修正后的值
    }
    df = pd.DataFrame(corrected_data).set_index('date')
  
    # 使用 update 模式
    em.insert_data('my_table', df, mode='update')
  
    # 结果：
    # - 01-03, 01-04 被更新为新值
    # - 01-08 被插入（新数据）
```

#### append 模式详解

**使用场景：**

- 初始批量导入
- 确定数据无重复
- 追求最快性能

**示例：**

```python
with EasyManager() as em:
    # 快速导入大量历史数据
    for file in data_files:
        df = pd.read_csv(file, index_col=0)
        # 使用 append 模式，最快
        em.insert_data('stocks', df, mode='append')
```

---

## 完整 API 参考

### EasyManager 类

#### 初始化

```python
EasyManager(
    database: str = "test_data_base",
    user: str = "postgres",
    password: str = "cbw88982449",
    host: str = "localhost",
    port: str = "5432"
)
```

#### 方法列表

| 方法                                                        | 说明         | 参数                                                                               | 返回值    |
| ----------------------------------------------------------- | ------------ | ---------------------------------------------------------------------------------- | --------- |
| `create_table(table_name, dataframe, overwrite=False)`    | 创建表       | table_name: 表名`<br>`dataframe: DataFrame`<br>`overwrite: 是否覆盖            | bool      |
| `add_columns(table_name, dataframe, merge_on_index=True)` | 添加新列     | table_name: 表名`<br>`dataframe: DataFrame`<br>`merge_on_index: 是否按索引合并 | bool      |
| `insert_data(table_name, dataframe, mode='skip')`         | 插入数据     | table_name: 表名`<br>`dataframe: DataFrame`<br>`mode: 'skip'/'update'/'append' | bool      |
| `drop_table(table_name)`                                  | 删除表       | table_name: 表名                                                                   | bool      |
| `load_table(table_name, limit=None)`                      | 导入表       | table_name: 表名`<br>`limit: 限制行数                                            | DataFrame |
| `list_tables(schema='public', verbose=False, pattern=None, print_table=False)` | 列出所有表   | schema: 模式名`<br>`verbose: 详细模式`<br>`pattern: 名称过滤`<br>`print_table: 美观打印 | List[Dict] |
| `get_table_info(table_name, print_info=False)`            | 获取表信息   | table_name: 表名`<br>`print_info: 美观打印                                        | Dict      |
| `help()`                                                  | 显示帮助信息 | 无                                                                                 | None      |

#### list_tables 方法详细说明

**参数：**

- `schema` (str, 默认='public'): 数据库模式名
- `verbose` (bool, 默认=False): 
  - False: 只返回表名
  - True: 返回详细信息（行数、列数、大小）
- `pattern` (str, 默认=None): 表名过滤模式，支持 SQL LIKE 语法
  - 示例: 'stock%' - 以 stock 开头的表
  - 示例: '%_data' - 以 _data 结尾的表
  - 示例: '%price%' - 包含 price 的表
- `print_table` (bool, 默认=False): 是否以美观的表格形式打印

**返回值：**

- 简单模式: `[{'table_name': 'table1'}, {'table_name': 'table2'}, ...]`
- 详细模式: `[{'table_name': 'table1', 'row_count': 100, 'column_count': 5, 'size': '64 kB'}, ...]`

**使用示例：**

```python
# 1. 快速查看所有表名
tables = em.list_tables()
print([t['table_name'] for t in tables])

# 2. 查看所有以 'stock' 开头的表的详细信息
stock_tables = em.list_tables(pattern='stock%', verbose=True, print_table=True)

# 3. 找到最大的表
tables = em.list_tables(verbose=True)
largest = max(tables, key=lambda t: t['row_count'])
print(f"最大的表: {largest['table_name']}, 行数: {largest['row_count']}")
```

#### get_table_info 方法详细说明

**参数：**

- `table_name` (str): 表名
- `print_info` (bool, 默认=False): 是否美观打印表信息

**返回值字典包含：**

- `table_name` (str): 表名
- `row_count` (int): 总行数
- `column_count` (int): 总列数
- `size` (str): 表大小（人类可读格式，如 "128 kB"）
- `columns` (list): 列信息列表，每个元素包含：
  - `column_name`: 列名
  - `data_type`: 数据类型
  - `is_nullable`: 是否可为空
  - `column_default`: 默认值
  - `character_maximum_length`: 字符最大长度（字符串类型）
- `indexes` (list): 索引信息列表
- `primary_keys` (list): 主键列名列表

**使用示例：**

```python
# 1. 检查表是否存在且有数据
info = em.get_table_info('my_table')
if info and info['row_count'] > 0:
    print(f"表存在，包含 {info['row_count']} 行数据")

# 2. 获取所有列名
info = em.get_table_info('my_table')
column_names = [col['column_name'] for col in info['columns']]
print(f"列名: {column_names}")

# 3. 检查主键
info = em.get_table_info('my_table')
print(f"主键: {info['primary_keys']}")

# 4. 美观打印所有信息
em.get_table_info('my_table', print_info=True)
```

---

## 使用场景示例

### 场景 1：股票数据管理系统

```python
from easy_manager import EasyManager
import pandas as pd

with EasyManager() as em:
    # Day 1: 创建初始价格表
    day1_data = {
        'date': pd.date_range('2020-01-01', periods=5),
        'AAPL': [100, 101, 102, 103, 104],
        'GOOGL': [1000, 1010, 1020, 1030, 1040]
    }
    df_day1 = pd.DataFrame(day1_data).set_index('date')
    em.create_table('stock_prices', df_day1)
  
    # Day 2: 添加新股票列
    day2_data = {
        'date': pd.date_range('2020-01-01', periods=5),
        'MSFT': [200, 202, 204, 206, 208],
        'TSLA': [500, 510, 520, 530, 540]
    }
    df_day2 = pd.DataFrame(day2_data).set_index('date')
    em.add_columns('stock_prices', df_day2)
  
    # Day 3: 插入新日期数据（skip模式）
    day3_data = {
        'date': pd.date_range('2020-01-06', periods=3),
        'AAPL': [105, 106, 107],
        'GOOGL': [1050, 1060, 1070],
        'MSFT': [210, 212, 214],
        'TSLA': [550, 560, 570]
    }
    df_day3 = pd.DataFrame(day3_data).set_index('date')
    em.insert_data('stock_prices', df_day3, mode='skip')
  
    # Day 4: 修正错误数据（update模式）
    day4_data = {
        'date': pd.to_datetime(['2020-01-03']),
        'AAPL': [102.5],
        'GOOGL': [1025],
        'MSFT': [205],
        'TSLA': [525]
    }
    df_day4 = pd.DataFrame(day4_data).set_index('date')
    em.insert_data('stock_prices', df_day4, mode='update')
```

### 场景 2：每日数据更新

```python
def daily_update():
    """每日数据更新流程"""
    with EasyManager() as em:
        # 获取今天的数据
        today_data = fetch_today_data()
        today_data = today_data.set_index('date')
      
        # 使用 skip 模式避免重复
        em.insert_data('daily_prices', today_data, mode='skip')
      
        print(f"成功更新 {len(today_data)} 条记录")

# 每天执行
daily_update()
```

### 场景 3：数据修正

```python
def fix_data_errors(error_dates):
    """修正特定日期的错误数据"""
    with EasyManager() as em:
        # 从数据源获取正确的数据
        corrected_data = fetch_corrected_data(error_dates)
        corrected_data = corrected_data.set_index('date')
      
        # 使用 update 模式覆盖错误数据
        em.insert_data('stock_prices', corrected_data, mode='update')
      
        print(f"成功修正 {len(error_dates)} 个日期的数据")

# 修正特定日期
fix_data_errors(['2020-01-15', '2020-01-16'])
```

### 场景 4：快速批量导入

```python
def bulk_import(data_files):
    """快速批量导入历史数据"""
    with EasyManager() as em:
        for file in data_files:
            df = pd.read_csv(file, index_col=0)
          
            # 使用 append 模式，最快
            em.insert_data('historical_data', df, mode='append')
          
            print(f"导入 {file}: {len(df)} 行")

# 导入所有历史文件
bulk_import(['2020.csv', '2021.csv', '2022.csv'])
```

---

---

# 第二部分：LongManager（长格式数据管理器）

---

## LongManager 简介

`LongManager` 是专门为 **Panel Data（面板数据/长格式数据）** 设计的数据管理器，继承自 `EasyManager`。

### 为什么需要 LongManager？

在金融研究、经济分析等领域，我们经常遇到 **长格式数据**：

```python
# 长格式数据示例
datetime    | company | factor_A | factor_B | factor_C
------------|---------|----------|----------|----------
2020-01-01  | AAPL    | 25.3     | 0.15     | 100
2020-01-01  | GOOGL   | 28.7     | 0.18     | 120
2020-01-01  | MSFT    | 22.1     | 0.12     | 95
2020-01-02  | AAPL    | 25.5     | 0.16     | 102
2020-01-02  | GOOGL   | 28.9     | 0.19     | 122
```

**特点：**
- 每行是一个 **（时间，公司）** 的观测值
- 同一时间有多个公司
- 同一公司有多个时间点
- **唯一性由（时间，公司）复合键决定**

### LongManager vs EasyManager

| 特性 | EasyManager | LongManager |
|------|-------------|-------------|
| **数据格式** | 宽表（时间为索引） | 长表（时间、公司为列） |
| **索引** | 时间列作为主键 | 自增 ID 作为主键 |
| **唯一性判断** | 基于时间索引 | 基于（时间，公司）复合键 |
| **列顺序** | 无特定要求 | 时间第一，公司第二 |
| **索引优化** | 时间列索引 | （时间，公司）复合索引 |
| **适用场景** | 时间序列宽表 | Panel Data / 长格式数据 |

### 核心设计

```python
class LongManager(EasyManager):
    """长格式数据管理器
    
    核心特性：
    1. 使用（时间，公司）作为复合键
    2. 自动创建复合索引
    3. 强制列顺序：时间第一，公司第二
    4. 自动去重（时间，公司）组合
    """
    
    def __init__(self, time_col='datetime', entity_col='company', **kwargs):
        super().__init__(**kwargs)
        self.time_col = time_col      # 时间列名（默认 'datetime'）
        self.entity_col = entity_col  # 实体列名（默认 'company'）
```

---

## LongManager 快速开始

### 1. 基本用法

```python
import pandas as pd
from easy_manager import LongManager

# 准备长格式数据
data = {
    'datetime': ['2020-01-01', '2020-01-01', '2020-01-02', '2020-01-02'],
    'company': ['AAPL', 'GOOGL', 'AAPL', 'GOOGL'],
    'PE_ratio': [25.3, 28.7, 25.5, 28.9],
    'ROE': [0.15, 0.18, 0.16, 0.19]
}
df = pd.DataFrame(data)

# 使用 LongManager
with LongManager() as lm:
    # 创建表（自动设置复合索引）
    lm.create_table('stock_factors', df)
    
    # 查看表信息
    lm.get_table_info('stock_factors', print_info=True)
    
    # 读取数据
    result = lm.load_table('stock_factors')
    print(result)
```

### 2. 自定义时间和公司列名

```python
# 如果你的数据使用不同的列名
df = pd.DataFrame({
    'date': ['2020-01-01', '2020-01-02'],
    'ticker': ['AAPL', 'GOOGL'],
    'PE': [25.3, 28.7]
})

# 指定列名
with LongManager(time_col='date', entity_col='ticker') as lm:
    lm.create_table('my_data', df)
```

### 3. 获取帮助

```python
# 查看 LongManager 的详细使用说明
LongManager.help()
```

---

## LongManager 核心功能

### 1. 创建表（create_table）

**自动处理：**
- ✅ 检查时间列和公司列是否存在
- ✅ 重置 DataFrame 索引（使用自增 ID）
- ✅ 调整列顺序（时间第一，公司第二）
- ✅ 转换时间列为 datetime 类型
- ✅ 检测并去除重复的（时间，公司）组合
- ✅ 创建（时间，公司）复合索引

```python
import pandas as pd
from easy_manager import LongManager

# 准备数据
df = pd.DataFrame({
    'datetime': pd.date_range('2020-01-01', periods=100, freq='D'),
    'company': ['AAPL', 'GOOGL', 'MSFT', 'AMZN'] * 25,
    'factor_1': np.random.randn(100),
    'factor_2': np.random.randn(100),
    'factor_3': np.random.randn(100)
})

with LongManager() as lm:
    # 创建表
    lm.create_table('stock_factors', df)
    # 输出：自动检查、去重、建立索引
```

**注意事项：**
```python
# ❌ 错误：缺少时间列或公司列
df_bad = pd.DataFrame({
    'datetime': [...],
    'value': [...]  # 缺少 company 列
})
lm.create_table('table', df_bad)  # 会报错

# ✅ 正确：包含时间列和公司列
df_good = pd.DataFrame({
    'datetime': [...],
    'company': [...],
    'value': [...]
})
lm.create_table('table', df_good)  # 成功
```

### 2. 插入数据（insert_data）

**三种模式，基于（时间，公司）复合键去重：**

#### Mode 1: skip（跳过重复）

```python
# 表中已有数据
existing = pd.DataFrame({
    'datetime': ['2020-01-01', '2020-01-01'],
    'company': ['AAPL', 'GOOGL'],
    'PE': [25.0, 28.0]
})

# 新数据（包含重复和新数据）
new_data = pd.DataFrame({
    'datetime': ['2020-01-01', '2020-01-01', '2020-01-02'],
    'company': ['AAPL', 'MSFT', 'AAPL'],  # AAPL+2020-01-01 重复
    'PE': [99.9, 30.0, 26.0]
})

with LongManager() as lm:
    lm.insert_data('stock_factors', new_data, mode='skip')
    # 结果：跳过 (2020-01-01, AAPL)
    #       插入 (2020-01-01, MSFT) 和 (2020-01-02, AAPL)
```

#### Mode 2: update（更新重复）

```python
# 修正错误数据
corrected = pd.DataFrame({
    'datetime': ['2020-01-01'],
    'company': ['AAPL'],
    'PE': [25.5]  # 更新后的正确值
})

with LongManager() as lm:
    lm.insert_data('stock_factors', corrected, mode='update')
    # 结果：更新 (2020-01-01, AAPL) 的 PE 值为 25.5
```

#### Mode 3: append（直接追加）

```python
# 确保无重复时使用（最快）
new_data = pd.DataFrame({
    'datetime': ['2020-01-03', '2020-01-03'],
    'company': ['AAPL', 'GOOGL'],
    'PE': [26.0, 29.0]
})

with LongManager() as lm:
    lm.insert_data('stock_factors', new_data, mode='append')
    # 结果：直接插入，不检查重复
```

### 3. 添加新列（add_columns）

**基于（时间，公司）复合键合并新列：**

```python
# 表中已有 PE 和 ROE
# 现在要添加 ROA 列

new_factors = pd.DataFrame({
    'datetime': ['2020-01-01', '2020-01-01', '2020-01-02'],
    'company': ['AAPL', 'GOOGL', 'AAPL'],
    'ROA': [0.12, 0.15, 0.13],        # 新列
    'Debt_Ratio': [0.3, 0.2, 0.31]   # 新列
})

with LongManager() as lm:
    lm.add_columns('stock_factors', new_factors)
    # 结果：
    # - 添加 ROA 和 Debt_Ratio 列到表结构
    # - 根据（时间，公司）匹配更新对应行的新列值
```

**智能处理：**
```python
# 自动识别哪些是新列
df_with_mixed = pd.DataFrame({
    'datetime': [...],
    'company': [...],
    'PE': [...],        # 已存在，忽略
    'ROA': [...],       # 新列，添加
    'New_Factor': [...] # 新列，添加
})

lm.add_columns('table', df_with_mixed)
# 只添加 ROA 和 New_Factor
```

### 4. 读取数据（load_table）

```python
with LongManager() as lm:
    # 读取整个表
    df = lm.load_table('stock_factors')
    
    # 查看结构
    print(df.head())
    # id | datetime   | company | PE   | ROE  | ROA
    # 1  | 2020-01-01 | AAPL    | 25.3 | 0.15 | 0.12
    # 2  | 2020-01-01 | GOOGL   | 28.7 | 0.18 | 0.15
    # ...
```

### 5. 其他功能（继承自 EasyManager）

```python
with LongManager() as lm:
    # 删除表
    lm.drop_table('old_table')
    
    # 列出所有表
    lm.list_tables(verbose=True, print_table=True)
    
    # 查看表信息
    lm.get_table_info('stock_factors', print_info=True)
```

---

## LongManager API 参考

### 初始化

```python
LongManager(time_col='datetime', entity_col='company', **kwargs)
```

**参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `time_col` | str | `'datetime'` | 时间列名 |
| `entity_col` | str | `'company'` | 实体（公司）列名 |
| `database` | str | `'test_data_base'` | 数据库名 |
| `user` | str | `'postgres'` | 用户名 |
| `password` | str | `'cbw88982449'` | 密码 |
| `host` | str | `'localhost'` | 主机地址 |
| `port` | int | `5432` | 端口号 |

**示例：**

```python
# 使用默认配置
lm = LongManager()

# 自定义时间和公司列名
lm = LongManager(time_col='trade_date', entity_col='stock_code')

# 自定义数据库连接
lm = LongManager(
    time_col='date',
    entity_col='ticker',
    database='my_database',
    password='my_password'
)
```

### 核心方法

#### 1. create_table

```python
create_table(table_name: str, df: pd.DataFrame) -> None
```

从 DataFrame 创建数据库表，针对长格式数据优化。

**处理流程：**
1. 验证时间列和公司列存在
2. 重置索引为自增 ID
3. 调整列顺序（时间第一，公司第二）
4. 转换时间列为 datetime
5. 去除重复的（时间，公司）组合
6. 创建表结构和复合索引
7. 插入数据

**示例：**

```python
df = pd.DataFrame({
    'datetime': pd.date_range('2020-01-01', periods=200),
    'company': ['AAPL', 'GOOGL'] * 100,
    'PE': np.random.randn(200),
    'ROE': np.random.randn(200)
})

with LongManager() as lm:
    lm.create_table('factors', df)
```

#### 2. insert_data

```python
insert_data(table_name: str, df: pd.DataFrame, mode: str = 'skip') -> None
```

插入数据，基于（时间，公司）复合键处理重复。

**参数：**

| 参数 | 说明 | 可选值 |
|------|------|--------|
| `table_name` | 表名 | - |
| `df` | 数据 | 必须包含时间列和公司列 |
| `mode` | 重复处理模式 | `'skip'`, `'update'`, `'append'` |

**模式详解：**

| 模式 | 行为 | 性能 | 使用场景 |
|------|------|------|----------|
| `skip` | 跳过重复的（时间，公司） | 中 | 增量更新 |
| `update` | 更新重复的（时间，公司） | 慢 | 数据修正 |
| `append` | 直接插入，不检查 | 快 | 确保无重复 |

**示例：**

```python
# 增量插入
today = pd.DataFrame({
    'datetime': ['2020-01-05'] * 3,
    'company': ['AAPL', 'GOOGL', 'MSFT'],
    'PE': [25.0, 28.0, 22.0]
})
lm.insert_data('factors', today, mode='skip')

# 修正数据
fix = pd.DataFrame({
    'datetime': ['2020-01-01'],
    'company': ['AAPL'],
    'PE': [25.5]  # 正确值
})
lm.insert_data('factors', fix, mode='update')
```

#### 3. add_columns

```python
add_columns(table_name: str, df: pd.DataFrame) -> None
```

添加新列，基于（时间，公司）复合键合并数据。

**处理流程：**
1. 识别新列（排除时间列、公司列和已存在列）
2. 向表结构添加新列
3. 根据（时间，公司）匹配更新对应行

**示例：**

```python
# 添加新因子列
new_factors = pd.DataFrame({
    'datetime': df['datetime'],
    'company': df['company'],
    'ROA': [...],           # 新列
    'Debt_Ratio': [...]     # 新列
})

lm.add_columns('factors', new_factors)
```

#### 4. load_table

```python
load_table(table_name: str) -> pd.DataFrame
```

读取表数据到 DataFrame。

**返回：** 包含所有列的 DataFrame（包括 id 列）

**示例：**

```python
df = lm.load_table('factors')
print(df.columns)
# Index(['id', 'datetime', 'company', 'PE', 'ROE', 'ROA', ...])
```

#### 5. drop_table

```python
drop_table(table_name: str) -> None
```

删除指定表。

#### 6. list_tables / get_table_info

继承自 `EasyManager`，功能相同。

### 静态方法

#### help()

```python
LongManager.help()
```

打印 `LongManager` 的详细使用说明，包括特性、数据格式、使用示例等。

---

## LongManager 使用示例

### 场景 1：因子数据库构建

```python
import pandas as pd
import numpy as np
from easy_manager import LongManager

def build_factor_database():
    """构建股票因子数据库"""
    
    # 1. 准备基础数据
    dates = pd.date_range('2020-01-01', '2023-12-31', freq='D')
    companies = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    # 生成长格式数据
    data = []
    for date in dates:
        for company in companies:
            data.append({
                'datetime': date,
                'company': company,
                'PE_ratio': np.random.uniform(15, 35),
                'PB_ratio': np.random.uniform(2, 8),
                'ROE': np.random.uniform(0.05, 0.25)
            })
    
    df_basic = pd.DataFrame(data)
    
    with LongManager() as lm:
        # 创建基础因子表
        print("创建基础因子表...")
        lm.create_table('stock_basic_factors', df_basic)
        
        # 后续添加技术因子
        print("添加技术因子...")
        df_basic['MA_20'] = np.random.uniform(80, 120, len(df_basic))
        df_basic['RSI'] = np.random.uniform(30, 70, len(df_basic))
        lm.add_columns('stock_basic_factors', df_basic[['datetime', 'company', 'MA_20', 'RSI']])
        
        # 查看最终表结构
        lm.get_table_info('stock_basic_factors', print_info=True)
        
        print("因子数据库构建完成！")

build_factor_database()
```

### 场景 2：多来源数据合并

```python
def merge_multiple_sources():
    """合并多个数据源的因子"""
    
    # 数据源1：基本面因子
    df_fundamental = pd.read_csv('fundamental_factors.csv')
    # datetime | company | PE | PB | ROE
    
    # 数据源2：技术面因子
    df_technical = pd.read_csv('technical_factors.csv')
    # datetime | company | MA_20 | RSI | MACD
    
    # 数据源3：财务因子
    df_financial = pd.read_csv('financial_factors.csv')
    # datetime | company | Revenue | NetIncome | Debt
    
    with LongManager() as lm:
        # 1. 创建主表（基本面）
        lm.create_table('all_factors', df_fundamental)
        
        # 2. 添加技术面因子
        lm.add_columns('all_factors', df_technical)
        
        # 3. 添加财务因子
        lm.add_columns('all_factors', df_financial)
        
        # 4. 验证
        result = lm.load_table('all_factors')
        print(f"最终表包含 {len(result.columns)} 列")
        print(result.head())

merge_multiple_sources()
```

### 场景 3：增量更新工作流

```python
def daily_factor_update():
    """每日因子更新"""
    
    def fetch_today_factors():
        """模拟获取今天的因子数据"""
        today = pd.Timestamp.now().date()
        companies = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        
        return pd.DataFrame({
            'datetime': [today] * len(companies),
            'company': companies,
            'PE_ratio': np.random.uniform(15, 35, len(companies)),
            'PB_ratio': np.random.uniform(2, 8, len(companies)),
            'ROE': np.random.uniform(0.05, 0.25, len(companies))
        })
    
    with LongManager() as lm:
        # 获取今日数据
        today_data = fetch_today_factors()
        
        # 使用 skip 模式避免重复
        lm.insert_data('stock_factors', today_data, mode='skip')
        
        print(f"✓ 成功更新 {len(today_data)} 条因子数据")

# 定时执行
daily_factor_update()
```

### 场景 4：数据质量检查与修正

```python
def quality_check_and_fix():
    """数据质量检查与修正"""
    
    with LongManager() as lm:
        # 1. 读取数据
        df = lm.load_table('stock_factors')
        
        # 2. 检查异常值
        print("检查异常值...")
        errors = df[(df['PE_ratio'] < 0) | (df['PE_ratio'] > 100)]
        
        if len(errors) > 0:
            print(f"发现 {len(errors)} 条异常数据")
            
            # 3. 重新获取正确数据
            correct_data = fetch_correct_data(
                errors['datetime'].tolist(),
                errors['company'].tolist()
            )
            
            # 4. 使用 update 模式修正
            lm.insert_data('stock_factors', correct_data, mode='update')
            
            print(f"✓ 已修正 {len(errors)} 条数据")
        else:
            print("✓ 数据质量良好")

quality_check_and_fix()
```

### 场景 5：自定义列名

```python
def custom_column_names():
    """使用自定义的时间和公司列名"""
    
    # 数据使用 'trade_date' 和 'stock_code' 而非默认名称
    df = pd.DataFrame({
        'trade_date': pd.date_range('2020-01-01', periods=100),
        'stock_code': ['000001', '000002'] * 50,
        'close_price': np.random.uniform(10, 50, 100),
        'volume': np.random.randint(1000, 10000, 100)
    })
    
    # 指定自定义列名
    with LongManager(time_col='trade_date', entity_col='stock_code') as lm:
        lm.create_table('cn_stocks', df)
        
        # 后续操作会自动使用 trade_date 和 stock_code
        new_data = pd.DataFrame({
            'trade_date': ['2020-04-11'],
            'stock_code': ['000001'],
            'close_price': [45.0]
        })
        
        lm.insert_data('cn_stocks', new_data, mode='update')

custom_column_names()
```

### 场景 6：完整的 ETL 流程

```python
class FactorETL:
    """完整的因子数据 ETL 流程"""
    
    def __init__(self, table_name='factor_database'):
        self.table_name = table_name
        self.lm = LongManager()
    
    def extract(self, start_date, end_date):
        """提取：从各数据源获取原始数据"""
        print(f"提取数据：{start_date} 至 {end_date}")
        
        # 模拟从多个数据源提取
        df1 = self._fetch_from_source1(start_date, end_date)
        df2 = self._fetch_from_source2(start_date, end_date)
        df3 = self._fetch_from_source3(start_date, end_date)
        
        return df1, df2, df3
    
    def transform(self, *dataframes):
        """转换：数据清洗和特征工程"""
        print("转换数据...")
        
        transformed = []
        for df in dataframes:
            # 清洗
            df = df.dropna()
            df = df[df['PE_ratio'] > 0]
            
            # 特征工程
            df['log_PE'] = np.log(df['PE_ratio'])
            
            transformed.append(df)
        
        return transformed
    
    def load(self, *dataframes):
        """加载：写入数据库"""
        print("加载到数据库...")
        
        with self.lm as lm:
            # 首个数据框创建表
            lm.create_table(self.table_name, dataframes[0])
            
            # 其余数据框添加列
            for df in dataframes[1:]:
                lm.add_columns(self.table_name, df)
        
        print("✓ ETL 完成")
    
    def run(self, start_date, end_date):
        """运行完整 ETL"""
        # Extract
        dfs = self.extract(start_date, end_date)
        
        # Transform
        dfs = self.transform(*dfs)
        
        # Load
        self.load(*dfs)

# 使用
etl = FactorETL()
etl.run('2020-01-01', '2023-12-31')
```

---

## 性能优化

### add_columns 性能

**适合：**

- ✅ 少量列（< 20个）
- ✅ 中等数据量（< 100万行）

**优化建议：**

```python
# 不好：逐个添加100个列
for col in columns:
    em.add_columns('table', df[[col]])

# 好：一次添加所有列
em.add_columns('table', df[columns])
```

### insert_data 性能排序

```
append (最快) > skip > update (最慢)
```

**性能对比（10万行）：**

- append: ~1秒
- skip: ~3秒
- update: ~5秒

**优化建议：**

1. 明确无重复时使用 `append`
2. 大量更新考虑：删除 → 重建
3. 为索引列建立数据库索引

### 批量操作技巧

```python
# 批量添加多个因子列
with EasyManager() as em:
    factors = ['PB', 'PE', 'ROE', 'Revenue_Growth']
  
    for factor in factors:
        df = pd.read_csv(f'sample_data/Fundamental_{factor}.csv', index_col=0)
        em.add_columns('stock_data', df)
```

---

## 故障排除

### 常见错误及解决方案

#### 1. 连接错误

**错误信息：** "数据库连接失败"

**解决方案：**

```python
# 检查 PostgreSQL 服务是否运行
# 检查数据库名、用户名、密码是否正确

# 测试连接
with EasyManager(database='test_data_base') as em:
    tables = em.list_tables()
    print("连接成功！")
```

#### 2. add_columns 没有效果

**原因：** 列已存在

**解决方案：**

```python
# add_columns 会自动跳过已存在的列
# 检查日志查看哪些列被跳过
```

#### 3. skip/update 模式报错

**错误信息：** "需要索引列"

**解决方案：**

```python
# 确保 DataFrame 有索引
df = df.set_index('date')
em.insert_data('table', df, mode='skip')
```

#### 4. 数据没有被更新

**原因：** 索引值不匹配

**解决方案：**

```python
# 检查索引列的值是否与表中一致
existing_df = em.load_table('table')
print("表中的索引:", existing_df['date'].unique())
print("新数据的索引:", df.index.unique())
```

#### 5. 列名包含特殊字符

**说明：** 特殊字符会被自动转换

```python
# 自动转换规则：
# '000001.SZ' → '000001_SZ'
# 'A-B' → 'A_B'
# 'A B' → 'A_B'
```

---

## 更新日志

### v2.2 (2025-11-18) - 当前版本

#### 🚀 查询功能大幅增强

**1. list_tables 方法升级：**

- ✨ 新增 `verbose` 参数：显示表的行数、列数、大小等详细信息
- ✨ 新增 `pattern` 参数：支持按表名模式过滤（SQL LIKE 语法）
- ✨ 新增 `print_table` 参数：以美观的表格形式打印
- 📊 详细模式下自动统计总行数

**2. get_table_info 方法升级：**

- ✨ 新增表大小信息（人类可读格式）
- ✨ 新增主键信息列表
- ✨ 新增索引详细信息
- ✨ 增强的列信息：包含默认值、可空性、字符长度等
- ✨ 新增 `print_info` 参数：美观格式打印所有信息
- 🎨 主键列在打印时带有 🔑 标记

**使用示例：**

```python
# 查看所有以 'stock' 开头的表（详细模式，美观打印）
em.list_tables(pattern='stock%', verbose=True, print_table=True)

# 美观打印表的完整信息（包括主键、索引、列详情等）
em.get_table_info('my_table', print_info=True)
```

### v2.1 (2025)

#### 🔄 重要变更

**insert_data skip 模式改进：**

- 从基于全行比较改为基于索引列判断
- 与 update 模式逻辑统一
- 现在 skip 模式也需要 DataFrame 有索引列

**影响：**

```python
# 表中已有: date=2020-01-01, value=100
# 尝试插入: date=2020-01-01, value=200

# v2.0 行为：会插入（因为 value 不同）
# v2.1 行为：会跳过（因为 date 索引重复）✅
```

### v2.3 (2025-11-18)

#### ✨ 重大新功能

**LongManager - 长格式数据管理器：**

专门为 Panel Data（面板数据/长格式数据）设计的新管理器类。

**核心特性：**

1. **复合键唯一性** - 使用（时间，公司）复合键判断重复
2. **自动列顺序** - 强制时间列第一，公司列第二
3. **复合索引** - 自动创建（时间，公司）复合索引
4. **智能去重** - 自动检测并去除重复的（时间，公司）组合
5. **继承功能** - 继承 EasyManager 所有基础功能

**新增方法（重写）：**

- `create_table()` - 针对长格式优化，自动设置复合索引
- `insert_data()` - 基于（时间，公司）复合键处理重复
- `add_columns()` - 基于（时间，公司）复合键合并新列
- `help()` - 静态方法，打印 LongManager 使用说明

**使用示例：**

```python
from easy_manager import LongManager

# 长格式数据
df = pd.DataFrame({
    'datetime': ['2020-01-01', '2020-01-01', '2020-01-02'],
    'company': ['AAPL', 'GOOGL', 'AAPL'],
    'PE': [25.3, 28.7, 25.5],
    'ROE': [0.15, 0.18, 0.16]
})

with LongManager() as lm:
    # 创建表（自动创建复合索引）
    lm.create_table('stock_factors', df)
    
    # 插入数据（基于复合键去重）
    lm.insert_data('stock_factors', new_data, mode='skip')
    
    # 添加新列（基于复合键匹配）
    lm.add_columns('stock_factors', new_factors)
```

**适用场景：**

- 股票因子数据库
- 面板数据分析
- 多实体时间序列数据
- 需要（时间 × 实体）唯一性的数据

### v2.0 (2025)

#### ✨ 新功能

1. **add_columns** - 添加新列
2. **insert_data 升级** - 三种模式（skip/update/append）

#### 🔧 改进

- 完整的错误处理和日志记录
- 详细的使用文档和示例

### v1.0 (2025)

#### ✨ 初始发布

- 基础的增删改查功能
- 自动类型推断
- 批量插入优化

---

## 最佳实践

### ✅ 推荐做法

#### 0. 选择正确的管理器

```python
# 宽表数据（时间为索引） → 使用 EasyManager
df_wide = pd.DataFrame({
    'stock_A': [100, 101, 102],
    'stock_B': [200, 199, 201]
}, index=pd.date_range('2020-01-01', periods=3))

with EasyManager() as em:
    em.create_table('wide_data', df_wide)

# 长格式数据（时间+公司为列） → 使用 LongManager
df_long = pd.DataFrame({
    'datetime': ['2020-01-01', '2020-01-01', '2020-01-02'],
    'company': ['AAPL', 'GOOGL', 'AAPL'],
    'price': [100, 200, 101]
})

with LongManager() as lm:
    lm.create_table('long_data', df_long)
```

#### 1. 使用 with 语句管理连接

```python
# 好：自动管理连接
with EasyManager() as em:
    em.create_table('table', df)

# 不好：手动管理连接
em = EasyManager()
em.create_table('table', df)
em.close()  # 容易忘记
```

#### 2. 为时间序列数据设置日期索引

```python
# 好：使用日期作为索引
df = df.set_index('date')
em.create_table('stocks', df)

# 不好：不设置索引
em.create_table('stocks', df)  # 会创建额外的 index 列
```

#### 3. 选择合适的 insert_data 模式

```python
# 每日增量：skip
em.insert_data('table', today_data, mode='skip')

# 数据修正：update
em.insert_data('table', corrected_data, mode='update')

# 初始导入：append
em.insert_data('table', historical_data, mode='append')
```

#### 4. 批量添加列

```python
# 好：一次添加所有新列
em.add_columns('table', df_all_new_cols)

# 不好：逐个添加
for col in new_cols:
    em.add_columns('table', df[[col]])
```

### ❌ 避免的做法

#### 1. 频繁创建连接

```python
# 不好：每次操作都创建新连接
for df in dataframes:
    with EasyManager() as em:
        em.insert_data('table', df)

# 好：复用连接
with EasyManager() as em:
    for df in dataframes:
        em.insert_data('table', df)
```

#### 2. 滥用 append 模式

```python
# 不好：可能产生重复数据
em.insert_data('table', df, mode='append')

# 好：使用 skip 模式避免重复
em.insert_data('table', df, mode='skip')
```

#### 3. 不检查表是否存在

```python
# 不好：直接操作可能不存在的表
em.insert_data('table', df)

# 好：先检查表是否存在
if 'table' in em.list_tables():
    em.insert_data('table', df)
else:
    em.create_table('table', df)
```

---

## 数据类型映射

| Pandas 类型               | PostgreSQL 类型  |
| ------------------------- | ---------------- |
| int64                     | BIGINT           |
| float64                   | DOUBLE PRECISION |
| bool                      | BOOLEAN          |
| datetime64                | TIMESTAMP        |
| object/string (≤255字符) | VARCHAR          |
| object/string (>255字符)  | TEXT             |

---

## 注意事项

### EasyManager 注意事项

1. **索引要求**

   - skip 和 update 模式需要 DataFrame 有索引列
   - append 模式不需要索引列

2. **列名处理**

   - 特殊字符（`.`, `-`, 空格）会自动转换为 `_`
   - 建议使用简单的英文列名

3. **性能考虑**

   - 大数据量（>100万行）建议分批处理
   - 频繁更新建议使用数据库索引

4. **日志记录**

   - 所有操作记录在 `datadeal.log`
   - 可用于调试和问题排查

### LongManager 注意事项

1. **必需列**

   - 数据必须包含时间列（默认 `datetime`）和实体列（默认 `company`）
   - 可通过初始化参数自定义列名
   
   ```python
   # 使用默认列名
   lm = LongManager()  # 需要 'datetime' 和 'company' 列
   
   # 自定义列名
   lm = LongManager(time_col='date', entity_col='ticker')
   ```

2. **数据格式**

   - 时间列会自动转换为 `datetime` 类型
   - 索引会被重置为自增 ID
   - 列顺序会自动调整（时间第一，实体第二）

3. **复合键去重**

   - 所有插入和更新操作基于（时间，实体）复合键
   - 创建表时会自动去除重复的（时间，实体）组合
   - 复合键自动创建索引提升查询性能

4. **适用场景**

   ✅ **适合：**
   - Panel Data / 面板数据
   - 多实体时间序列（股票因子、经济指标等）
   - 需要（时间 × 实体）唯一性的数据
   
   ❌ **不适合：**
   - 宽表格式的时间序列（使用 EasyManager）
   - 单一实体的时间序列（使用 EasyManager）

5. **性能优化**

   - 复合索引显著提升查询性能
   - `mode='append'` 适合确保无重复时使用（最快）
   - 大量数据建议先本地去重再插入

---

## 测试

### 运行测试脚本

```bash
# EasyManager 测试
python test_file/demo_usage.py

# LongManager 测试
python test_long_manager.py

# 查询功能测试
python test_file/demo_query_features.py
```

### 测试前准备

1. 确保 PostgreSQL 服务运行
2. 确保数据库 `test_data_base` 存在
3. 准备测试数据

创建测试数据库：

```sql
-- PostgreSQL
CREATE DATABASE test_data_base;
```

或使用命令行：

```bash
psql -U postgres
CREATE DATABASE test_data_base;
\q
```

---

## 获取帮助

- **测试示例**：`test_new_features.py`
- **问题反馈**：查看日志文件 `datadeal.log`

---

## 快速参考卡片

```python
# 导入
from easy_manager import EasyManager
import pandas as pd

# 查看帮助
EasyManager.help()  # 显示所有功能和使用示例

# 连接
with EasyManager() as em:
  
    # 创建表
    em.create_table('table', df, overwrite=True)
  
    # 添加新列
    em.add_columns('table', df_new_cols)
  
    # 插入数据
    em.insert_data('table', df, mode='skip')    # 忽略重复索引
    em.insert_data('table', df, mode='update')  # 覆盖重复
    em.insert_data('table', df, mode='append')  # 直接追加
  
    # 导入表
    df = em.load_table('table', limit=100)
  
    # 删除表
    em.drop_table('table')
  
    # 查询
    tables = em.list_tables()
    info = em.get_table_info('table')
```

---

**EasyManager v2.1** - 让 PostgreSQL 数据管理更简单！

如有问题或建议，欢迎反馈！
