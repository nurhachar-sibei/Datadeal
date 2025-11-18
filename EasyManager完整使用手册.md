# EasyManager 完整使用手册

**版本：v2.1**
**更新日期：2025-11-17**
**Python 3.7+ | PostgreSQL 10+**

---

## 📑 目录

1. [简介](#简介)
2. [快速开始](#快速开始)
3. [获取帮助](#获取帮助)
4. [核心功能](#核心功能)
5. [新功能详解](#新功能详解)
6. [完整 API 参考](#完整-api-参考)
7. [使用场景示例](#使用场景示例)
8. [性能优化](#性能优化)
9. [故障排除](#故障排除)
10. [更新日志](#更新日志)
11. [最佳实践](#最佳实践)

---

## 简介

`EasyManager` 是一个简易的 PostgreSQL 数据管理工具，专为快速数据存储和管理设计。

### 主要特性

✅ **创建表格** - 从 pandas DataFrame 创建数据库表
✅ **添加新列** - 智能扩展表结构，自动识别已存在的列
✅ **插入数据** - 三种模式处理重复数据（skip/update/append）
✅ **删除表格** - 安全删除指定的数据库表
✅ **导入表格** - 从数据库导入表格到 Python
✅ **表信息查询** - 列出表、查看表结构和统计信息

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

```python
with EasyManager() as em:
    # 列出所有表
    tables = em.list_tables()
    print(f"所有表: {tables}")
  
    # 获取表详细信息
    info = em.get_table_info('table_name')
    print(f"表名: {info['table_name']}")
    print(f"行数: {info['row_count']}")
    print(f"列信息: {info['columns']}")
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
| `list_tables(schema='public')`                            | 列出所有表   | schema: 模式名                                                                     | List[str] |
| `get_table_info(table_name)`                              | 获取表信息   | table_name: 表名                                                                   | Dict      |
| `help()`                                                  | 显示帮助信息 | 无                                                                                 | None      |

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

### v2.1 (2025) - 当前版本

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

---

## 测试

### 运行测试脚本

```bash
# 快速测试（推荐）
python quick_test.py

# 样本数据测试
python sample_data_test.py

# 新功能测试
python test_new_features.py
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
