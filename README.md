# PostgreSQL数据管理系统

一个专为金融时间序列数据设计的高性能PostgreSQL数据管理系统，提供完整的数据导入、查询、分析和管理功能。

## 🚀 项目概述

本系统专门针对金融数据分析场景设计，支持股票价格、基本面指标、技术指标等多种类型的时间序列数据管理。系统采用PostgreSQL作为后端数据库，提供高效的数据存储和查询能力。

## ✨ 核心特性

### 数据管理功能

- **智能表格创建**: 自动从CSV文件或DataFrame创建优化的数据库表
- **多源数据支持**: 支持CSV文件、pandas DataFrame、字典等多种数据源
- **批量数据导入**: 支持单文件和多文件批量导入
- **增量数据更新**: 智能处理重复数据，支持数据合并和更新
- **灵活数据查询**: 支持条件查询、时间切片、列筛选等多种查询方式
- **多因子查询**: 支持跨表多因子数据联合查询和分析
- **数据导出**: 支持多种格式的数据导出功能

### 高级功能

- **时间序列优化**: 专为时间序列数据设计的索引和查询优化
- **数据质量验证**: 自动检查数据完整性和一致性
- **性能监控**: 详细的操作日志和性能统计
- **错误恢复**: 完善的事务管理和错误处理机制
- **批量操作**: 高效的批量数据处理能力

## 📊 支持的数据格式

### 输入数据格式

系统支持以下类型的数据源：

#### 1. CSV文件数据源

支持标准的CSV时间序列数据文件：

```csv
datetime,000001.SZ,000002.SZ,000858.SZ,002415.SZ,002594.SZ,600000.SH,600036.SH,600519.SH
2020-01-01,0.907,2.534,0.578,2.116,4.195,1.158,3.295,3.179
2020-01-02,1.116,4.664,4.135,3.609,3.188,0.779,3.237,1.396
```

#### 2. pandas DataFrame数据源

直接支持pandas DataFrame对象：

```python
import pandas as pd
import numpy as np

# 创建DataFrame
dates = pd.date_range('2020-01-01', periods=100, freq='D')
df = pd.DataFrame({
    '000001.SZ': np.random.randn(100),
    '000002.SZ': np.random.randn(100),
    '600000.SH': np.random.randn(100)
}, index=dates)

# 直接使用DataFrame创建表
db.create_table("my_table", df)
```

#### 3. 字典数据源

支持字典格式的数据：

```python
# 字典数据源
data_dict = {
    'datetime': ['2020-01-01', '2020-01-02', '2020-01-03'],
    '000001.SZ': [1.23, 1.45, 1.67],
    '000002.SZ': [2.34, 2.56, 2.78],
    '600000.SH': [3.45, 3.67, 3.89]
}

# 使用字典创建表
db.create_table("dict_table", data_dict)
```

### 数据类型支持

#### 基本面数据 (Fundamental Data)

- **PB比率**: 市净率数据
- **PE比率**: 市盈率数据
- **ROE**: 净资产收益率数据
- **营收增长率**: 营收增长率数据

#### 价格数据 (Price Data)

- **开盘价**: 股票开盘价格
- **收盘价**: 股票收盘价格
- **最高价**: 股票最高价格
- **最低价**: 股票最低价格
- **成交量**: 股票成交量数据

#### 技术指标数据 (Technical Data)

- **RSI**: 相对强弱指标
- **MACD**: 指数平滑移动平均线
- **布林带宽度**: 布林带技术指标
- **移动平均比率**: 移动平均线比率

### 数据库表结构

系统自动创建的表结构如下：

```sql
CREATE TABLE table_name (
    datetime TIMESTAMP PRIMARY KEY,
    "000001.SZ" DOUBLE PRECISION,
    "000002.SZ" DOUBLE PRECISION,
    "000858.SZ" DOUBLE PRECISION,
    -- ... 其他股票代码列
);

-- 自动创建时间索引
CREATE INDEX idx_table_name_datetime ON table_name (datetime);
```

### 输出数据格式

#### 查询结果DataFrame

```python
# 标准查询输出
                000001.SZ  000002.SZ  000858.SZ  002415.SZ
datetime                                          
2020-01-01      0.907444   2.533771   0.578473   2.115682
2020-01-02      1.116457   4.664031   4.134741   3.608640
2020-01-03      1.064950   0.674015   2.350563   4.018990
```

#### 统计信息输出

```python
{
    'row_count': 1043,
    'column_count': 15,
    'date_range': {
        'start': '2020-01-01',
        'end': '2022-12-30'
    },
    'missing_values': 0,
    'data_types': {
        'datetime': 'datetime64[ns]',
        '000001.SZ': 'float64',
        '000002.SZ': 'float64'
    }
}
```

## 🏗️ 项目结构

```
Datadeal/
├── postgres_manager.py      # 核心数据库管理类
├── advanced_manager.py      # 高级功能扩展类
├── README.md              # 项目文档
├── easy_test.ipynb         # 简单的测试文件（复杂的在测试文件和案例文件-虽然可能没啥用）
├── test_file/             # 各种测试文件目录
├── sample_data/           # 样本数据目录 (13个CSV文件)
│   ├── Fundamental_PB_Ratio.csv      # 市净率数据
│   ├── Fundamental_PE_Ratio.csv      # 市盈率数据
│   ├── Fundamental_ROE.csv           # 净资产收益率数据
│   ├── Fundamental_Revenue_Growth.csv # 营收增长率数据
│   ├── Price_Close.csv               # 收盘价数据
│   ├── Price_High.csv                # 最高价数据
│   ├── Price_Low.csv                 # 最低价数据
│   ├── Price_Open.csv                # 开盘价数据
│   ├── Price_Volume.csv              # 成交量数据
│   ├── Technical_BB_Width.csv        # 布林带宽度
│   ├── Technical_MACD.csv            # MACD指标
│   ├── Technical_MA_Ratio.csv        # 移动平均比率
│   └── Technical_RSI.csv             # RSI指标
├── examples/             # 各种案例文件目录
└── datadeal.log           # 系统日志文件
```

## 🚀 快速开始

### 环境要求

```bash
# Python依赖
pip install pandas numpy psycopg2-binary python-dotenv

# PostgreSQL数据库 (版本 >= 12.0)
```

### 数据库配置

创建 `.env` 文件或设置环境变量：

```bash
# .env 文件内容
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
```

### 基础使用示例

#### 1. 创建数据库管理器实例

```python
from postgres_manager import PostgreSQLManager

# 使用上下文管理器（推荐）
with PostgreSQLManager() as db:
    # 数据库操作
    pass

# 或者手动管理连接
db = PostgreSQLManager()
db.connect()
# ... 操作
db.close()
```

#### 2. 从CSV文件创建表

```python
with PostgreSQLManager() as db:
    # 从CSV文件创建表
    success = db.create_table(
        table_name="stock_pb_ratio",
        data_source="sample_data/Fundamental_PB_Ratio.csv",
        overwrite=True  # 如果表存在则覆盖
    )
  
    if success:
        print("表创建成功!")
  
        # 获取表信息
        info = db.get_table_info("stock_pb_ratio")
        print(f"表信息: {info}")
        # 输出: {'row_count': 1043, 'column_count': 15, ...}
```

#### 3. 从DataFrame创建表

```python
import pandas as pd
import numpy as np

# 创建示例数据
dates = pd.date_range('2020-01-01', periods=100, freq='D')
data = pd.DataFrame({
    '000001.SZ': np.random.randn(100),
    '000002.SZ': np.random.randn(100),
    '600000.SH': np.random.randn(100)
}, index=dates)

with PostgreSQLManager() as db:
    success = db.create_table("custom_data", data, overwrite=True)
    if success:
        print("从DataFrame创建表成功!")
```

#### 4. 从字典创建表

```python
# 创建字典数据
data_dict = {
    'datetime': ['2020-01-01', '2020-01-02', '2020-01-03'],
    '000001.SZ': [1.23, 1.45, 1.67],
    '000002.SZ': [2.34, 2.56, 2.78],
    '600000.SH': [3.45, 3.67, 3.89]
}

with PostgreSQLManager() as db:
    success = db.create_table("dict_data", data_dict, overwrite=True)
    if success:
        print("从字典创建表成功!")
```

#### 5. 数据查询操作

```python
with PostgreSQLManager() as db:
    # 基础查询 - 获取所有数据
    df_all = db.query_data("stock_pb_ratio")
    print(f"全部数据形状: {df_all.shape}")
  
    # 限制查询行数
    df_limited = db.query_data("stock_pb_ratio", limit=100)
    print(f"限制100行: {df_limited.shape}")
  
    # 时间切片查询
    df_slice = db.query_data(
        "stock_pb_ratio",
        start_date='2020-01-01',
        end_date='2020-12-31'
    )
    print(f"2020年数据: {df_slice.shape}")
  
    # 列筛选查询
    df_columns = db.query_data(
        "stock_pb_ratio",
        columns=['000001.SZ', '000002.SZ', '600000.SH']
    )
    print(f"选定列数据: {df_columns.shape}")
  
    # 组合查询
    df_complex = db.query_data(
        "stock_pb_ratio",
        start_date='2021-01-01',
        end_date='2021-06-30',
        columns=['000001.SZ', '600000.SH'],
        limit=50
    )
    print(f"复合查询结果: {df_complex.shape}")
```

#### 6. 数据插入和更新

```python
# 准备新数据
new_dates = pd.date_range('2023-01-01', periods=10, freq='D')
new_data = pd.DataFrame({
    '000001.SZ': np.random.randn(10),
    '000002.SZ': np.random.randn(10),
    '600000.SH': np.random.randn(10)
}, index=new_dates)

with PostgreSQLManager() as db:
    # 插入新数据
    success = db.insert_data("stock_pb_ratio", new_data)
    if success:
        print("数据插入成功!")
  
    # 查询验证
    latest_data = db.query_data(
        "stock_pb_ratio",
        start_date='2023-01-01'
    )
    print(f"新插入的数据: {latest_data.shape}")
```

### 高级功能使用

#### 1. 批量数据导入

```python
from advanced_manager import AdvancedPostgreSQLManager

with AdvancedPostgreSQLManager() as db:
    # 批量导入多个数据源（混合类型）
    data_sources = [
        "sample_data/Fundamental_PB_Ratio.csv",  # CSV文件
        dataframe_pe_ratio,                      # DataFrame对象
        {"datetime": ["2020-01-01"], "000001.SZ": [1.23]}  # 字典数据
    ]
  
    table_names = ["pb_data", "pe_data", "custom_data"]
  
    results = db.batch_insert_data(data_sources, table_names, overwrite=True)
    print(f"批量导入结果: {results}")
```

#### 2. 多因子查询功能

```python
with AdvancedPostgreSQLManager() as db:
    # 跨表多因子查询
    result = db.query_data_multifactor(
        start_date='2020-01-01',
        end_date='2020-12-31',
        codes=['000001.SZ', '000002.SZ', '600000.SH']  # 可选：指定股票代码
    )
  
    print(f"多因子数据形状: {result.shape}")
    print(f"包含因子: {result.columns.tolist()}")
  
    # 结果包含所有因子表的数据，按时间和股票代码组织
    # 列名格式: factor_table_name + '_' + stock_code
```

#### 3. 批量文件导入

```python
with AdvancedPostgreSQLManager() as db:
    # 批量导入文件夹中的所有CSV文件
    results = db.batch_insert_files(
        file_directory="sample_data/",
        table_prefix="factor_",
        file_pattern="*.csv",
        overwrite=True
    )
  
    print(f"导入结果: {results}")
  
    # 查询导入的表
    tables = db.list_tables()
    factor_tables = [t for t in tables if t.startswith('factor_')]
    print(f"因子表: {factor_tables}")
```

#### 4. 数据统计分析

```python
with AdvancedPostgreSQLManager() as db:
    # 获取数据统计信息
    stats = db.get_data_statistics("stock_pb_ratio")
    print("数据统计:")
    print(f"- 总行数: {stats['row_count']}")
    print(f"- 总列数: {stats['column_count']}")
    print(f"- 日期范围: {stats['date_range']}")
    print(f"- 缺失值: {stats['missing_values']}")
  
    # 获取数据质量报告
    quality_report = db.validate_data_quality("stock_pb_ratio")
    print(f"数据质量: {quality_report}")
```

#### 5. 数据导出

```python
with AdvancedPostgreSQLManager() as db:
    # 导出全部数据
    success = db.export_data(
        "stock_pb_ratio", 
        "exported_data.csv"
    )
  
    # 导出部分数据
    success = db.export_data(
        "stock_pb_ratio",
        "partial_data.csv",
        start_date='2020-01-01',
        end_date='2020-12-31',
        limit=1000
    )
  
    if success:
        print("数据导出成功!")
```

#### 6. 表格管理

```python
with PostgreSQLManager() as db:
    # 列出所有表
    tables = db.list_tables()
    print(f"数据库中的表: {tables}")
  
    # 获取表详细信息
    for table in tables:
        info = db.get_table_info(table)
        print(f"{table}: {info}")
  
    # 删除表（谨慎使用）
    # db.drop_table("old_table_name")
```

## 📋 API文档

### PostgreSQLManager 类

#### 核心方法

##### `__init__(host=None, port=None, database=None, user=None, password=None)`

初始化数据库管理器

**参数**:

- `host` (str, optional): 数据库主机地址，默认从环境变量读取
- `port` (int, optional): 数据库端口，默认5432
- `database` (str, optional): 数据库名称
- `user` (str, optional): 用户名
- `password` (str, optional): 密码

##### `create_table(table_name, data_source, overwrite=False)`

创建数据库表

**参数**:

- `table_name` (str): 表名称
- `data_source` (str|DataFrame|dict): CSV文件路径、pandas DataFrame或字典数据
- `overwrite` (bool): 是否覆盖已存在的表

**返回**: `bool` - 创建是否成功

**示例**:

```python
# 从CSV创建
success = db.create_table("my_table", "data.csv", overwrite=True)

# 从DataFrame创建
success = db.create_table("my_table", dataframe, overwrite=True)

# 从字典创建
data_dict = {'datetime': ['2020-01-01'], 'col1': [1.23]}
success = db.create_table("my_table", data_dict, overwrite=True)
```

##### `insert_data(table_name, data_source, update_existing=False)`

插入数据

**参数**:

- `table_name` (str): 表名称
- `data_source` (str|DataFrame|dict): 数据源（CSV文件路径、DataFrame或字典）
- `update_existing` (bool): 是否更新已存在的数据

**返回**: `bool` - 插入是否成功

**示例**:

```python
# 插入DataFrame数据
new_data = pd.DataFrame({...})
success = db.insert_data("my_table", new_data)

# 插入字典数据
dict_data = {'datetime': ['2023-01-01'], 'col1': [1.23]}
success = db.insert_data("my_table", dict_data)
```

##### `query_data(table_name, start_date=None, end_date=None, columns=None, limit=None)`

查询数据

**参数**:

- `table_name` (str): 表名称
- `start_date` (str, optional): 开始日期 (YYYY-MM-DD)
- `end_date` (str, optional): 结束日期 (YYYY-MM-DD)
- `columns` (list, optional): 要查询的列名列表
- `limit` (int, optional): 限制返回行数

**返回**: `pandas.DataFrame` - 查询结果

**示例**:

```python
# 查询全部数据
df = db.query_data("my_table")

# 时间切片查询
df = db.query_data("my_table", start_date='2020-01-01', end_date='2020-12-31')

# 列筛选查询
df = db.query_data("my_table", columns=['col1', 'col2'])

# 组合查询
df = db.query_data("my_table", start_date='2020-01-01', columns=['col1'], limit=100)
```

##### `get_table_info(table_name)`

获取表信息

**参数**:

- `table_name` (str): 表名称

**返回**: `dict` - 表信息字典

**示例**:

```python
info = db.get_table_info("my_table")
# 返回: {'row_count': 1000, 'column_count': 10, 'columns': [...]}
```

##### `list_tables()`

列出所有表

**返回**: `list` - 表名列表

##### `drop_table(table_name)`

删除表

**参数**:

- `table_name` (str): 表名称

**返回**: `bool` - 删除是否成功

### AdvancedPostgreSQLManager 类

继承自 `PostgreSQLManager`，提供额外的高级功能。

##### `batch_insert_data(data_sources, table_names, overwrite=False)`

批量导入多种数据源

**参数**:

- `data_sources` (list): 数据源列表（CSV文件路径、DataFrame或字典）
- `table_names` (list): 对应的表名列表
- `overwrite` (bool): 是否覆盖已存在的表

**返回**: `dict` - 每个数据源的导入结果

**示例**:

```python
data_sources = ["file1.csv", dataframe, {"datetime": ["2020-01-01"], "col1": [1.23]}]
table_names = ["table1", "table2", "table3"]
results = db.batch_insert_data(data_sources, table_names, overwrite=True)
```

##### `query_data_multifactor(start_date=None, end_date=None, codes=None)`

多因子查询功能

**参数**:

- `start_date` (str, optional): 开始日期 (YYYY-MM-DD)
- `end_date` (str, optional): 结束日期 (YYYY-MM-DD)
- `codes` (list, optional): 股票代码列表

**返回**: `pandas.DataFrame` - 多因子查询结果

**示例**:

```python
# 查询所有因子数据
df = db.query_data_multifactor(start_date='2020-01-01', end_date='2020-12-31')

# 查询指定股票的因子数据
df = db.query_data_multifactor(codes=['000001.SZ', '600000.SH'])
```

##### `batch_insert_files(file_directory, table_prefix="", file_pattern="*.csv", overwrite=False)`

批量导入文件夹中的文件

**参数**:

- `file_directory` (str): 文件夹路径
- `table_prefix` (str): 表名前缀
- `file_pattern` (str): 文件匹配模式
- `overwrite` (bool): 是否覆盖已存在的表

**返回**: `dict` - 每个文件的导入结果

##### `get_data_statistics(table_name)`

获取数据统计信息

**参数**:

- `table_name` (str): 表名称

**返回**: `dict` - 统计信息字典

##### `export_data(table_name, output_file, **kwargs)`

导出数据

**参数**:

- `table_name` (str): 表名称
- `output_file` (str): 输出文件路径
- `**kwargs`: 查询参数（同query_data方法）

**返回**: `bool` - 导出是否成功

##### `validate_data_quality(table_name)`

验证数据质量

**参数**:

- `table_name` (str): 表名称

**返回**: `dict` - 数据质量报告

## ⚙️ 配置说明

### 环境变量配置

系统支持通过环境变量或 `.env` 文件进行配置：

```bash
# 数据库连接配置
DB_HOST=localhost          # 数据库主机
DB_PORT=5432              # 数据库端口
DB_NAME=financial_data    # 数据库名称
DB_USER=postgres          # 用户名
DB_PASSWORD=your_password # 密码

# 日志配置
LOG_LEVEL=INFO            # 日志级别: DEBUG, INFO, WARNING, ERROR
LOG_FILE=datadeal.log     # 日志文件路径

# 性能配置
BATCH_SIZE=1000           # 批量操作大小
CONNECTION_TIMEOUT=30     # 连接超时时间（秒）
```

### 数据库优化配置

推荐的PostgreSQL配置优化：

```sql
-- 针对时间序列数据的优化
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;
```

## 🧪 测试和验证

### 运行测试

```bash
# 基础功能测试
python simple_test.py

# 综合功能测试
python comprehensive_test.py

# 完整演示
python demo_usage.py
```

### 测试覆盖范围

- ✅ 数据库连接测试
- ✅ 表格创建和删除测试
- ✅ 数据导入和查询测试
- ✅ 时间切片功能测试
- ✅ 批量操作测试
- ✅ 错误处理测试
- ✅ 性能基准测试

## 📈 性能特性

### 性能指标

- **数据导入速度**: 10,000+ 行/秒
- **查询响应时间**: < 100ms (典型查询)
- **内存使用**: 优化的批量处理，内存占用 < 500MB
- **并发支持**: 支持多连接并发操作

### 性能优化建议

1. **索引优化**: 系统自动为datetime列创建索引
2. **批量操作**: 使用批量插入而非逐行插入
3. **分页查询**: 对大数据集使用limit参数
4. **连接池**: 在高并发场景下使用连接池

## 📝 日志记录

### 日志级别

- `DEBUG`: 详细的调试信息
- `INFO`: 一般信息记录
- `WARNING`: 警告信息
- `ERROR`: 错误信息

### 日志格式

```
2025-01-21 18:15:02 | INFO | 数据开始导入表 stock_data
2025-01-21 18:15:02 | INFO | 当前时间2025-01-21, 导入时间点2020-01-01 因子PB_Ratio 导入成功
2025-01-21 18:15:02 | INFO | 当前时间2025-01-21, 导入时间点2020-01-02 因子PB_Ratio 导入成功
...
2025-01-21 18:15:05 | INFO | 数据成功导入表 stock_data, 共 1043 行
```

### 查看日志

```python
# 查看最新日志
with open('datadeal.log', 'r', encoding='utf-8') as f:
    print(f.read())
```

## ⚠️ 注意事项

### 数据格式要求

1. **时间列**: 必须命名为 `datetime`，格式为 YYYY-MM-DD
2. **数值列**: 必须为数值类型，支持整数和浮点数
3. **列名**: 避免使用PostgreSQL保留字作为列名
4. **编码**: CSV文件建议使用UTF-8编码

### 安全注意事项

1. **数据库凭据**: 不要在代码中硬编码数据库密码
2. **SQL注入**: 系统已内置SQL注入防护
3. **权限控制**: 确保数据库用户具有适当的权限
4. **备份**: 定期备份重要数据

### 性能注意事项

1. **大数据集**: 对于超大数据集，建议分批处理
2. **内存管理**: 查询大量数据时使用limit参数
3. **索引维护**: 定期维护数据库索引
4. **连接管理**: 使用上下文管理器确保连接正确关闭

## 🔧 故障排除

### 常见问题

#### 1. 连接失败

```
错误: could not connect to server
解决: 检查数据库服务是否启动，确认连接参数正确
```

#### 2. 表已存在

```
错误: relation "table_name" already exists
解决: 使用 overwrite=True 参数或手动删除表
```

#### 3. 数据类型错误

```
错误: invalid input syntax for type double precision
解决: 检查CSV文件中的数据格式，确保数值列不包含非数字字符
```

#### 4. 内存不足

```
错误: out of memory
解决: 使用limit参数限制查询结果，或增加系统内存
```

### 调试模式

启用详细日志记录：

```python
import logging
logging.basicConfig(level=logging.DEBUG)

with PostgreSQLManager() as db:
    # 详细的操作日志将被记录
    pass
```

## 📚 版本历史

### v3.0.0 (2025-01-21) - 长表格格式重构

- 🔄 **重大更新**: 数据存储格式从宽表格（datetime*code）改为长表格（datetime, code, metric, value）
- 🎯 **解决问题**: 突破PostgreSQL 1600字段限制，支持全A股5000只股票数据
- ✅ **核心修改**:
  - 修改 `_load_data` 函数：将宽表格数据转换为长表格格式
  - 修改 `_create_table_from_dataframe` 函数：创建标准化的长表格结构（datetime, code, metric, value）
  - 修改 `_insert_dataframe` 函数：适配长表格数据插入，保持日期日志显示
  - 修改 `query_data` 函数：从长表格读取数据后转换为宽表格格式返回
  - 保持 `query_data_multifactor` 函数原有功能，适配长表格查询
  - 修改 `advanced_manager.py` 中相关函数以支持新的长表格格式
- 🚀 **性能提升**: 支持更大规模的股票数据存储和查询
- 📊 **兼容性**: 查询接口保持不变，用户无需修改现有代码

### v2.0.0 (2025-01-21)

- ✅ **新增功能**: 支持pandas DataFrame和字典数据源
- ✅ **新增功能**: 多因子查询功能，支持跨表联合查询
- ✅ **新增功能**: 批量数据导入，支持混合数据源类型
- ✅ **增强功能**: 改进数据加载机制，支持更多数据格式
- ✅ **优化性能**: 提升大数据集处理效率
- ✅ **完善文档**: 更新API文档和使用示例

### v1.0.0 (2025-01-21)

- ✅ 初始版本发布
- ✅ 核心数据管理功能
- ✅ 高级分析功能
- ✅ 完整的测试套件
- ✅ 详细的文档和示例

## 🤝 贡献指南

欢迎贡献代码和建议！

### 开发环境设置

```bash
# 克隆项目
git clone <repository_url>
cd Datadeal

# 安装依赖
pip install -r requirements.txt

# 运行测试
python -m pytest tests/
```

### 提交规范

- 使用清晰的提交信息
- 添加适当的测试用例
- 更新相关文档
- 遵循代码风格规范

## 📞 支持与联系

如有问题或建议，请通过以下方式联系：

- 📧 Email: cbw_18810739172@163.com
- 🐛 Issues: [GitHub Issues](https://github.com/your-repo/issues)

---

**PostgreSQL数据管理系统** - 让金融数据管理变得简单高效！ 🚀
