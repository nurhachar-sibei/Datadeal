# 长格式因子数据说明

## 数据格式

这些文件采用**长格式（Panel Data）**存储，每一行代表一个公司在某个时间点的观测值。

### 列结构
1. **datetime**: 观测时间
2. **company**: 公司代码
3. **factor_xxx**: 各种因子值

### 示例
```
datetime,company,pe_ratio,pb_ratio,roe
2020-01-01,AAPL,25.3,8.5,0.35
2020-01-01,GOOGL,28.7,6.2,0.28
2020-01-02,AAPL,25.5,8.6,0.35
...
```

## 文件说明

### 1. full_factors.csv
- **描述**: 完整的因子数据集，包含所有类型的因子
- **公司数**: 30家
- **时间范围**: 2020-01-01 至 2024-12-31
- **频率**: 日频
- **因子数**: 20+

### 2. fundamental_factors.csv
- **描述**: 基本面因子数据集
- **公司数**: 30家
- **时间范围**: 2020-01-01 至 2024-12-31
- **频率**: 月频
- **因子**: PE、PB、ROE、ROA、资产负债率等

### 3. technical_factors.csv
- **描述**: 技术指标数据集
- **公司数**: 30家
- **时间范围**: 2020-01-01 至 2024-12-31
- **频率**: 日频
- **因子**: RSI、MACD、波动率、动量等

### 4. test_sample.csv
- **描述**: 小样本测试数据
- **公司数**: 5家
- **时间范围**: 2024-01-01 至 2024-03-31
- **频率**: 日频
- **用途**: 快速测试和开发

## 因子列表

### 基本面因子
- `pe_ratio`: 市盈率
- `pb_ratio`: 市净率
- `roe`: 净资产收益率
- `roa`: 总资产收益率
- `debt_ratio`: 资产负债率
- `revenue_growth`: 营收增长率
- `gross_margin`: 毛利率
- `operating_margin`: 营业利润率

### 技术因子
- `rsi`: 相对强弱指标
- `macd`: MACD指标
- `volatility`: 波动率
- `volume_ratio`: 成交量比率
- `momentum_5d/20d/60d`: 动量指标

### 估值因子
- `ev_ebitda`: 企业价值倍数
- `ps_ratio`: 市销率
- `pcf_ratio`: 市现率

### 成长因子
- `earnings_growth`: 盈利增长率
- `asset_growth`: 资产增长率

## 使用 LongManager 导入数据库（推荐）

**LongManager 是专门为长格式数据设计的管理器！**

### 为什么使用 LongManager？

- ✅ **专门处理 Panel Data**: 针对长格式数据优化
- ✅ **复合键去重**: 基于（时间，公司）判断重复
- ✅ **自动列顺序**: 时间列第一，公司列第二
- ✅ **高效索引**: 自动创建复合索引提高查询性能
- ✅ **更简单**: 不需要手动设置索引

### 基本用法

```python
from easy_manager import LongManager
import pandas as pd

# 1. 初始化
with LongManager(time_col='datetime', entity_col='company') as lm:
    
    # 2. 读取数据
    df = pd.read_csv('long_data/full_factors.csv')
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    # 3. 创建表（自动处理）
    lm.create_table('factor_panel', df)
    
    # 4. 增量更新（基于时间-公司键去重）
    df_new = pd.read_csv('long_data/new_data.csv')
    df_new['datetime'] = pd.to_datetime(df_new['datetime'])
    lm.insert_data('factor_panel', df_new, mode='skip')
    
    # 5. 添加新因子列
    df_new_factors = pd.read_csv('long_data/new_factors.csv')
    df_new_factors['datetime'] = pd.to_datetime(df_new_factors['datetime'])
    lm.add_columns('factor_panel', df_new_factors)
```

### 完整示例

参见 `import_with_longmanager.py` 文件。

---

## 使用 EasyManager 导入数据库（传统方法）

### 方法1: 设置索引后导入

```python
from easy_manager import EasyManager
import pandas as pd

# 读取数据
df = pd.read_csv('long_data/full_factors.csv')
df['datetime'] = pd.to_datetime(df['datetime'])

# 方案A: 使用 datetime 作为索引
df = df.set_index('datetime')

with EasyManager() as em:
    em.create_table('factors_by_date', df)

# 方案B: 使用 datetime + company 作为复合索引
df = df.set_index(['datetime', 'company'])

with EasyManager() as em:
    em.create_table('factors_panel', df)
```

### 方法2: 不设置索引直接导入

```python
from easy_manager import EasyManager
import pandas as pd

df = pd.read_csv('long_data/full_factors.csv')
df['datetime'] = pd.to_datetime(df['datetime'])

with EasyManager() as em:
    # 直接导入，会自动创建 index 列
    em.create_table('factors_long', df)
```

### 方法3: 分批导入大文件

```python
from easy_manager import EasyManager
import pandas as pd

with EasyManager() as em:
    # 分块读取大文件
    for chunk in pd.read_csv('long_data/full_factors.csv', chunksize=10000):
        chunk['datetime'] = pd.to_datetime(chunk['datetime'])
        chunk = chunk.set_index('datetime')
        
        # 使用 append 模式追加数据
        em.insert_data('factors_large', chunk, mode='append')
```

## 数据特点

1. **面板数据结构**: 同一时间有多个公司，同一公司有多个时间点
2. **真实性模拟**: 包含适量的缺失值，更接近真实数据
3. **数据分布**: 使用正态分布生成，符合金融数据特征
4. **灵活频率**: 支持日频、周频、月频、季频等

## 注意事项

⚠️ **导入数据库前必须转换时间格式**

```python
df['datetime'] = pd.to_datetime(df['datetime'])
```

⚠️ **使用 skip 或 update 模式时需要设置索引**

```python
df = df.set_index('datetime')  # 或其他列
em.insert_data('table', df, mode='skip')
```

⚠️ **大文件建议分批处理**

文件大于10万行时，建议使用 `chunksize` 参数分批读取和导入。

## 生成时间

由 `generate_long_data.py` 生成
