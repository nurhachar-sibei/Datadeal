"""
生成长格式（Panel Data）的因子数据文件
适用于多公司多时间点的数据存储

数据格式：
- 第一列：时间（datetime）
- 第二列：公司名（company）
- 第三到N列：各种因子值
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# 创建 long_data 文件夹
output_dir = Path('long_data')
output_dir.mkdir(exist_ok=True)

print("=" * 80)
print("生成长格式因子数据")
print("=" * 80)

# ============================================================================
# 配置参数
# ============================================================================

# 公司列表（示例：30家公司）
COMPANIES = [
    'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA',
    'META', 'NVDA', 'AMD', 'INTC', 'IBM',
    'JPM', 'BAC', 'WFC', 'GS', 'MS',
    'JNJ', 'PFE', 'UNH', 'MRK', 'ABBV',
    'XOM', 'CVX', 'COP', 'SLB', 'MPC',
    'WMT', 'HD', 'TGT', 'COST', 'LOW'
]

# 时间范围
START_DATE = '2020-01-01'
END_DATE = '2024-12-31'
FREQ = 'D'  # 日频数据，也可以改为 'W'（周）、'M'（月）、'Q'（季）

# 因子配置
FACTORS = {
    # 基本面因子
    'pe_ratio': (5, 50),          # 市盈率
    'pb_ratio': (0.5, 10),        # 市净率
    'roe': (-0.2, 0.4),           # 净资产收益率
    'roa': (-0.1, 0.3),           # 总资产收益率
    'debt_ratio': (0.1, 0.9),     # 资产负债率
    'revenue_growth': (-0.3, 0.5), # 营收增长率
    
    # 技术指标
    'rsi': (20, 80),              # 相对强弱指标
    'macd': (-2, 2),              # MACD
    'volatility': (0.05, 0.5),    # 波动率
    'volume_ratio': (0.5, 3.0),   # 成交量比率
    
    # 动量因子
    'momentum_5d': (-0.1, 0.1),   # 5日动量
    'momentum_20d': (-0.2, 0.2),  # 20日动量
    'momentum_60d': (-0.3, 0.3),  # 60日动量
    
    # 质量因子
    'gross_margin': (0.1, 0.6),   # 毛利率
    'operating_margin': (0.05, 0.4), # 营业利润率
    'cash_ratio': (0.1, 2.0),     # 现金比率
    
    # 估值因子
    'ev_ebitda': (5, 30),         # 企业价值倍数
    'ps_ratio': (0.5, 15),        # 市销率
    'pcf_ratio': (5, 25),         # 市现率
    
    # 成长因子
    'earnings_growth': (-0.3, 0.6), # 盈利增长率
    'asset_growth': (-0.2, 0.4),    # 资产增长率
}

# ============================================================================
# 生成数据函数
# ============================================================================

def generate_panel_data(companies, start_date, end_date, freq, factors, 
                        missing_rate=0.05, output_file=None):
    """
    生成面板数据（长格式）
    
    Args:
        companies: 公司列表
        start_date: 开始日期
        end_date: 结束日期
        freq: 频率（'D'日, 'W'周, 'M'月, 'Q'季）
        factors: 因子字典 {因子名: (最小值, 最大值)}
        missing_rate: 缺失率（0-1）
        output_file: 输出文件路径
    """
    print(f"\n生成数据配置:")
    print(f"  - 公司数量: {len(companies)}")
    print(f"  - 时间范围: {start_date} 至 {end_date}")
    print(f"  - 数据频率: {freq}")
    print(f"  - 因子数量: {len(factors)}")
    print(f"  - 缺失率: {missing_rate*100}%")
    
    # 生成日期序列
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    
    # 创建基础数据框架
    data_list = []
    
    for date in date_range:
        for company in companies:
            # 随机决定是否缺失该公司在该日期的数据（模拟真实场景）
            if np.random.random() > missing_rate:
                row = {
                    'datetime': date,
                    'company': company
                }
                
                # 生成各因子值
                for factor_name, (min_val, max_val) in factors.items():
                    # 正态分布生成，更符合真实数据
                    mean = (min_val + max_val) / 2
                    std = (max_val - min_val) / 6  # 约99.7%的数据在范围内
                    value = np.random.normal(mean, std)
                    
                    # 限制在范围内
                    value = np.clip(value, min_val, max_val)
                    
                    # 随机添加一些NaN（缺失因子值）
                    if np.random.random() < missing_rate * 0.5:
                        value = np.nan
                    
                    row[factor_name] = value
                
                data_list.append(row)
    
    # 创建DataFrame
    df = pd.DataFrame(data_list)
    
    # 排序
    df = df.sort_values(['datetime', 'company']).reset_index(drop=True)
    
    print(f"\n生成结果:")
    print(f"  - 总记录数: {len(df):,}")
    print(f"  - 唯一日期数: {df['datetime'].nunique():,}")
    print(f"  - 唯一公司数: {df['company'].nunique()}")
    print(f"  - 数据形状: {df.shape}")
    
    # 保存文件
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"  - 已保存至: {output_file}")
    
    return df

# ============================================================================
# 生成多个因子文件
# ============================================================================

print("\n" + "=" * 80)
print("开始生成因子文件")
print("=" * 80)

# 1. 完整因子数据集
print("\n【1】生成完整因子数据集")
print("-" * 80)
df_full = generate_panel_data(
    companies=COMPANIES,
    start_date=START_DATE,
    end_date=END_DATE,
    freq=FREQ,
    factors=FACTORS,
    missing_rate=0.05,
    output_file=output_dir / 'full_factors.csv'
)

# 2. 基本面因子
print("\n【2】生成基本面因子数据集")
print("-" * 80)
fundamental_factors = {
    'pe_ratio': (5, 50),
    'pb_ratio': (0.5, 10),
    'roe': (-0.2, 0.4),
    'roa': (-0.1, 0.3),
    'debt_ratio': (0.1, 0.9),
    'revenue_growth': (-0.3, 0.5),
    'gross_margin': (0.1, 0.6),
    'operating_margin': (0.05, 0.4),
}

df_fundamental = generate_panel_data(
    companies=COMPANIES,
    start_date=START_DATE,
    end_date=END_DATE,
    freq='M',  # 基本面通常是月度或季度
    factors=fundamental_factors,
    missing_rate=0.03,
    output_file=output_dir / 'fundamental_factors.csv'
)

# 3. 技术指标
print("\n【3】生成技术指标数据集")
print("-" * 80)
technical_factors = {
    'rsi': (20, 80),
    'macd': (-2, 2),
    'volatility': (0.05, 0.5),
    'volume_ratio': (0.5, 3.0),
    'momentum_5d': (-0.1, 0.1),
    'momentum_20d': (-0.2, 0.2),
    'momentum_60d': (-0.3, 0.3),
}

df_technical = generate_panel_data(
    companies=COMPANIES,
    start_date=START_DATE,
    end_date=END_DATE,
    freq='D',  # 技术指标通常是日频
    factors=technical_factors,
    missing_rate=0.02,
    output_file=output_dir / 'technical_factors.csv'
)

# 4. 小样本数据（用于测试）
print("\n【4】生成测试用小样本数据集")
print("-" * 80)
test_factors = {
    'factor_A': (0, 100),
    'factor_B': (-10, 10),
    'factor_C': (0, 1),
}

df_test = generate_panel_data(
    companies=COMPANIES[:5],  # 只选5家公司
    start_date='2024-01-01',
    end_date='2024-03-31',
    freq='D',
    factors=test_factors,
    missing_rate=0.1,
    output_file=output_dir / 'test_sample.csv'
)

# ============================================================================
# 数据预览
# ============================================================================

print("\n" + "=" * 80)
print("数据预览")
print("=" * 80)

print("\n完整因子数据集（前10行）:")
print("-" * 80)
print(df_full.head(10))

print("\n\n数据统计信息:")
print("-" * 80)
print(df_full.describe())

print("\n\n数据质量检查:")
print("-" * 80)
print(f"缺失值统计:")
missing_stats = df_full.isnull().sum()
print(missing_stats[missing_stats > 0])

# ============================================================================
# 生成数据说明文档
# ============================================================================

readme_content = """# 长格式因子数据说明

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

## 使用 EasyManager 导入数据库

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
"""

readme_path = output_dir / 'README.md'
with open(readme_path, 'w', encoding='utf-8') as f:
    f.write(readme_content)

print(f"\n已生成数据说明文档: {readme_path}")

# ============================================================================
# 生成导入示例脚本
# ============================================================================

import_example = """
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
    print("\\n【1】导入基本面因子数据")
    print("-" * 80)
    
    df_fund = pd.read_csv('long_data/fundamental_factors.csv')
    df_fund['datetime'] = pd.to_datetime(df_fund['datetime'])
    df_fund = df_fund.set_index('datetime')
    
    em.create_table('fundamental_factors', df_fund, overwrite=True)
    print(f"✓ 已导入 {len(df_fund)} 条基本面数据")
    
    # 示例2: 导入技术指标（日频数据）
    print("\\n【2】导入技术指标数据")
    print("-" * 80)
    
    df_tech = pd.read_csv('long_data/technical_factors.csv')
    df_tech['datetime'] = pd.to_datetime(df_tech['datetime'])
    df_tech = df_tech.set_index('datetime')
    
    em.create_table('technical_factors', df_tech, overwrite=True)
    print(f"✓ 已导入 {len(df_tech)} 条技术指标数据")
    
    # 示例3: 查看导入的表
    print("\\n【3】查看导入的表")
    print("-" * 80)
    
    em.list_tables(pattern='%factors%', verbose=True, print_table=True)
    
    # 示例4: 查看表详细信息
    print("\\n【4】查看表详细信息")
    print("-" * 80)
    
    em.get_table_info('fundamental_factors', print_info=True)

print("\\n" + "=" * 80)
print("导入完成！")
print("=" * 80)
"""

example_path = output_dir / 'import_example.py'
with open(example_path, 'w', encoding='utf-8') as f:
    f.write(import_example)

print(f"已生成导入示例脚本: {example_path}")

print("\n" + "=" * 80)
print("[完成] 所有文件生成完成！")
print("=" * 80)
print(f"\n生成的文件:")
print(f"  1. {output_dir / 'full_factors.csv'}")
print(f"  2. {output_dir / 'fundamental_factors.csv'}")
print(f"  3. {output_dir / 'technical_factors.csv'}")
print(f"  4. {output_dir / 'test_sample.csv'}")
print(f"  5. {output_dir / 'README.md'}")
print(f"  6. {output_dir / 'import_example.py'}")

print(f"\n下一步:")
print(f"  - 查看数据: 打开 CSV 文件查看生成的数据")
print(f"  - 阅读说明: 查看 {output_dir / 'README.md'}")
print(f"  - 导入数据库: 运行 python {output_dir / 'import_example.py'}")

print("=" * 80 + "\n")

