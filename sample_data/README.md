# Datadeal 样本数据说明

本目录包含了用于测试和演示的股票样本数据，所有数据均为模拟生成，仅供学习和测试使用。

## 数据格式

所有数据文件均采用**宽表格格式**：
- **行索引**: datetime (交易日期，YYYY-MM-DD格式)
- **列索引**: 股票代码 (如 000001.SZ, 600000.SH等)
- **数据值**: 对应的价格或因子数值

## 数据文件结构

### 价格数据 (Price_*.csv)
- `Price_Open.csv` - 开盘价数据
- `Price_High.csv` - 最高价数据
- `Price_Low.csv` - 最低价数据
- `Price_Close.csv` - 收盘价数据
- `Price_Volume.csv` - 成交量数据

### 技术指标 (Technical_*.csv)
- `Technical_RSI.csv` - RSI相对强弱指标 (0-100)
- `Technical_MACD.csv` - MACD指标
- `Technical_BB_Width.csv` - 布林带宽度
- `Technical_MA_Ratio.csv` - 移动平均线比率 (MA5/MA20)

### 基本面因子 (Fundamental_*.csv)
- `Fundamental_PE_Ratio.csv` - 市盈率
- `Fundamental_PB_Ratio.csv` - 市净率
- `Fundamental_ROE.csv` - 净资产收益率
- `Fundamental_Revenue_Growth.csv` - 营收增长率

## 股票代码列表

包含以下15只股票的数据：
- 000001.SZ (平安银行)
- 000002.SZ (万科A)
- 000858.SZ (五粮液)
- 002415.SZ (海康威视)
- 002594.SZ (比亚迪)
- 600000.SH (浦发银行)
- 600036.SH (招商银行)
- 600519.SH (贵州茅台)
- 600887.SH (伊利股份)
- 601318.SH (中国平安)
- 300059.SZ (东方财富)
- 300750.SZ (宁德时代)
- 688981.SH (中芯国际)
- 002142.SZ (宁波银行)

## 时间范围

- 开始日期: 2020-01-01
- 结束日期: 2023-12-31
- 仅包含交易日数据（排除周末和节假日）

## 使用示例

### Python pandas 读取数据

```python
import pandas as pd

# 读取收盘价数据
close_data = pd.read_csv('Price_Close.csv', index_col=0, parse_dates=True)
print(close_data.head())

# 读取特定股票的数据
stock_code = '000001.SZ'
stock_close = close_data[stock_code]
print(f"{stock_code} 收盘价数据:")
print(stock_close.head())

# 读取技术指标数据
rsi_data = pd.read_csv('Technical_RSI.csv', index_col=0, parse_dates=True)
print("RSI指标数据:")
print(rsi_data.head())

# 计算相关性
correlation = close_data.corr()
print("股票价格相关性矩阵:")
print(correlation)
```

### 数据分析示例

```python
import matplotlib.pyplot as plt

# 绘制价格走势图
plt.figure(figsize=(12, 6))
close_data['000001.SZ'].plot(title='平安银行股价走势')
plt.ylabel('价格 (元)')
plt.show()

# 计算收益率
returns = close_data.pct_change().dropna()
print("日收益率统计:")
print(returns.describe())

# 计算波动率
volatility = returns.std() * (252 ** 0.5)  # 年化波动率
print("年化波动率:")
print(volatility.sort_values(ascending=False))
```

## 注意事项

1. **模拟数据**: 所有数据均为随机生成，不代表真实市场情况
2. **仅供测试**: 请勿用于实际投资决策
3. **数据完整性**: 所有交易日都有完整的数据记录
4. **格式统一**: 所有文件采用UTF-8编码，CSV格式

## 数据生成

如需重新生成样本数据，请运行：

```bash
python generate_sample_data.py
```

或直接使用生成器类：

```python
from sample_data_generator import SampleDataGenerator

generator = SampleDataGenerator(
    start_date="2020-01-01",
    end_date="2023-12-31",
    output_dir="sample_data"
)
generator.generate_all_sample_data()
```