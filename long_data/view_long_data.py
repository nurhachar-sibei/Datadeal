"""
查看 long_data 文件夹中生成的长格式数据
"""

import pandas as pd
from pathlib import Path

print("=" * 80)
print("长格式数据查看工具")
print("=" * 80)

# 数据文件列表
data_files = {
    '1. 完整因子数据集': 'long_data/full_factors.csv',
    '2. 基本面因子': 'long_data/fundamental_factors.csv',
    '3. 技术指标': 'long_data/technical_factors.csv',
    '4. 测试样本': 'long_data/test_sample.csv',
}

# 遍历所有文件
for name, file_path in data_files.items():
    print(f"\n{name}")
    print("-" * 80)
    
    if Path(file_path).exists():
        df = pd.read_csv(file_path)
        
        print(f"总行数: {len(df):,}")
        print(f"时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")
        print(f"公司数量: {df['company'].nunique()}")
        print(f"因子数量: {len(df.columns) - 2}")
        print(f"列名: {', '.join(df.columns.tolist()[:5])}...")
        
        # 显示数据示例
        print("\n数据示例（前5行）:")
        print(df.head(5).to_string())
        
        # 显示特定公司的数据
        if len(df) > 0:
            company = df['company'].iloc[0]
            company_data = df[df['company'] == company].head(3)
            print(f"\n公司 {company} 的前3条记录:")
            print(company_data.to_string())
        
        # 显示缺失值统计
        missing = df.isnull().sum()
        missing = missing[missing > 0]
        if len(missing) > 0:
            print(f"\n缺失值统计:")
            for col, count in missing.items():
                pct = count / len(df) * 100
                print(f"  {col}: {count} ({pct:.1f}%)")
    else:
        print(f"文件不存在: {file_path}")

print("\n" + "=" * 80)
print("查看完成！")
print("=" * 80)
print("\n提示:")
print("  - 长格式数据：每行是一个公司在某时间点的观测")
print("  - 同一时间有多家公司")
print("  - 同一公司有多个时间点")
print("  - 适合 Panel Data 分析")
print("=" * 80 + "\n")

