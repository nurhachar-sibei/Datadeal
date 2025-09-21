#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多因子分析示例
演示如何使用多因子查询功能进行复杂的金融数据分析
包括跨表联合查询、因子相关性分析、投资组合构建等
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import sys

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from postgres_manager import PostgreSQLManager
from advanced_manager import AdvancedPostgreSQLManager

def setup_multifactor_data():
    """设置多因子分析所需的数据"""
    print("=" * 60)
    print("1. 设置多因子分析数据")
    print("=" * 60)
    
    # 股票代码列表
    stock_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
    dates = pd.date_range('2020-01-01', '2023-12-31', freq='D')
    
    with PostgreSQLManager() as db:
        print("\n1.1 创建价格数据表")
        print("-" * 40)
        
        # 1. 价格数据
        np.random.seed(42)  # 确保结果可重现
        price_data = pd.DataFrame(
            np.random.uniform(10.0, 100.0, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        
        # 添加一些趋势性
        for i, code in enumerate(stock_codes):
            trend = np.linspace(0, i*5, len(dates))
            price_data[code] += trend
        
        success = db.create_table("multifactor_prices", price_data, overwrite=True)
        print(f"✓ 价格数据表创建: {success}")
        
        print("\n1.2 创建基本面数据表")
        print("-" * 40)
        
        # 2. 基本面数据 - PB比率
        pb_data = pd.DataFrame(
            np.random.uniform(0.5, 5.0, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        success = db.create_table("multifactor_pb", pb_data, overwrite=True)
        print(f"✓ PB比率数据表创建: {success}")
        
        # 3. 基本面数据 - PE比率
        pe_data = pd.DataFrame(
            np.random.uniform(5.0, 50.0, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        success = db.create_table("multifactor_pe", pe_data, overwrite=True)
        print(f"✓ PE比率数据表创建: {success}")
        
        print("\n1.3 创建技术指标数据表")
        print("-" * 40)
        
        # 4. 技术指标 - RSI
        rsi_data = pd.DataFrame(
            np.random.uniform(20.0, 80.0, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        success = db.create_table("multifactor_rsi", rsi_data, overwrite=True)
        print(f"✓ RSI指标数据表创建: {success}")
        
        # 5. 技术指标 - MACD
        macd_data = pd.DataFrame(
            np.random.uniform(-2.0, 2.0, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        success = db.create_table("multifactor_macd", macd_data, overwrite=True)
        print(f"✓ MACD指标数据表创建: {success}")
        
        print("\n1.4 创建市场数据表")
        print("-" * 40)
        
        # 6. 成交量数据
        volume_data = pd.DataFrame(
            np.random.uniform(1000000, 10000000, (len(dates), len(stock_codes))),
            index=dates,
            columns=stock_codes
        )
        success = db.create_table("multifactor_volume", volume_data, overwrite=True)
        print(f"✓ 成交量数据表创建: {success}")
        
        # 验证数据创建
        tables = db.list_tables()
        multifactor_tables = [t for t in tables if t.startswith('multifactor_')]
        print(f"\n✅ 成功创建 {len(multifactor_tables)} 个多因子数据表")
        
        return stock_codes, multifactor_tables

def demonstrate_basic_multifactor_query():
    """演示基础多因子查询"""
    print("\n" + "=" * 60)
    print("2. 基础多因子查询演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n2.1 单股票多因子查询")
        print("-" * 40)
        
        # 查询单只股票的多个因子
        stock_code = "000001.SZ"
        table_names = ["multifactor_prices", "multifactor_pb", "multifactor_pe", "multifactor_rsi"]
        
        result = db.query_data_multifactor(
            table_names=table_names,
            stock_codes=[stock_code],
            start_date="2023-01-01",
            end_date="2023-01-31"
        )
        
        if result is not None:
            print(f"✓ 查询结果: {result.shape}")
            print("数据预览:")
            print(result.head())
            print("\n数据统计:")
            print(result.describe())
        else:
            print("✗ 查询失败")
        
        print("\n2.2 多股票单因子查询")
        print("-" * 40)
        
        # 查询多只股票的单个因子
        stock_codes = ["000001.SZ", "000002.SZ", "600000.SH"]
        
        result = db.query_data_multifactor(
            table_names=["multifactor_prices"],
            stock_codes=stock_codes,
            start_date="2023-06-01",
            end_date="2023-06-30"
        )
        
        if result is not None:
            print(f"✓ 查询结果: {result.shape}")
            print("股票列表:", [col for col in result.columns if col != 'datetime'])
            print("数据预览:")
            print(result.head())
        
        print("\n2.3 全市场多因子查询")
        print("-" * 40)
        
        # 查询所有股票的多个因子
        result = db.query_data_multifactor(
            table_names=["multifactor_prices", "multifactor_pb", "multifactor_volume"],
            start_date="2023-12-01",
            end_date="2023-12-31",
            limit=100
        )
        
        if result is not None:
            print(f"✓ 查询结果: {result.shape}")
            print("因子数量:", len([col for col in result.columns if col != 'datetime']))
            print("时间范围:", result['datetime'].min(), "到", result['datetime'].max())

def demonstrate_factor_correlation_analysis():
    """演示因子相关性分析"""
    print("\n" + "=" * 60)
    print("3. 因子相关性分析")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n3.1 获取因子数据")
        print("-" * 40)
        
        # 获取多因子数据
        table_names = ["multifactor_prices", "multifactor_pb", "multifactor_pe", 
                      "multifactor_rsi", "multifactor_volume"]
        
        factor_data = db.query_data_multifactor(
            table_names=table_names,
            stock_codes=["000001.SZ"],  # 专注于单只股票
            start_date="2022-01-01",
            end_date="2023-12-31"
        )
        
        if factor_data is None:
            print("✗ 无法获取因子数据")
            return
        
        print(f"✓ 获取因子数据: {factor_data.shape}")
        
        print("\n3.2 计算因子相关性")
        print("-" * 40)
        
        # 提取数值列（排除datetime列）
        numeric_cols = [col for col in factor_data.columns if col != 'datetime']
        correlation_matrix = factor_data[numeric_cols].corr()
        
        print("✓ 相关性矩阵:")
        print(correlation_matrix.round(3))
        
        print("\n3.3 识别高相关性因子对")
        print("-" * 40)
        
        # 找出高相关性的因子对
        high_corr_pairs = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # 相关性阈值
                    high_corr_pairs.append({
                        'factor1': correlation_matrix.columns[i],
                        'factor2': correlation_matrix.columns[j],
                        'correlation': corr_value
                    })
        
        if high_corr_pairs:
            print("✓ 发现高相关性因子对:")
            for pair in high_corr_pairs:
                print(f"  {pair['factor1']} <-> {pair['factor2']}: {pair['correlation']:.3f}")
        else:
            print("✓ 未发现高相关性因子对（|相关系数| > 0.7）")
        
        print("\n3.4 因子统计特征")
        print("-" * 40)
        
        factor_stats = factor_data[numeric_cols].describe()
        print("✓ 因子统计特征:")
        print(factor_stats.round(3))
        
        return factor_data, correlation_matrix

def demonstrate_factor_screening():
    """演示因子筛选功能"""
    print("\n" + "=" * 60)
    print("4. 因子筛选功能演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n4.1 价值因子筛选（低PB）")
        print("-" * 40)
        
        # 筛选PB比率较低的股票
        value_stocks = db.query_data_multifactor(
            table_names=["multifactor_prices", "multifactor_pb"],
            start_date="2023-12-01",
            end_date="2023-12-31"
        )
        
        if value_stocks is not None:
            # 计算每只股票的平均PB
            pb_cols = [col for col in value_stocks.columns if 'multifactor_pb' in col]
            avg_pb = value_stocks[pb_cols].mean()
            
            # 筛选PB < 2.0的股票
            low_pb_stocks = avg_pb[avg_pb < 2.0].index.tolist()
            print(f"✓ 低PB股票 (PB < 2.0): {len(low_pb_stocks)} 只")
            for stock in low_pb_stocks[:5]:  # 显示前5只
                print(f"  {stock}: PB = {avg_pb[stock]:.2f}")
        
        print("\n4.2 技术指标筛选（RSI超卖）")
        print("-" * 40)
        
        # 筛选RSI指标显示超卖的股票
        technical_data = db.query_data_multifactor(
            table_names=["multifactor_rsi"],
            start_date="2023-12-25",
            end_date="2023-12-31"
        )
        
        if technical_data is not None:
            # 获取最新的RSI值
            latest_rsi = technical_data.iloc[-1]
            rsi_cols = [col for col in latest_rsi.index if col != 'datetime']
            
            # 筛选RSI < 30的股票（超卖）
            oversold_stocks = []
            for col in rsi_cols:
                if latest_rsi[col] < 30:
                    oversold_stocks.append((col, latest_rsi[col]))
            
            print(f"✓ 超卖股票 (RSI < 30): {len(oversold_stocks)} 只")
            for stock, rsi in oversold_stocks:
                print(f"  {stock}: RSI = {rsi:.2f}")
        
        print("\n4.3 综合因子筛选")
        print("-" * 40)
        
        # 综合多个因子进行筛选
        comprehensive_data = db.query_data_multifactor(
            table_names=["multifactor_pb", "multifactor_pe", "multifactor_rsi"],
            start_date="2023-12-01",
            end_date="2023-12-31"
        )
        
        if comprehensive_data is not None:
            # 计算平均值
            pb_cols = [col for col in comprehensive_data.columns if 'pb' in col]
            pe_cols = [col for col in comprehensive_data.columns if 'pe' in col]
            rsi_cols = [col for col in comprehensive_data.columns if 'rsi' in col]
            
            avg_pb = comprehensive_data[pb_cols].mean()
            avg_pe = comprehensive_data[pe_cols].mean()
            avg_rsi = comprehensive_data[rsi_cols].mean()
            
            # 综合筛选条件：低PB + 低PE + 适中RSI
            selected_stocks = []
            for stock_code in ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']:
                pb_col = f"multifactor_pb_{stock_code}"
                pe_col = f"multifactor_pe_{stock_code}"
                rsi_col = f"multifactor_rsi_{stock_code}"
                
                if (pb_col in avg_pb.index and pe_col in avg_pe.index and rsi_col in avg_rsi.index):
                    pb_val = avg_pb[pb_col]
                    pe_val = avg_pe[pe_col]
                    rsi_val = avg_rsi[rsi_col]
                    
                    # 筛选条件
                    if pb_val < 3.0 and pe_val < 25.0 and 30 < rsi_val < 70:
                        selected_stocks.append({
                            'stock': stock_code,
                            'pb': pb_val,
                            'pe': pe_val,
                            'rsi': rsi_val
                        })
            
            print(f"✓ 综合筛选结果: {len(selected_stocks)} 只股票")
            for stock_info in selected_stocks:
                print(f"  {stock_info['stock']}: PB={stock_info['pb']:.2f}, "
                      f"PE={stock_info['pe']:.2f}, RSI={stock_info['rsi']:.2f}")

def demonstrate_portfolio_construction():
    """演示投资组合构建"""
    print("\n" + "=" * 60)
    print("5. 投资组合构建演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n5.1 获取组合构建数据")
        print("-" * 40)
        
        # 获取价格和风险因子数据
        portfolio_data = db.query_data_multifactor(
            table_names=["multifactor_prices", "multifactor_pb", "multifactor_pe"],
            start_date="2023-01-01",
            end_date="2023-12-31"
        )
        
        if portfolio_data is None:
            print("✗ 无法获取组合构建数据")
            return
        
        print(f"✓ 获取数据: {portfolio_data.shape}")
        
        print("\n5.2 计算收益率")
        print("-" * 40)
        
        # 提取价格列
        price_cols = [col for col in portfolio_data.columns if 'prices' in col]
        price_data = portfolio_data[['datetime'] + price_cols].copy()
        
        # 计算日收益率
        returns_data = price_data.copy()
        for col in price_cols:
            returns_data[col] = price_data[col].pct_change()
        
        # 删除第一行（NaN值）
        returns_data = returns_data.dropna()
        
        print(f"✓ 计算收益率数据: {returns_data.shape}")
        print("收益率统计:")
        print(returns_data[price_cols].describe().round(4))
        
        print("\n5.3 等权重组合构建")
        print("-" * 40)
        
        # 构建等权重组合
        equal_weight = 1.0 / len(price_cols)
        portfolio_returns = returns_data[price_cols].mean(axis=1)
        
        # 计算组合统计指标
        annual_return = portfolio_returns.mean() * 252
        annual_volatility = portfolio_returns.std() * np.sqrt(252)
        sharpe_ratio = annual_return / annual_volatility if annual_volatility > 0 else 0
        
        print(f"✓ 等权重组合表现:")
        print(f"  年化收益率: {annual_return:.2%}")
        print(f"  年化波动率: {annual_volatility:.2%}")
        print(f"  夏普比率: {sharpe_ratio:.3f}")
        
        print("\n5.4 基于因子的权重分配")
        print("-" * 40)
        
        # 基于PB比率的权重分配（低PB高权重）
        pb_cols = [col for col in portfolio_data.columns if 'pb' in col]
        latest_pb = portfolio_data[pb_cols].iloc[-1]
        
        # 计算权重（PB越低权重越高）
        pb_weights = 1 / latest_pb
        pb_weights = pb_weights / pb_weights.sum()
        
        print("✓ 基于PB的权重分配:")
        for i, (pb_col, price_col) in enumerate(zip(pb_cols, price_cols)):
            stock_code = price_col.split('_')[-1]
            print(f"  {stock_code}: 权重={pb_weights.iloc[i]:.2%}, PB={latest_pb.iloc[i]:.2f}")
        
        # 计算基于PB权重的组合收益
        pb_portfolio_returns = (returns_data[price_cols] * pb_weights.values).sum(axis=1)
        pb_annual_return = pb_portfolio_returns.mean() * 252
        pb_annual_volatility = pb_portfolio_returns.std() * np.sqrt(252)
        pb_sharpe_ratio = pb_annual_return / pb_annual_volatility if pb_annual_volatility > 0 else 0
        
        print(f"\n✓ PB权重组合表现:")
        print(f"  年化收益率: {pb_annual_return:.2%}")
        print(f"  年化波动率: {pb_annual_volatility:.2%}")
        print(f"  夏普比率: {pb_sharpe_ratio:.3f}")
        
        print("\n5.5 组合表现对比")
        print("-" * 40)
        
        comparison_data = pd.DataFrame({
            '等权重组合': [annual_return, annual_volatility, sharpe_ratio],
            'PB权重组合': [pb_annual_return, pb_annual_volatility, pb_sharpe_ratio]
        }, index=['年化收益率', '年化波动率', '夏普比率'])
        
        print("✓ 组合表现对比:")
        print(comparison_data.round(4))

def demonstrate_factor_timing():
    """演示因子择时功能"""
    print("\n" + "=" * 60)
    print("6. 因子择时功能演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n6.1 获取择时分析数据")
        print("-" * 40)
        
        # 获取技术指标数据用于择时
        timing_data = db.query_data_multifactor(
            table_names=["multifactor_prices", "multifactor_rsi", "multifactor_macd"],
            stock_codes=["000001.SZ"],
            start_date="2023-01-01",
            end_date="2023-12-31"
        )
        
        if timing_data is None:
            print("✗ 无法获取择时数据")
            return
        
        print(f"✓ 获取择时数据: {timing_data.shape}")
        
        print("\n6.2 RSI择时策略")
        print("-" * 40)
        
        # 提取RSI数据
        rsi_col = [col for col in timing_data.columns if 'rsi' in col][0]
        price_col = [col for col in timing_data.columns if 'prices' in col][0]
        
        # RSI择时信号
        timing_data['rsi_signal'] = 0
        timing_data.loc[timing_data[rsi_col] < 30, 'rsi_signal'] = 1  # 买入信号
        timing_data.loc[timing_data[rsi_col] > 70, 'rsi_signal'] = -1  # 卖出信号
        
        # 统计信号
        buy_signals = (timing_data['rsi_signal'] == 1).sum()
        sell_signals = (timing_data['rsi_signal'] == -1).sum()
        
        print(f"✓ RSI择时信号统计:")
        print(f"  买入信号: {buy_signals} 次")
        print(f"  卖出信号: {sell_signals} 次")
        
        print("\n6.3 MACD择时策略")
        print("-" * 40)
        
        # 提取MACD数据
        macd_col = [col for col in timing_data.columns if 'macd' in col][0]
        
        # MACD择时信号（简化版：正值买入，负值卖出）
        timing_data['macd_signal'] = 0
        timing_data.loc[timing_data[macd_col] > 0, 'macd_signal'] = 1
        timing_data.loc[timing_data[macd_col] < 0, 'macd_signal'] = -1
        
        # 统计MACD信号
        macd_buy_days = (timing_data['macd_signal'] == 1).sum()
        macd_sell_days = (timing_data['macd_signal'] == -1).sum()
        
        print(f"✓ MACD择时信号统计:")
        print(f"  看多天数: {macd_buy_days} 天")
        print(f"  看空天数: {macd_sell_days} 天")
        
        print("\n6.4 综合择时策略")
        print("-" * 40)
        
        # 综合RSI和MACD信号
        timing_data['combined_signal'] = timing_data['rsi_signal'] + timing_data['macd_signal']
        
        # 强买入：RSI超卖且MACD看多
        strong_buy = (timing_data['combined_signal'] == 2).sum()
        # 强卖出：RSI超买且MACD看空
        strong_sell = (timing_data['combined_signal'] == -2).sum()
        
        print(f"✓ 综合择时信号统计:")
        print(f"  强买入信号: {strong_buy} 次")
        print(f"  强卖出信号: {strong_sell} 次")
        
        # 显示最近的信号
        recent_signals = timing_data[['datetime', rsi_col, macd_col, 'rsi_signal', 'macd_signal', 'combined_signal']].tail(10)
        print("\n✓ 最近10天的择时信号:")
        print(recent_signals.round(3))

def demonstrate_performance_analysis():
    """演示绩效分析功能"""
    print("\n" + "=" * 60)
    print("7. 绩效分析功能演示")
    print("=" * 60)
    
    with AdvancedPostgreSQLManager() as db:
        print("\n7.1 获取绩效分析数据")
        print("-" * 40)
        
        # 获取多只股票的价格数据
        performance_data = db.query_data_multifactor(
            table_names=["multifactor_prices"],
            start_date="2023-01-01",
            end_date="2023-12-31"
        )
        
        if performance_data is None:
            print("✗ 无法获取绩效数据")
            return
        
        print(f"✓ 获取绩效数据: {performance_data.shape}")
        
        print("\n7.2 计算个股绩效指标")
        print("-" * 40)
        
        # 提取价格列
        price_cols = [col for col in performance_data.columns if col != 'datetime']
        
        # 计算收益率
        returns_data = performance_data[price_cols].pct_change().dropna()
        
        # 计算绩效指标
        performance_metrics = {}
        
        for col in price_cols:
            stock_code = col.split('_')[-1]
            returns = returns_data[col]
            
            # 基本统计
            total_return = (performance_data[col].iloc[-1] / performance_data[col].iloc[0] - 1)
            annual_return = returns.mean() * 252
            annual_volatility = returns.std() * np.sqrt(252)
            sharpe_ratio = annual_return / annual_volatility if annual_volatility > 0 else 0
            
            # 最大回撤
            cumulative_returns = (1 + returns).cumprod()
            rolling_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - rolling_max) / rolling_max
            max_drawdown = drawdown.min()
            
            performance_metrics[stock_code] = {
                '总收益率': total_return,
                '年化收益率': annual_return,
                '年化波动率': annual_volatility,
                '夏普比率': sharpe_ratio,
                '最大回撤': max_drawdown
            }
        
        # 显示绩效指标
        performance_df = pd.DataFrame(performance_metrics).T
        print("✓ 个股绩效指标:")
        print(performance_df.round(4))
        
        print("\n7.3 绩效排名分析")
        print("-" * 40)
        
        # 按不同指标排名
        rankings = {
            '收益率排名': performance_df['总收益率'].rank(ascending=False),
            '夏普比率排名': performance_df['夏普比率'].rank(ascending=False),
            '波动率排名': performance_df['年化波动率'].rank(ascending=True),  # 波动率越低越好
            '回撤排名': performance_df['最大回撤'].rank(ascending=False)  # 回撤越小（越接近0）越好
        }
        
        ranking_df = pd.DataFrame(rankings)
        print("✓ 绩效排名（1为最佳）:")
        print(ranking_df.astype(int))
        
        print("\n7.4 风险调整后收益分析")
        print("-" * 40)
        
        # 计算风险调整后的收益指标
        risk_adjusted_metrics = pd.DataFrame({
            '收益风险比': performance_df['年化收益率'] / performance_df['年化波动率'],
            '回撤调整收益': performance_df['年化收益率'] / abs(performance_df['最大回撤']),
            '综合评分': (performance_df['夏普比率'] * 0.4 + 
                       (performance_df['年化收益率'] / performance_df['年化波动率']) * 0.3 +
                       (performance_df['年化收益率'] / abs(performance_df['最大回撤'])) * 0.3)
        })
        
        print("✓ 风险调整后收益指标:")
        print(risk_adjusted_metrics.round(4))
        
        # 找出综合表现最佳的股票
        best_stock = risk_adjusted_metrics['综合评分'].idxmax()
        print(f"\n🏆 综合表现最佳股票: {best_stock}")
        print(f"   综合评分: {risk_adjusted_metrics.loc[best_stock, '综合评分']:.4f}")

def cleanup_multifactor_data():
    """清理多因子分析数据"""
    print("\n" + "=" * 60)
    print("8. 清理多因子分析数据")
    print("=" * 60)
    
    with PostgreSQLManager() as db:
        tables = db.list_tables()
        multifactor_tables = [t for t in tables if t.startswith('multifactor_')]
        
        print(f"发现 {len(multifactor_tables)} 个多因子分析表")
        
        for table in multifactor_tables:
            success = db.drop_table(table)
            if success:
                print(f"✓ 删除表: {table}")
            else:
                print(f"✗ 删除失败: {table}")

def main():
    """主函数"""
    print("PostgreSQL数据管理系统 - 多因子分析示例")
    print("=" * 80)
    
    try:
        # 1. 设置多因子数据
        stock_codes, tables = setup_multifactor_data()
        
        # 2. 基础多因子查询
        demonstrate_basic_multifactor_query()
        
        # 3. 因子相关性分析
        demonstrate_factor_correlation_analysis()
        
        # 4. 因子筛选
        demonstrate_factor_screening()
        
        # 5. 投资组合构建
        demonstrate_portfolio_construction()
        
        # 6. 因子择时
        demonstrate_factor_timing()
        
        # 7. 绩效分析
        demonstrate_performance_analysis()
        
        # 8. 清理数据
        cleanup_multifactor_data()
        
        print("\n" + "=" * 80)
        print("✅ 多因子分析示例演示完成！")
        print("=" * 80)
        
        print("\n📋 功能总结:")
        print("1. ✓ 多因子数据管理：价格、基本面、技术指标等")
        print("2. ✓ 跨表联合查询：一次性获取多个因子数据")
        print("3. ✓ 因子相关性分析：识别因子间的关联关系")
        print("4. ✓ 因子筛选策略：基于多重条件筛选股票")
        print("5. ✓ 投资组合构建：等权重和因子权重分配")
        print("6. ✓ 因子择时策略：基于技术指标的买卖信号")
        print("7. ✓ 绩效分析评估：风险调整后的收益指标")
        
    except Exception as e:
        print(f"\n❌ 多因子分析演示过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()