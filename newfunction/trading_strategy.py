"""
T+1 尾盘自适应选股策略 (防御潜伏版 v2.0)
- 引入 StrategyContext 管理市场状态和参数
- 支持熊市(0)、震荡市(1)、牛市(2)三种模式
- 当前默认：震荡市模式，防御潜伏型策略

核心变更 (Sideways Regime Physics):
- 拒绝追高：涨幅区间从 3.0%~8.5% 下调至 0.5%~4.2%
- 成本控制：严格限制VWAP偏离度 -0.002 < bias < 0.015
- 拒绝过热：换手率上限 8.0%
- 主力必须在场：保持大单异动 ats_ratio > 1.3
"""
import pandas as pd
import sqlite3
import numpy as np
import sys
import time
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB
from config.token_manager import get_tushare_token
from newfunction.strategy_context import StrategyContext, default_context
import tushare as ts


def get_history_snapshot(target_date: str, pro) -> pd.DataFrame:
    """获取历史某一天的模拟实时快照 (增加盘口模拟)"""
    df = pro.daily(trade_date=target_date)
    if df is None or df.empty:
        return pd.DataFrame()
    
    # 模拟实时接口字段
    df['amount'] = df['amount'] * 1000  # 转换为元
    df = df.rename(columns={'vol': 'vol_lots'})
    
    # 在历史回测模式下模拟缺失字段 (num, bid_vol1, ask_vol1)
    if 'num' not in df.columns:
        df['num'] = (df['vol_lots'] / 5).clip(lower=1).astype(int)
    if 'bid_vol1' not in df.columns:
        df['bid_vol1'] = 1.0
        df['ask_vol1'] = 1.0
    
    # 移除冲突列
    if 'pre_close' in df.columns:
        df = df.drop(columns=['pre_close'])
    return df


def run_funnel_strategy(
    target_date: str = "20251219",
    t1_date: str = "20251218",
    context: Optional[StrategyContext] = None,
    return_df: bool = False
) -> Optional[pd.DataFrame]:
    """
    运行漏斗策略
    
    Args:
        target_date: 目标交易日 (T日)
        t1_date: 基准日 (T-1日)
        context: 策略上下文，默认使用 default_context (震荡市)
        return_df: 是否返回DataFrame而不是打印输出
    
    Returns:
        如果 return_df=True，返回筛选后的DataFrame；否则返回None
    """
    # 使用默认上下文（震荡市）
    ctx = context or default_context
    
    pro = ts.pro_api(get_tushare_token())
    
    # 1. 加载基准数据 (T-1)
    conn = sqlite3.connect(CACHE_DB)
    df_history = pd.read_sql(f"SELECT * FROM t1_data WHERE trade_date = '{t1_date}'", conn)
    conn.close()
    
    if df_history.empty:
        print(f"[ERROR] 未找到 {t1_date} 的基准数据，请先采集。")
        return None if return_df else None

    # 2. 获取目标日快照 (T)
    df_rt = get_history_snapshot(target_date, pro)
    if df_rt.empty:
        return None if return_df else None

    # 3. 特征工程 (向量化)
    df = df_rt.merge(df_history, on='ts_code', how='inner')
    
    # 单位统一
    df['vol_shares'] = df['vol_lots'] * 100
    df['vwap'] = df['amount'] / (df['vol_shares'] + 1e-9)
    df['float_mv_cny'] = df['total_mv'] * 10000
    
    # 计算核心指标
    df['turnover_rate'] = (df['vol_shares'] / (df['float_share'] * 10000 + 1e-9)) * 100
    df['bias'] = (df['close'] - df['vwap']) / (df['vwap'] + 1e-9)
    df['pct_chg'] = (df['close'] - df['pre_close']) / (df['pre_close'] + 1e-9) * 100
    df['ats'] = df['amount'] / (df['num'] + 1e-9)
    df['ats_ratio'] = df['ats'] / (df['pre_ats'] + 1e-9)
    df['efficiency'] = df['pct_chg'] / (df['turnover_rate'] + 0.001)
    df['high_close_dist'] = (df['high'] - df['close']) / (df['close'] + 1e-9)
    df['vol_ratio'] = df['vol_lots'] / (df['pre_vol'] + 1e-9)
    
    # 新增：日内高点确认指标 (收盘价接近最高价的比例)
    df['close_to_high'] = df['close'] / (df['high'] + 1e-9)
    
    # 4. 漏斗初步筛选 (Phase 2 Hard Filters) - 使用 StrategyContext 参数
    print(f"\n[STEP 1] 正在分析 {target_date} 市场数据 | 模式: {ctx.regime_name}")
    print(f"初始样本: {len(df)}")
    
    # === 核心变更：使用 context 中的参数 ===
    mask_funnel = (
        # 涨幅区间 (震荡市：0.5% ~ 4.2%，拒绝追高)
        (df['pct_chg'] >= ctx.get('pct_chg_min')) & 
        (df['pct_chg'] <= ctx.get('pct_chg_max')) &
        
        # VWAP偏离度 (震荡市：-0.002 ~ 0.015，成本控制)
        (df['bias'] >= ctx.get('bias_min')) &
        (df['bias'] <= ctx.get('bias_max')) &
        
        # 尾盘强度 (收盘价接近最高价)
        (df['high_close_dist'] < ctx.get('high_close_dist_max')) &
        
        # 日内高点确认 (收盘价必须接近日最高价的98%以上)
        (df['close_to_high'] >= ctx.get('close_to_high_ratio')) &
        
        # 拒绝虚假单
        (df['bid_vol1'] / (df['ask_vol1'] + 1e-9) < 10) &
        
        # 大单异动 (主力必须在场)
        (df['ats_ratio'] > ctx.get('ats_ratio_min'))
    )
    df = df[mask_funnel].copy()
    print(f"初步漏斗通过: {len(df)}")

    if df.empty:
        print("[NONE] 无标的通过初步漏斗。")
        return pd.DataFrame() if return_df else None

    # 5. 自适应风控 (Phase 2.5 Adaptive Safety)
    # 使用 context 中的市值分层换手率阈值
    conditions = [
        (df['float_mv_cny'] > 200 * 1e8),
        (df['float_mv_cny'] > 50 * 1e8) & (df['float_mv_cny'] <= 200 * 1e8),
        (df['float_mv_cny'] <= 50 * 1e8)
    ]
    choices = [
        ctx.get('turnover_large_cap'),
        ctx.get('turnover_mid_cap'),
        ctx.get('turnover_small_cap')
    ]
    df['max_turnover_limit'] = np.select(conditions, choices, default=ctx.get('turnover_mid_cap'))
    
    # 过滤：换手率超标 或 爆发过热
    mask_adaptive = (
        (df['turnover_rate'] <= df['max_turnover_limit']) &
        (df['turnover_rate'] <= ctx.get('turnover_max')) &  # 全局换手率上限
        ~((df['turnover_rate'] > ctx.get('turnover_danger')) & 
          (df['vol_ratio'] > ctx.get('vol_ratio_danger')))
    )
    df = df[mask_adaptive].copy()
    print(f"自适应风控通过: {len(df)}")

    # 6. 筹码 Tier 系统 - 使用 context 参数
    df['Chip_Tier'] = 0
    df.loc[df['winner_rate'] > ctx.get('chip_tier1_winner_rate'), 'Chip_Tier'] = 1
    df.loc[
        (df['Chip_Tier'] == 0) & 
        (df['winner_rate'] > ctx.get('chip_tier2_winner_rate')) & 
        (df['cost_concentration'] < ctx.get('chip_tier2_concentration')), 
        'Chip_Tier'
    ] = 2
    
    df = df[df['Chip_Tier'] > 0].copy()
    print(f"筹码分级通过: {len(df)}")

    if df.empty:
        return pd.DataFrame() if return_df else None

    # 7. 量化评分系统 (Final_Score) - 使用 context 权重
    df['norm_eff'] = df['efficiency'].rank(pct=True)
    df['norm_win'] = df['winner_rate'].rank(pct=True)
    df['norm_ats'] = df['ats_ratio'].rank(pct=True)
    
    df['Final_Score'] = (
        df['norm_eff'] * ctx.get('score_weight_efficiency') +
        df['norm_win'] * ctx.get('score_weight_winner_rate') +
        df['norm_ats'] * ctx.get('score_weight_ats_ratio')
    )
    
    # 8. 最终选股 (优先 Tier 1)
    df = df.sort_values(['Chip_Tier', 'Final_Score'], ascending=[True, False])
    
    tier1_count = len(df[df['Chip_Tier'] == 1])
    top_n = ctx.get('top_n_output')
    if tier1_count >= 5:
        output_df = df[df['Chip_Tier'] == 1].head(top_n)
    else:
        output_df = df.head(top_n)

    # 9. 格式化输出
    if not output_df.empty and not return_df:
        output_df = output_df.rename(columns={'turnover_rate': 'turnover'})
        cols = ['ts_code', 'pct_chg', 'turnover', 'bias', 'ats_ratio', 'winner_rate', 'Chip_Tier', 'Final_Score']
        print(f"\n[FINAL SELECTION] 目标日: {target_date} Top {len(output_df)} 候选股")
        print(output_df[cols].to_string(index=False, float_format=lambda x: "{:.3f}".format(x)))
        
        print(f"\n[第一性原理注释] 模式: {ctx.regime_name}")
        if ctx.regime == 1:
            print("- 拒绝追高: 涨幅限制在 0.5%~4.2%，吃鱼身不吃鱼尾。")
            print("- 成本控制: VWAP偏离度 -0.2%~1.5%，允许微弱负溢价（主力打压吸筹）。")
            print("- 拒绝过热: 换手率上限 8%，高换手=高分歧=高风险。")
            print("- 主力在场: 大单异动 > 1.3，确保有主力资金流入。")
        elif ctx.regime == 2:
            print("- 追涨模式: 涨幅 3%~8.5%，抓住动量趋势。")
            print("- 大单异动 > 1.2，跟随主力资金。")
        else:
            print("- 极度保守: 涨幅 0%~2.5%，仅潜伏超跌反弹。")
    
    if return_df:
        return df.head(ctx.get('top_n_candidates'))
    
    return None


if __name__ == '__main__':
    # 使用震荡市模式运行
    print(default_context.describe())
    run_funnel_strategy()
