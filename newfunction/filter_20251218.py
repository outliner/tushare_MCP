"""
震荡行情下A股20251218日股票筛选脚本
- 使用震荡市(Sideways)策略模式
- T日: 20251218
- T-1日: 20251217 (需要基准数据)
"""
import sys
from pathlib import Path
import sqlite3
import pandas as pd
import time

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB
from config.token_manager import get_tushare_token
from newfunction.strategy_context import StrategyContext
from newfunction.batch_data_collector import BatchDataCollector
import tushare as ts


def check_t1_data_exists(t1_date: str) -> bool:
    """检查T-1日数据是否已存在"""
    conn = sqlite3.connect(CACHE_DB)
    try:
        query = f"SELECT COUNT(*) FROM t1_data WHERE trade_date = '{t1_date}'"
        count = pd.read_sql(query, conn).iloc[0, 0]
        return count > 0
    except Exception as e:
        print(f"[WARNING] 检查数据时出错: {e}")
        return False
    finally:
        conn.close()


def collect_t1_data_if_needed(t1_date: str):
    """如果T-1数据不存在，则采集"""
    if check_t1_data_exists(t1_date):
        conn = sqlite3.connect(CACHE_DB)
        count = pd.read_sql(f"SELECT COUNT(*) FROM t1_data WHERE trade_date = '{t1_date}'", conn).iloc[0, 0]
        conn.close()
        print(f"[INFO] T-1日({t1_date})数据已存在，共 {count} 条记录")
        return True
    
    print(f"[INFO] T-1日({t1_date})数据不存在，开始采集...")
    collector = BatchDataCollector()
    collector.collect_for_date(t1_date)
    collector.print_final_report()
    return True


def run_sideways_filter(target_date: str, t1_date: str):
    """
    运行震荡市策略筛选
    
    Args:
        target_date: 目标交易日 (T日)
        t1_date: 基准日 (T-1日)
    """
    # 创建震荡市上下文
    ctx = StrategyContext(regime=1)  # 1 = 震荡市
    
    print(ctx.describe())
    
    pro = ts.pro_api(get_tushare_token())
    
    # 1. 加载基准数据 (T-1)
    conn = sqlite3.connect(CACHE_DB)
    df_history = pd.read_sql(f"SELECT * FROM t1_data WHERE trade_date = '{t1_date}'", conn)
    conn.close()
    
    if df_history.empty:
        print(f"[ERROR] 未找到 {t1_date} 的基准数据")
        return None
    
    print(f"[INFO] 加载T-1基准数据: {len(df_history)} 条")
    
    # 2. 获取T日行情数据
    print(f"[INFO] 获取T日({target_date})行情数据...")
    df_daily = pro.daily(trade_date=target_date)
    
    if df_daily is None or df_daily.empty:
        print(f"[ERROR] {target_date} 无行情数据")
        return None
    
    print(f"[INFO] T日行情数据: {len(df_daily)} 条")
    
    # 模拟实时接口字段
    df_rt = df_daily.copy()
    df_rt['amount'] = df_rt['amount'] * 1000  # 转换为元
    df_rt = df_rt.rename(columns={'vol': 'vol_lots'})
    
    # 模拟缺失字段
    if 'num' not in df_rt.columns:
        df_rt['num'] = (df_rt['vol_lots'] / 5).clip(lower=1).astype(int)
    if 'bid_vol1' not in df_rt.columns:
        df_rt['bid_vol1'] = 1.0
        df_rt['ask_vol1'] = 1.0
    
    # 移除冲突列
    if 'pre_close' in df_rt.columns:
        df_rt = df_rt.drop(columns=['pre_close'])
    
    # 3. 合并数据
    df = df_rt.merge(df_history, on='ts_code', how='inner')
    print(f"[INFO] 合并后数据: {len(df)} 条")
    
    # 4. 特征工程
    df['vol_shares'] = df['vol_lots'] * 100
    df['vwap'] = df['amount'] / (df['vol_shares'] + 1e-9)
    df['float_mv_cny'] = df['total_mv'] * 10000
    
    df['turnover_rate'] = (df['vol_shares'] / (df['float_share'] * 10000 + 1e-9)) * 100
    df['bias'] = (df['close'] - df['vwap']) / (df['vwap'] + 1e-9)
    df['pct_chg'] = (df['close'] - df['pre_close']) / (df['pre_close'] + 1e-9) * 100
    df['ats'] = df['amount'] / (df['num'] + 1e-9)
    df['ats_ratio'] = df['ats'] / (df['pre_ats'] + 1e-9)
    df['efficiency'] = df['pct_chg'] / (df['turnover_rate'] + 0.001)
    df['high_close_dist'] = (df['high'] - df['close']) / (df['close'] + 1e-9)
    df['vol_ratio'] = df['vol_lots'] / (df['pre_vol'] + 1e-9)
    df['close_to_high'] = df['close'] / (df['high'] + 1e-9)
    
    # 5. 震荡市漏斗筛选
    print(f"\n{'='*60}")
    print(f"[STEP 1] 正在分析 {target_date} 市场数据 | 模式: {ctx.regime_name}")
    print(f"{'='*60}")
    print(f"初始样本: {len(df)}")
    
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
    df_filtered = df[mask_funnel].copy()
    print(f"初步漏斗通过: {len(df_filtered)}")
    
    if df_filtered.empty:
        print("[NONE] 无标的通过初步漏斗。")
        return None
    
    # 6. 自适应风控
    import numpy as np
    conditions = [
        (df_filtered['float_mv_cny'] > 200 * 1e8),
        (df_filtered['float_mv_cny'] > 50 * 1e8) & (df_filtered['float_mv_cny'] <= 200 * 1e8),
        (df_filtered['float_mv_cny'] <= 50 * 1e8)
    ]
    choices = [
        ctx.get('turnover_large_cap'),
        ctx.get('turnover_mid_cap'),
        ctx.get('turnover_small_cap')
    ]
    df_filtered['max_turnover_limit'] = np.select(conditions, choices, default=ctx.get('turnover_mid_cap'))
    
    mask_adaptive = (
        (df_filtered['turnover_rate'] <= df_filtered['max_turnover_limit']) &
        (df_filtered['turnover_rate'] <= ctx.get('turnover_max')) &
        ~((df_filtered['turnover_rate'] > ctx.get('turnover_danger')) & 
          (df_filtered['vol_ratio'] > ctx.get('vol_ratio_danger')))
    )
    df_filtered = df_filtered[mask_adaptive].copy()
    print(f"自适应风控通过: {len(df_filtered)}")
    
    # 7. 筹码 Tier 系统
    df_filtered['Chip_Tier'] = 0
    df_filtered.loc[df_filtered['winner_rate'] > ctx.get('chip_tier1_winner_rate'), 'Chip_Tier'] = 1
    df_filtered.loc[
        (df_filtered['Chip_Tier'] == 0) & 
        (df_filtered['winner_rate'] > ctx.get('chip_tier2_winner_rate')) & 
        (df_filtered['cost_concentration'] < ctx.get('chip_tier2_concentration')), 
        'Chip_Tier'
    ] = 2
    
    df_filtered = df_filtered[df_filtered['Chip_Tier'] > 0].copy()
    print(f"筹码分级通过: {len(df_filtered)}")
    
    if df_filtered.empty:
        print("[NONE] 无标的通过筹码分级筛选。")
        return None
    
    # 8. 量化评分系统
    df_filtered['norm_eff'] = df_filtered['efficiency'].rank(pct=True)
    df_filtered['norm_win'] = df_filtered['winner_rate'].rank(pct=True)
    df_filtered['norm_ats'] = df_filtered['ats_ratio'].rank(pct=True)
    
    df_filtered['Final_Score'] = (
        df_filtered['norm_eff'] * ctx.get('score_weight_efficiency') +
        df_filtered['norm_win'] * ctx.get('score_weight_winner_rate') +
        df_filtered['norm_ats'] * ctx.get('score_weight_ats_ratio')
    )
    
    # 9. 最终选股 (优先 Tier 1)
    df_filtered = df_filtered.sort_values(['Chip_Tier', 'Final_Score'], ascending=[True, False])
    
    tier1_count = len(df_filtered[df_filtered['Chip_Tier'] == 1])
    top_n = ctx.get('top_n_output')
    if tier1_count >= 5:
        output_df = df_filtered[df_filtered['Chip_Tier'] == 1].head(top_n)
    else:
        output_df = df_filtered.head(top_n)
    
    # 10. 格式化输出
    output_df = output_df.rename(columns={'turnover_rate': 'turnover'})
    
    print(f"\n{'='*60}")
    print(f"[FINAL SELECTION] 目标日: {target_date} Top {len(output_df)} 候选股")
    print(f"{'='*60}")
    
    cols = ['ts_code', 'pct_chg', 'turnover', 'bias', 'ats_ratio', 'winner_rate', 'Chip_Tier', 'Final_Score']
    print(output_df[cols].to_string(index=False, float_format=lambda x: "{:.3f}".format(x)))
    
    print(f"\n[第一性原理注释] 模式: {ctx.regime_name}")
    print("- 拒绝追高: 涨幅限制在 0.5%~4.2%，吃鱼身不吃鱼尾。")
    print("- 成本控制: VWAP偏离度 -0.2%~1.5%，允许微弱负溢价（主力打压吸筹）。")
    print("- 拒绝过热: 换手率上限 8%，高换手=高分歧=高风险。")
    print("- 主力在场: 大单异动 > 1.3，确保有主力资金流入。")
    
    # 返回完整候选列表供后续15分钟线筛选
    return df_filtered.head(ctx.get('top_n_candidates'))


def main():
    """主函数"""
    target_date = "20251218"  # T日
    t1_date = "20251217"      # T-1日
    
    print(f"\n{'#'*60}")
    print(f"# 震荡行情下A股股票筛选")
    print(f"# T日: {target_date}")
    print(f"# T-1日: {t1_date}")
    print(f"{'#'*60}")
    
    # Step 1: 确保T-1数据已采集
    collect_t1_data_if_needed(t1_date)
    
    # Step 2: 运行震荡市策略筛选
    candidates = run_sideways_filter(target_date, t1_date)
    
    if candidates is not None and len(candidates) > 0:
        print(f"\n[SUMMARY] 共筛选出 {len(candidates)} 只候选股")
        
        # 保存结果到文件
        output_file = project_root / "doc" / f"filter_result_{target_date}.csv"
        candidates.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"[INFO] 结果已保存至: {output_file}")
    else:
        print("\n[SUMMARY] 无候选股通过筛选")


if __name__ == '__main__':
    main()

