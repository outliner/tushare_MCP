"""
15分钟线二次筛选 (防御潜伏版 v2.0)
- 支持 StrategyContext 管理市场状态
- 新增震荡市专属防御逻辑

新增检查规则 (Sideways Regime):
- Rule E: 拒绝死鱼 (Activity Check) - 尾盘必须有成交量，不能完全缩量
- Rule F: 日内高点确认 - 确保没有早盘冲高回落的形态
"""
import pandas as pd
import numpy as np
import sqlite3
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DIR, CACHE_DB
from newfunction.strategy_context import StrategyContext, default_context
import tushare as ts
from config.token_manager import get_tushare_token

MIN15_DB_PATH = CACHE_DIR / "min15_cache.db"


def check_15min_structure(
    df_15min: pd.DataFrame,
    daily_high: float = None,
    context: Optional[StrategyContext] = None
) -> tuple:
    """
    使用15分钟线进行尾盘体检
    
    Args:
        df_15min: 15分钟线数据，包含 'open', 'high', 'low', 'close', 'vol'
        daily_high: 当日最高价（用于日内高点确认）
        context: 策略上下文
    
    Returns:
        (is_pass, fail_reasons): 是否通过及失败原因列表
    """
    ctx = context or default_context
    fail_reasons = []
    
    # 截取当天最后的数据
    df_tail = df_15min.tail(5).copy()
    
    if len(df_tail) < 3:
        return False, ["数据不足"]

    # --- 1. 计算近似 VWAP ---
    df_tail['typical_price'] = (df_tail['high'] + df_tail['low'] + df_tail['close']) / 3
    cum_value = (df_tail['typical_price'] * df_tail['vol']).cumsum()
    cum_vol = df_tail['vol'].cumsum()
    df_tail['approx_vwap'] = cum_value / (cum_vol + 1e-9)
    
    current_close = df_tail.iloc[-1]['close']
    current_vwap = df_tail.iloc[-1]['approx_vwap']

    # --- 2. 核心逻辑检验 ---

    # Rule A: 均线支撑 (当前价 > 近似均价)
    is_above_vwap = current_close > current_vwap
    if not is_above_vwap:
        fail_reasons.append("A:低于VWAP")

    # Rule B: 趋势向上 (低点抬升)
    lows = df_tail['low'].values[-3:]
    is_trend_up = lows[-1] >= lows[0]
    if not is_trend_up:
        fail_reasons.append("B:低点下移")

    # Rule C: 拒绝避雷针 (检查上影线)
    df_tail['upper_shadow'] = (
        df_tail['high'] - df_tail[['open', 'close']].max(axis=1)
    ) / (df_tail['close'] + 1e-9)
    upper_shadow_max = ctx.get('upper_shadow_max', 0.01)
    has_bad_shadow = (df_tail['upper_shadow'] > upper_shadow_max).any()
    if has_bad_shadow:
        fail_reasons.append("C:上影线过长")

    # Rule D: 拒绝阴包阳 (势能检查)
    start_open = df_tail.iloc[-3]['open']
    is_positive_session = current_close > start_open
    if not is_positive_session:
        fail_reasons.append("D:下午阴跌")

    # === 新增：震荡市专属防御规则 ===
    
    # Rule E: 拒绝死鱼 (Activity Check)
    # 检查尾盘最后3根K线的成交量，不能完全缩量
    tail_3_vol = df_tail['vol'].values[-3:]
    total_vol = df_15min['vol'].sum()
    tail_vol_ratio = tail_3_vol.sum() / (total_vol + 1e-9)
    
    min_tail_vol_ratio = ctx.get('tail_vol_ratio_min', 0.6)
    # 计算尾盘成交量相对于全天均值的比例
    avg_vol_per_bar = total_vol / len(df_15min)
    tail_avg_vol = tail_3_vol.mean()
    vol_activity_ratio = tail_avg_vol / (avg_vol_per_bar + 1e-9)
    
    # 尾盘成交量不能低于全天平均的60%（表示有人抢尾盘）
    is_tail_active = vol_activity_ratio >= min_tail_vol_ratio
    if not is_tail_active:
        fail_reasons.append(f"E:尾盘缩量({vol_activity_ratio:.1%})")

    # Rule F: 日内高点确认 (防止早盘冲高回落)
    # 收盘价必须接近当日最高价的98%以上
    if daily_high is not None and daily_high > 0:
        close_to_high_ratio = ctx.get('close_to_high_ratio', 0.98)
        intraday_high_check = current_close / daily_high
        is_near_high = intraday_high_check >= close_to_high_ratio
        if not is_near_high:
            fail_reasons.append(f"F:远离日高点({intraday_high_check:.1%})")
    else:
        is_near_high = True  # 无日线高点数据时跳过此检查
    
    # === 综合判定 ===
    if ctx.regime == 1:  # 震荡市：更严格
        is_pass = (
            is_above_vwap and 
            is_trend_up and 
            not has_bad_shadow and 
            is_positive_session and
            is_tail_active and
            is_near_high
        )
    else:  # 牛市/熊市：使用原逻辑
        is_pass = (
            is_above_vwap and 
            is_trend_up and 
            not has_bad_shadow and 
            is_positive_session
        )
    
    return is_pass, fail_reasons


def get_min15_data(ts_code: str, target_date: str) -> pd.DataFrame:
    """从本地数据库获取指定股票在目标日期的 15 分钟线数据"""
    formatted_date = f"{target_date[:4]}/{target_date[4:6]}/{target_date[6:]}"
    db_code = ts_code.split('.')[1].lower() + ts_code.split('.')[0]
    
    conn = sqlite3.connect(MIN15_DB_PATH)
    query = f"""
        SELECT * FROM stock_min15 
        WHERE ts_code = '{db_code}' AND trade_time LIKE '{formatted_date}%' 
        ORDER BY trade_time ASC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def run_second_filter(
    target_date: str = "20251219",
    t1_date: str = "20251218",
    context: Optional[StrategyContext] = None
) -> List[Dict[str, Any]]:
    """
    运行二次筛选流程
    
    Args:
        target_date: 目标交易日
        t1_date: 基准日 (T-1)
        context: 策略上下文
    
    Returns:
        通过筛选的股票列表
    """
    ctx = context or default_context
    
    print(f"\n{'='*60}")
    print(f"开始二次筛选流程 | 目标日期: {target_date} | 模式: {ctx.regime_name}")
    print(f"{'='*60}")

    # 获取初步过滤结果
    from newfunction.trading_strategy import get_history_snapshot
    pro = ts.pro_api(get_tushare_token())
    
    conn = sqlite3.connect(CACHE_DB)
    df_history = pd.read_sql(f"SELECT * FROM t1_data WHERE trade_date = '{t1_date}'", conn)
    conn.close()
    
    if df_history.empty:
        print(f"[ERROR] 未找到 {t1_date} 的基准数据。")
        return []

    df_rt = get_history_snapshot(target_date, pro)
    if df_rt.empty:
        return []

    df = df_rt.merge(df_history, on='ts_code', how='inner')
    
    # 特征工程
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
    df['close_to_high'] = df['close'] / (df['high'] + 1e-9)
    
    # 使用 context 参数的漏斗筛选
    mask_funnel = (
        (df['pct_chg'] >= ctx.get('pct_chg_min')) &
        (df['pct_chg'] <= ctx.get('pct_chg_max')) &
        (df['bias'] >= ctx.get('bias_min')) &
        (df['bias'] <= ctx.get('bias_max')) &
        (df['high_close_dist'] < ctx.get('high_close_dist_max')) &
        (df['close_to_high'] >= ctx.get('close_to_high_ratio')) &
        (df['ats_ratio'] > ctx.get('ats_ratio_min'))
    )
    df = df[mask_funnel].copy()
    
    # 自适应风控
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
    df = df[
        (df['turnover_rate'] <= df['max_turnover_limit']) &
        (df['turnover_rate'] <= ctx.get('turnover_max'))
    ].copy()
    
    # 筹码 Tier
    df['Chip_Tier'] = 0
    df.loc[df['winner_rate'] > ctx.get('chip_tier1_winner_rate'), 'Chip_Tier'] = 1
    df.loc[
        (df['Chip_Tier'] == 0) & 
        (df['winner_rate'] > ctx.get('chip_tier2_winner_rate')) & 
        (df['cost_concentration'] < ctx.get('chip_tier2_concentration')), 
        'Chip_Tier'
    ] = 2
    df = df[df['Chip_Tier'] > 0].copy()
    
    # 评分
    if len(df) > 0:
        df['norm_eff'] = df['efficiency'].rank(pct=True)
        df['norm_win'] = df['winner_rate'].rank(pct=True)
        df['Final_Score'] = (
            df['norm_eff'] * ctx.get('score_weight_efficiency') +
            df['norm_win'] * ctx.get('score_weight_winner_rate')
        )
    
        candidates = df.sort_values(
            ['Chip_Tier', 'Final_Score'], 
            ascending=[True, False]
        ).head(ctx.get('top_n_candidates'))
    else:
        candidates = df
    
    print(f"初步过滤候选股数量: {len(candidates)}")
    
    # --- 二次筛选 (15分钟线体检) ---
    final_list = []
    
    for _, row in candidates.iterrows():
        ts_code = row['ts_code']
        daily_high = row['high']  # 获取当日最高价
        df_15min = get_min15_data(ts_code, target_date)
        
        if df_15min.empty:
            print(f"  [SKIP] {ts_code}: 无 15 分钟线数据")
            continue
        
        is_pass, fail_reasons = check_15min_structure(
            df_15min, 
            daily_high=daily_high,
            context=ctx
        )
        
        if is_pass:
            status = "PASS"
            reason_str = ""
        else:
            status = "FAIL"
            reason_str = f" ({', '.join(fail_reasons)})"
        
        print(f"  [{status}] {ts_code}{reason_str}")
        
        if is_pass:
            final_list.append(row.to_dict())
            
    # 输出最终名单
    if final_list:
        final_df = pd.DataFrame(final_list)
        print(f"\n[FINAL SELECTION] 二次筛选通过股票 ({len(final_df)} 只):")
        cols = ['ts_code', 'pct_chg', 'turnover_rate', 'bias', 'winner_rate', 'Chip_Tier', 'Final_Score']
        available_cols = [c for c in cols if c in final_df.columns]
        print(final_df[available_cols].to_string(index=False, float_format=lambda x: "{:.3f}".format(x)))
        
        if ctx.regime == 1:
            print(f"\n[震荡市防御检查说明]")
            print("  Rule A: 均线支撑 - 收盘价>VWAP")
            print("  Rule B: 趋势向上 - 低点抬升")
            print("  Rule C: 拒绝避雷针 - 上影线<0.8%")
            print("  Rule D: 拒绝阴包阳 - 下午正收益")
            print("  Rule E: 拒绝死鱼 - 尾盘成交量≥平均60%")
            print("  Rule F: 日内高点确认 - 收盘价≥最高价98%")
    else:
        print("\n[NONE] 无标的通过二次筛选。")
    
    return final_list


if __name__ == '__main__':
    print(default_context.describe())
    run_second_filter()
