"""
上周策略回测收益统计脚本
分析 20251215 - 20251218 的选股次日表现
"""
import pandas as pd
import sqlite3
import sys
from pathlib import Path
import tushare as ts

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token
from newfunction.trading_strategy import run_funnel_strategy, get_history_snapshot

def get_next_trading_day(current_date, pro):
    """获取下一个交易日"""
    cal = pro.trade_cal(exchange='', start_date=current_date, is_open='1')
    if len(cal) > 1:
        return cal.iloc[1]['cal_date']
    return None

def backtest_last_week():
    pro = ts.pro_api(get_tushare_token())
    test_dates = [
        ('20251215', '20251212'),
        ('20251216', '20251215'),
        ('20251217', '20251216'),
        ('20251218', '20251217'),
        ('20251219', '20251218')
    ]
    
    all_results = []
    
    print("\n" + "="*70)
    print(f"{'交易日(T)':<10} | {'股票代码':<10} | {'Chip':<4} | {'Score':<6} | {'次日涨幅(T+1)':<10}")
    print("-" * 70)
    
    for target_date, t1_date in test_dates:
        # 1. 模拟运行策略获取选股 (修改 strategy 返回结果)
        # 这里为了演示，我们直接在脚本里复用逻辑
        # 我们需要一个能返回 dataframe 的 strategy 函数，我先临时在这里重写核心逻辑
        import sqlite3
        from config.settings import CACHE_DB
        import numpy as np

        # --- 策略核心逻辑简编 ---
        conn = sqlite3.connect(CACHE_DB)
        df_history = pd.read_sql(f"SELECT * FROM t1_data WHERE trade_date = '{t1_date}'", conn)
        conn.close()
        
        df_rt = get_history_snapshot(target_date, pro)
        if df_rt.empty or df_history.empty: continue
        
        df = df_rt.merge(df_history, on='ts_code', how='inner')
        df['vol_shares'] = df['vol_lots'] * 100
        df['vwap'] = df['amount'] / (df['vol_shares'] + 1e-9)
        df['float_mv_cny'] = df['total_mv'] * 10000
        df['turnover_rate'] = (df['vol_shares'] / (df['float_share'] * 10000 + 1e-9)) * 100
        df['bias'] = (df['close'] - df['vwap']) / (df['vwap'] + 1e-9)
        df['pct_chg_t'] = (df['close'] - df['pre_close']) / (df['pre_close'] + 1e-9) * 100
        df['ats'] = df['amount'] / (df['num'] + 1e-9)
        df['ats_ratio'] = df['ats'] / (df['pre_ats'] + 1e-9)
        df['efficiency'] = df['pct_chg_t'] / (df['turnover_rate'] + 0.001)
        df['high_close_dist'] = (df['high'] - df['close']) / (df['close'] + 1e-9)
        df['vol_ratio'] = df['vol_lots'] / (df['pre_vol'] + 1e-9)
        
        mask = (
            (df['pct_chg_t'] >= 3.0) & (df['pct_chg_t'] <= 8.5) &
            (df['bias'] > 0.005) & (df['high_close_dist'] < 0.008) &
            (df['ats_ratio'] > 1.2)
        )
        df = df[mask].copy()
        
        conditions = [(df['float_mv_cny'] > 200e8), (df['float_mv_cny'] > 50e8), (df['float_mv_cny'] <= 50e8)]
        df['max_turnover_limit'] = np.select(conditions, [8.0, 15.0, 20.0], default=15.0)
        df = df[(df['turnover_rate'] <= df['max_turnover_limit']) & ~((df['turnover_rate'] > 10.0) & (df['vol_ratio'] > 3.0))]
        
        df['Chip_Tier'] = 0
        df.loc[df['winner_rate'] > 80, 'Chip_Tier'] = 1
        df.loc[(df['Chip_Tier'] == 0) & (df['winner_rate'] > 60) & (df['cost_concentration'] < 0.12), 'Chip_Tier'] = 2
        df = df[df['Chip_Tier'] > 0].copy()
        
        if df.empty: continue
        
        df['Final_Score'] = (df['efficiency'].rank(pct=True) * 0.5) + (df['winner_rate'].rank(pct=True) * 0.3) + (df['ats_ratio'].rank(pct=True) * 0.2)
        df = df.sort_values(['Chip_Tier', 'Final_Score'], ascending=[True, False]).head(16)
        # --- 策略核心逻辑结束 ---

        # 2. 获取次日涨幅 (T+1)
        next_day = get_next_trading_day(target_date, pro)
        if next_day:
            df_next = pro.daily(trade_date=next_day)
            if df_next is not None and not df_next.empty:
                df = df.merge(df_next[['ts_code', 'pct_chg']], on='ts_code', how='left', suffixes=('', '_next'))
                
                for _, row in df.iterrows():
                    p_next = row['pct_chg_next']
                    status = f"{p_next:>+7.2f}%" if not pd.isna(p_next) else "N/A (Pending)"
                    print(f"{target_date:<10} | {row['ts_code']:<10} | T{int(row['Chip_Tier']):<3} | {row['Final_Score']:<6.2f} | {status}")
                    if not pd.isna(p_next):
                        all_results.append(p_next)
            else:
                for _, row in df.iterrows():
                    print(f"{target_date:<10} | {row['ts_code']:<10} | T{int(row['Chip_Tier']):<3} | {row['Final_Score']:<6.2f} | N/A (No Data)")
        else:
            for _, row in df.iterrows():
                print(f"{target_date:<10} | {row['ts_code']:<10} | T{int(row['Chip_Tier']):<3} | {row['Final_Score']:<6.2f} | N/A (Weekend)")

    if all_results:
        avg_ret = sum(all_results) / len(all_results)
        win_rate = len([x for x in all_results if x > 0]) / len(all_results)
        print("="*70)
        print(f"📊 回测汇总 (20251215 - 20251219)")
        print(f"总选股数: {len(all_results)}")
        print(f"平均次日收益: {avg_ret:.2f}%")
        print(f"次日胜率: {win_rate:.1%}")
        print("="*70)

if __name__ == '__main__':
    backtest_last_week()

