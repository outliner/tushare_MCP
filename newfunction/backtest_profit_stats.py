"""
上周策略回测盈利统计脚本 (以T+1开盘价为卖点)
分析 20251215 - 20251218 的选股次日开盘表现
"""
import pandas as pd
import sqlite3
import sys
import numpy as np
from pathlib import Path
import tushare as ts

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token
from config.settings import CACHE_DB
from newfunction.trading_strategy import get_history_snapshot

def get_next_trading_day(current_date, pro):
    """获取下一个交易日"""
    cal = pro.trade_cal(exchange='SSE', start_date=current_date, is_open='1')
    # 强制按日期升序排列
    cal = cal.sort_values('cal_date')
    if len(cal) > 1:
        # 寻找第一个严格大于当前日期的交易日
        next_dates = cal[cal['cal_date'] > current_date]
        if not next_dates.empty:
            return next_dates.iloc[0]['cal_date']
    return None

def backtest_profit_stats():
    pro = ts.pro_api(get_tushare_token())
    test_dates = [
        ('20251215', '20251212'),
        ('20251216', '20251215'),
        ('20251217', '20251216'),
        ('20251218', '20251217')
    ]
    
    all_trades = []
    
    print("\n" + "="*85)
    print(f"{'买入日(T)':<10} | {'股票代码':<10} | {'买入价(T)':<8} | {'卖出价(T+1)':<10} | {'收益率':<8} | {'Chip'}")
    print("-" * 85)
    
    for target_date, t1_date in test_dates:
        conn = sqlite3.connect(CACHE_DB)
        df_history = pd.read_sql(f"SELECT * FROM t1_data WHERE trade_date = '{t1_date}'", conn)
        conn.close()
        
        df_rt = get_history_snapshot(target_date, pro)
        if df_rt.empty or df_history.empty:
            continue
            
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
        
        # 归一化评分 (针对当前日期样本)
        if len(df) > 1:
            df['norm_eff'] = df['efficiency'].rank(pct=True)
            df['norm_win'] = df['winner_rate'].rank(pct=True)
            df['norm_ats'] = df['ats_ratio'].rank(pct=True)
            df['Final_Score'] = (df['norm_eff'] * 0.5) + (df['norm_win'] * 0.3) + (df['norm_ats'] * 0.2)
        else:
            df['Final_Score'] = 1.0
            
        selected_stocks = df.sort_values(['Chip_Tier', 'Final_Score'], ascending=[True, False]).head(9)

        next_day = get_next_trading_day(target_date, pro)
        if next_day:
            df_next = pro.daily(trade_date=next_day)
            if df_next is not None and not df_next.empty:
                # 合并获取次日开盘价
                res = selected_stocks.merge(df_next[['ts_code', 'open']], on='ts_code', suffixes=('', '_next'))
                for _, row in res.iterrows():
                    buy_price = row['close']
                    sell_price = row['open_next'] # 使用次日开盘价
                    profit = (sell_price - buy_price) / buy_price * 100
                    print(f"{target_date:<10} | {row['ts_code']:<10} | {buy_price:<8.2f} | {sell_price:<10.2f} | {profit:>+7.2f}% | T{int(row['Chip_Tier'])}")
                    all_trades.append(profit)
            else:
                print(f"[WARN] {next_day} 数据尚未同步")
        else:
            print(f"[WARN] 未找到 {target_date} 的下一个交易日")

    if all_trades:
        print("-" * 85)
        avg_profit = sum(all_trades) / len(all_trades)
        win_trades = [p for p in all_trades if p > 0]
        win_rate = len(win_trades) / len(all_trades)
        max_p = max(all_trades)
        min_p = min(all_trades)
        
        print(f"REPORT: Strategy Backtest (Buy at 14:40 T -> Sell at Open T+1)")
        print(f"Total Trades: {len(all_trades)}")
        print(f"Average Profit: {avg_profit:.2f}%")
        print(f"Win Rate: {win_rate:.1%}")
        print(f"Max Profit: {max_p:+.2f}%")
        print(f"Max Loss: {min_p:+.2f}%")
        print("=" * 85)
    else:
        print("\n[ERROR] No valid trade data found.")

if __name__ == '__main__':
    backtest_profit_stats()
