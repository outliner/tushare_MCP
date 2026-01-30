"""
板块强度补充采集器（基于分时快照版本）
功能：基于 stock_intraday_data 表中的5分钟快照数据，计算指定时间点的板块强度。
用于回测或补录历史时刻的板块强度快照。

数据来源：
1. stock_intraday_data 表：5分钟快照数据（close, vol, amount）
2. stock_daily_data 表：获取昨收价（pre_close）用于计算涨跌幅
3. stock_sector_mapping 表：股票-行业映射关系
"""
import os
import sys
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# 将项目根目录加入 sys.path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Windows 控制台 UTF-8 编码支持
if sys.platform == 'win32':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')

from config.token_manager import get_tushare_token
from cache.mapping_cache_manager import mapping_cache_manager
from cache.stock_intraday_cache_manager import stock_intraday_cache_manager
from cache.sector_strength_cache_manager import sector_strength_cache_manager
from config.settings import CACHE_DB


def get_previous_trading_date(trade_date: str) -> str:
    """获取前一个交易日（简化版：直接减一天，实际应查交易日历）"""
    dt = datetime.strptime(trade_date, "%Y%m%d")
    prev_dt = dt - timedelta(days=1)
    # 跳过周末
    while prev_dt.weekday() >= 5:  # 5=周六, 6=周日
        prev_dt -= timedelta(days=1)
    return prev_dt.strftime("%Y%m%d")


def run_sector_strength_backfill(target_date: str = None, target_time: str = "15:00:00"):
    """
    执行板块强度补充统计（基于分时快照数据）
    
    参数:
        target_date: 目标日期 (YYYYMMDD格式)，默认为今天
        target_time: 目标时间点 (HH:MM:SS格式)，默认为收盘时间 15:00:00
    """
    # 确定目标日期
    trade_date = target_date if target_date else datetime.now().strftime("%Y%m%d")
    trade_time = target_time
    
    print(f"========================================", file=sys.stderr)
    print(f"📊 板块强度补充采集器（分时快照版）", file=sys.stderr)
    print(f"目标日期: {trade_date}", file=sys.stderr)
    print(f"目标时间: {trade_time}", file=sys.stderr)
    print(f"========================================", file=sys.stderr)
    
    try:
        # 连接数据库
        conn = sqlite3.connect(CACHE_DB)
        
        # 1. 从 stock_intraday_data 表获取指定时间点的快照数据
        print(f"🚀 正在读取 {trade_date} {trade_time} 的分时快照数据...", file=sys.stderr)
        
        # 查询指定日期、最接近目标时间的快照，包含增强字段
        # 注意：对于旧数据，新字段可能为 NULL
        query_intraday = """
            SELECT ts_code, trade_date, trade_time, open, close, high, low, vol, amount, num, bid_volume1, ask_volume1
            FROM stock_intraday_data 
            WHERE trade_date = ? AND trade_time <= ?
        """
        df_intraday = pd.read_sql_query(query_intraday, conn, params=(trade_date, trade_time))
        
        if df_intraday.empty:
            print(f"⚠️ 未找到 {trade_date} 的分时快照数据", file=sys.stderr)
            conn.close()
            return
        
        # 每只股票只保留最接近目标时间的一条记录
        df_intraday = df_intraday.sort_values('trade_time', ascending=False)
        df_intraday = df_intraday.drop_duplicates(subset='ts_code', keep='first')
        
        actual_times = df_intraday['trade_time'].unique()
        print(f"📈 读取到 {len(df_intraday)} 只股票的快照数据", file=sys.stderr)
        
        # 2. 获取昨收价 (pre_close) - 多层次回退策略
        # 策略优先级：前一天分时收盘价 > stock_daily_data 缓存 > Tushare API
        
        # 2.1 首先尝试从前一天分时数据获取收盘价作为昨收
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(trade_date) FROM stock_intraday_data WHERE trade_date < ?", (trade_date,))
        prev_row = cursor.fetchone()
        prev_date = prev_row[0] if prev_row and prev_row[0] else get_previous_trading_date(trade_date)
        
        print(f"📅 前一交易日: {prev_date}", file=sys.stderr)
        
        # 从前一天分时数据的最后收盘价获取 pre_close
        query_prev_intraday = """
            SELECT ts_code, close as pre_close 
            FROM stock_intraday_data 
            WHERE trade_date = ? AND trade_time = '15:00:00'
        """
        df_preclose = pd.read_sql_query(query_prev_intraday, conn, params=(prev_date,))
        
        # 如果 15:00:00 没有数据，尝试获取最后一个时间点
        if df_preclose.empty or len(df_preclose) < 100:
            print(f"   ⚠️ 15:00 数据不足，尝试获取最后时间点...", file=sys.stderr)
            query_prev_last = """
                SELECT ts_code, close as pre_close 
                FROM stock_intraday_data 
                WHERE trade_date = ? AND trade_time = (
                    SELECT MAX(trade_time) FROM stock_intraday_data WHERE trade_date = ?
                )
            """
            df_preclose = pd.read_sql_query(query_prev_last, conn, params=(prev_date, prev_date))
        
        preclose_count = len(df_preclose)
        print(f"   从分时数据获取到 {preclose_count} 条昨收价", file=sys.stderr)
        
        # 2.2 如果分时数据不足，尝试从 stock_daily_data 补充
        if preclose_count < len(df_intraday) * 0.5:
            print(f"   ⚠️ 分时昨收覆盖不足，尝试从日线缓存补充...", file=sys.stderr)
            query_daily_preclose = """
                SELECT ts_code, pre_close 
                FROM stock_daily_data 
                WHERE trade_date = ?
            """
            df_daily_preclose = pd.read_sql_query(query_daily_preclose, conn, params=(trade_date,))
            
            if not df_daily_preclose.empty:
                # 合并：分时数据优先，日线数据补充
                if df_preclose.empty:
                    df_preclose = df_daily_preclose
                else:
                    df_preclose = df_preclose.set_index('ts_code')
                    df_daily_preclose = df_daily_preclose.set_index('ts_code')
                    # 用日线数据补充缺失的
                    missing_codes = df_daily_preclose.index.difference(df_preclose.index)
                    if len(missing_codes) > 0:
                        df_preclose = pd.concat([df_preclose, df_daily_preclose.loc[missing_codes]])
                    df_preclose = df_preclose.reset_index()
                print(f"   日线缓存补充后共 {len(df_preclose)} 条昨收价", file=sys.stderr)
        
        # 2.3 如果仍然不足，从 Tushare API 获取并缓存
        if len(df_preclose) < len(df_intraday) * 0.5:
            print(f"   ⚠️ 昨收数据仍不足，尝试从 Tushare API 获取...", file=sys.stderr)
            try:
                from cache.stock_daily_cache_manager import stock_daily_cache_manager
                import tushare as ts
                from config.token_manager import get_tushare_token
                
                pro = ts.pro_api(get_tushare_token())
                # 获取目标日期的日线数据（包含 pre_close）
                df_api = pro.daily(trade_date=trade_date, fields='ts_code,pre_close,open,high,low,close,vol,amount')
                
                if df_api is not None and not df_api.empty:
                    # 保存到缓存
                    df_api['trade_date'] = trade_date
                    stock_daily_cache_manager.save_stock_daily_data(df_api)
                    print(f"   ✅ 从 API 获取并缓存 {len(df_api)} 条日线数据", file=sys.stderr)
                    
                    # 合并昨收价
                    df_api_preclose = df_api[['ts_code', 'pre_close']].copy()
                    if df_preclose.empty:
                        df_preclose = df_api_preclose
                    else:
                        df_preclose = df_preclose.set_index('ts_code')
                        df_api_preclose = df_api_preclose.set_index('ts_code')
                        missing_codes = df_api_preclose.index.difference(df_preclose.index)
                        if len(missing_codes) > 0:
                            df_preclose = pd.concat([df_preclose, df_api_preclose.loc[missing_codes]])
                        df_preclose = df_preclose.reset_index()
            except Exception as e:
                print(f"   ❌ API 获取失败: {str(e)[:50]}", file=sys.stderr)
        
        # 2.4 获取日线辅助数据 (用于补全快照中缺失的 high/low/num 等)
        query_daily_aux = """
            SELECT ts_code, open as day_open, high as day_high, low as day_low, num as day_num, 
                   bid_volume1 as day_bid_vol, ask_volume1 as day_ask_vol
            FROM stock_daily_data 
            WHERE trade_date = ?
        """
        df_daily_aux = pd.read_sql_query(query_daily_aux, conn, params=(trade_date,))
        
        # 合并昨收价到 df_daily（用于后续合并）
        if not df_preclose.empty:
            if df_daily_aux.empty:
                df_daily = df_preclose
            else:
                df_daily = df_daily_aux.merge(df_preclose, on='ts_code', how='outer')
        else:
            df_daily = df_daily_aux
        
        print(f"   最终昨收数据: {len(df_preclose)} 条", file=sys.stderr)
        
        # 合并数据
        df_rt = df_intraday.merge(df_daily, on='ts_code', how='left')
        
        # 补全逻辑：如果分时快照中字段缺失（旧数据），则回退使用当日日线字段
        for col in ['open', 'high', 'low', 'num', 'bid_volume1', 'ask_volume1']:
            daily_col = f'day_{col}' if col != 'num' else 'day_num'
            if daily_col in df_rt.columns:
                # 确保参与运算的列均为数值型，避免 fillna 时的 Downcasting 警告
                df_rt[col] = pd.to_numeric(df_rt[col], errors='coerce')
                df_rt[daily_col] = pd.to_numeric(df_rt[daily_col], errors='coerce')
                df_rt[col] = df_rt[col].fillna(df_rt[daily_col])
        
        # 计算涨跌幅
        df_rt['pct_chg'] = 0.0
        if 'pre_close' in df_rt.columns:
            mask = (df_rt['pre_close'] > 0)
            df_rt.loc[mask, 'pct_chg'] = (df_rt.loc[mask, 'close'] - df_rt.loc[mask, 'pre_close']) / df_rt.loc[mask, 'pre_close'] * 100
        
        df_rt = df_rt.set_index('ts_code')
        
        # 4. 加载映射关系
        df_mapping = pd.read_sql_query("SELECT * FROM stock_sector_mapping", conn)
        print(f"📋 加载 {len(df_mapping)} 条股票-行业映射", file=sys.stderr)
        
        # 5. 获取历史基准量比数据
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(trade_date) FROM stock_intraday_data WHERE trade_date < ?", (trade_date,))
        last_date_row = cursor.fetchone()
        hist_date = last_date_row[0] if last_date_row and last_date_row[0] else None
        
        hist_amt_map = {}
        if hist_date:
            print(f"📅 历史基准日期: {hist_date}", file=sys.stderr)
            query_hist = "SELECT ts_code, trade_time, amount FROM stock_intraday_data WHERE trade_date = ? AND trade_time <= ?"
            df_hist = pd.read_sql_query(query_hist, conn, params=(hist_date, trade_time))
            if not df_hist.empty:
                df_hist = df_hist.sort_values('trade_time', ascending=False).drop_duplicates('ts_code')
                hist_amt_map = dict(zip(df_hist['ts_code'], df_hist['amount']))
        
        conn.close()
        
        # 6. 统计逻辑 (按板块聚合)
        df_combined = df_mapping.merge(df_rt, left_on='ts_code', right_index=True)
        
        results = []
        # 同时分析 sw_l2 和 em_industry 以增强覆盖
        for sector_type_key, sector_name_col in [('sw_l2', 'sw_l2_name'), ('em_industry', 'em_industry_name')]:
            print(f"   正在分析 {sector_type_key} 维度...", file=sys.stderr)
            
            for name, group in df_combined.groupby(sector_name_col):
                if not name or group.empty: continue
                
                count = len(group)
                
                # 广度
                avg_pct = group['pct_chg'].mean()
                rising_ratio = (group['pct_chg'] > 0).sum() / count * 100
                median_return = group['pct_chg'].median()
                
                # 质量
                price_range = group['high'] - group['low']
                intraday_pos_series = ((group['close'] - group['low']) / price_range.replace(0, pd.NA)).dropna()
                avg_intraday_pos = intraday_pos_series.mean() if not intraday_pos_series.empty else 0.5
                real_body_ratio = (group['close'] > group['open']).sum() / count * 100
                
                # 量能
                amt_now = group['amount'].sum()
                amt_hist = sum([hist_amt_map.get(tc, 0) for tc in group['ts_code']])
                volume_ratio = amt_now / amt_hist if amt_hist > 0 else 1.0
                
                sum_num = group['num'].sum()
                value_per_trade = (amt_now * 1000) / sum_num if sum_num > 0 else 0
                
                # 盘口
                bid_sum = pd.to_numeric(group['bid_volume1'], errors='coerce').fillna(0).sum()
                ask_sum = pd.to_numeric(group['ask_volume1'], errors='coerce').fillna(0).sum()
                order_imbalance = (bid_sum - ask_sum) / (bid_sum + ask_sum) * 100 if (bid_sum + ask_sum) > 0 else 0
                
                # ========== 评分（归一化 + 多维度均衡加权）==========
                # 1. 涨幅维度 (25%): avg_pct 归一化到 0-100
                #    假设正常涨跌幅范围 -10% ~ +10%
                pct_score = (avg_pct + 10) / 20 * 100
                pct_score = max(0, min(100, pct_score))
                
                # 2. 广度维度 (25%): 上涨家数比 + 中位数涨幅
                #    rising_ratio 已是 0-100, median_return 假设 -5% ~ +5%
                median_norm = (median_return + 5) / 10 * 100
                median_norm = max(0, min(100, median_norm))
                breadth_score = rising_ratio * 0.6 + median_norm * 0.4
                
                # 3. 质量维度 (20%): 日内位阶 + 真阳线比例
                #    avg_intraday_pos 是 0-1, real_body_ratio 是 0-100
                quality_score = avg_intraday_pos * 50 + real_body_ratio * 0.5
                
                # 4. 量能维度 (15%): 量比得分 + 单笔成交额
                #    volume_ratio 上限3倍, value_per_trade 假设上限5万元
                vr_score = min(volume_ratio, 3.0) / 3.0 * 100
                vpt_score = min(value_per_trade, 50000) / 50000 * 100
                volume_score = vr_score * 0.6 + vpt_score * 0.4
                
                # 5. 盘口维度 (15%): 委比归一化
                #    order_imbalance 范围 -100 ~ +100, 映射到 0-100
                order_score = (order_imbalance + 100) / 200 * 100
                order_score = max(0, min(100, order_score))
                
                # 综合评分（加权求和）
                score = (
                    pct_score * 0.25 +       # 涨幅权重 25%
                    breadth_score * 0.25 +   # 广度权重 25%
                    quality_score * 0.20 +   # 质量权重 20%
                    volume_score * 0.15 +    # 量能权重 15%
                    order_score * 0.15       # 盘口权重 15%
                )
                
                results.append({
                    '板块名称': name,
                    'sector_type': sector_type_key,
                    '平均涨幅': avg_pct,
                    '上涨家数比': rising_ratio,
                    '涨幅中位数': median_return,
                    '日内位阶': avg_intraday_pos,
                    '真阳线比例': real_body_ratio,
                    '实时量比': volume_ratio,
                    '单笔成交额': value_per_trade,
                    '委比': order_imbalance,
                    'stock_count': count,
                    'score': score
                })

        df_final = pd.DataFrame(results)
        
        if df_final.empty:
            print("⚠️ 无有效数据可保存", file=sys.stderr)
            return
        
        # 存档分片处理
        saved_total = 0
        actual_time = df_intraday['trade_time'].mode().iloc[0] if len(df_intraday) > 0 else trade_time
        
        for stype in df_final['sector_type'].unique():
            df_sub = df_final[df_final['sector_type'] == stype]
            saved = sector_strength_cache_manager.save_strength_snapshots(
                df_sub, 
                sector_type=stype, 
                trade_date=trade_date, 
                trade_time=actual_time
            )
            saved_total += saved
            print(f"   已保存 {saved} 个 {stype} 板块快照", file=sys.stderr)
        
        print(f"", file=sys.stderr)
        print(f"✅ 板块强度补充完成！累计保存 {saved_total} 条增强快照。 (Time: {actual_time})", file=sys.stderr)
        
        # 显示 Top 10 (混搭显示)
        df_sorted = df_final.sort_values('score', ascending=False).head(10)
        print(f"", file=sys.stderr)
        print(f"📊 Top 10 强势板块 (混合):", file=sys.stderr)
        for i, (_, r) in enumerate(df_sorted.iterrows(), 1):
            print(f"   {i:2d}. {r['板块名称']:<10} [{r['sector_type']}] 得分:{r['score']:.2f} 涨幅:{r['平均涨幅']:+.2f}% 委比:{r['委比']:.1f}", file=sys.stderr)
        
    except Exception as e:
        import traceback
        print(f"❌ 补充统计出错: {str(e)}", file=sys.stderr)
        traceback.print_exc()


def main():
    """主函数：支持命令行参数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='板块强度补充采集器（基于分时快照版本）')
    parser.add_argument('--date', '-d', type=str, default=None,
                        help='目标日期 (YYYYMMDD格式)，默认为今天')
    parser.add_argument('--time', '-t', type=str, default="15:00:00",
                        help='目标时间点 (HH:MM:SS格式)，默认为 15:00:00')
    
    args = parser.parse_args()
    
    run_sector_strength_backfill(target_date=args.date, target_time=args.time)


if __name__ == "__main__":
    main()
