"""
板块强度分时追踪采集器
功能：每隔5分钟全局计算一次所有东财行业板块的综合得分，并存入时间序列数据库。
用于纵向分析当天板块强弱的演变过程。
"""
import os
import sys
import time
import schedule
import pandas as pd
import json
import tushare as ts
from datetime import datetime, time as dt_time
from pathlib import Path

# 加载环境变量
from dotenv import load_dotenv
ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# 将项目根目录加入 sys.path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.token_manager import get_tushare_token
from cache import mapping_cache_manager, stock_intraday_cache_manager, sector_strength_cache_manager, stock_daily_cache_manager

def is_market_open():
    """判断当前是否在交易时间内"""
    now = datetime.now()
    if now.weekday() >= 5: return False
    
    current_time = now.time()
    # 09:30 - 11:30, 13:00 - 15:00
    return (dt_time(9, 30) <= current_time <= dt_time(11, 31)) or \
           (dt_time(13, 0) <= current_time <= dt_time(15, 1))

def run_sector_strength_analysis():
    """执行板块强度分析并存档"""
    if not is_market_open():
        print(f"[{datetime.now()}] 😴 非交易时间，跳过本次任务。", file=sys.stderr)
        return

    token = get_tushare_token()
    if not token:
        print("❌ 未配置 Tushare Token", file=sys.stderr)
        return
        
    pro = ts.pro_api()
    now = datetime.now()
    trade_date = now.strftime("%Y%m%d")
    trade_time = now.strftime("%H:%M:%S")
    
    print(f"[{now}] 🚀 开始执行板块强度分时统计...", file=sys.stderr)
    
    try:
        # 1. 获取最新实时行情 (rt_k)，包含更多字段以计算增强指标
        # 默认不返回 bid/ask 数据，需在 fields 中显式指定
        fields = 'ts_code,open,close,high,low,pre_close,pct_chg,vol,amount,num,bid_volume1,ask_volume1'
        patterns = ['6*.SH', '0*.SZ', '3*.SZ', '4*.BJ', '8*.BJ']
        rt_dfs = []
        for p in patterns:
            try:
                df_p = pro.rt_k(ts_code=p, fields=fields)
                if df_p is not None and not df_p.empty:
                    rt_dfs.append(df_p)
            except: continue
            
        if not rt_dfs:
            print("⚠️ 未抓取到实时行情", file=sys.stderr)
            return
            
        df_rt = pd.concat(rt_dfs).set_index('ts_code')
        
        # 确保数值列为正确类型，避免后续计算触发 FutureWarning
        numeric_cols = ['open', 'close', 'high', 'low', 'pre_close', 'pct_chg', 'vol', 'amount', 'num', 'bid_volume1', 'ask_volume1']
        for col in numeric_cols:
            if col in df_rt.columns:
                df_rt[col] = pd.to_numeric(df_rt[col], errors='coerce')
        
        # 确保包含必要字段 (处理 rt_k 可能不返回计算字段的情况)
        if 'pct_chg' not in df_rt.columns and 'pre_close' in df_rt.columns and 'close' in df_rt.columns:
            df_rt['pct_chg'] = (df_rt['close'] - df_rt['pre_close']) / df_rt['pre_close'] * 100
        
        # 2. 加载映射关系
        df_mapping = mapping_cache_manager.get_all_mapping()
        if df_mapping.empty:
            print("⚠️ 未加载到任何板块映射数据，请检查 mapping 缓存是否已初始化", file=sys.stderr)
            return
            
        # 3. 确定历史基准日期 (昨日最近的一天)
        # 获取最新的 stock_daily_data 日期作为基准日 (如果能获取到的话)
        daily_stats = stock_daily_cache_manager.get_stats()
        hist_date = None
        if daily_stats and 'total' in daily_stats and daily_stats['total'].get('total_records', 0) > 0:
            # 临时简化处理: 获取上一个工作日
            import datetime as dt
            today = dt.date.today()
            offset = max(1, (today.weekday() + 6) % 7 - 3)
            timedelta = dt.timedelta(offset)
            prev_trade = today - timedelta
            hist_date = prev_trade.strftime("%Y%m%d")
        
        # 4. 统计逻辑 (按板块聚合)
        # 我们在这里计算：
        # - 广度：平均涨幅, 上涨占比, 涨幅中位数
        # - 质量：日内位阶, 真阳线比例
        # - 量能：放量系数, 单笔成交金
        # - 盘口：委比
        
        # 先合并行情与板块映射
        df_combined = df_mapping.merge(df_rt, left_on='ts_code', right_index=True)
        
        # 如果有历史基准，获取历史分时数据用于量比计算
        hist_amounts = {}
        if hist_date:
            # 这里为了效率，可能需要批量获取历史数据
            # 但目前 stock_intraday_cache_manager 主要是单个获取
            # 优化：在循环外处理或维持现有逻辑
            pass

        results = []
        
        # 同时分析 sw_l2 和 em_industry 两个维度（与 backfill 保持一致）
        for sector_type_key, sector_name_col in [('sw_l2', 'sw_l2_name'), ('em_industry', 'em_industry_name')]:
            for sector_name, group in df_combined.groupby(sector_name_col):
                if not sector_name or group.empty: continue
                
                count = len(group)
                
                # 广度
                avg_pct = group['pct_chg'].mean()
                rising_count = (group['close'] > group['pre_close']).sum()
                rising_ratio = rising_count / count * 100
                median_return = group['pct_chg'].median()
                
                # 质量
                price_range = group['high'] - group['low']
                intraday_pos = ((group['close'] - group['low']) / price_range.replace(0, pd.NA)).dropna()
                avg_intraday_pos = intraday_pos.mean() if not intraday_pos.empty else 0.5
                real_body_count = (group['close'] > group['open']).sum()
                real_body_ratio = real_body_count / count * 100
                
                # 量能
                vol_now = group['amount'].sum()
                # 获取历史同期成交额 (简单起见，如果找不到历史则设为1.0)
                vol_hist = 0
                if hist_date:
                    # 为了性能，这里我们可能无法对每个股票都查一次，
                    # 但 collector 是周期运行的，可以接受一点延迟
                    # 或者我们就用 aggregate amount
                    for ts_code in group['ts_code']:
                        hist_snap = stock_intraday_cache_manager.get_historical_snapshot(ts_code, hist_date, trade_time)
                        if hist_snap:
                            vol_hist += hist_snap.get('amount', 0)
                
                volume_ratio = vol_now / vol_hist if vol_hist > 0 else 1.0
                
                # 单笔成交额
                sum_num = group['num'].sum()
                value_per_trade = (group['amount'].sum() * 1000) / sum_num if sum_num > 0 else 0
                
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
                    '板块名称': sector_name,
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
        if not df_final.empty:
            # 分板块类型存储
            saved_total = 0
            for stype in df_final['sector_type'].unique():
                df_sub = df_final[df_final['sector_type'] == stype]
                saved = sector_strength_cache_manager.save_strength_snapshots(
                    df_sub, 
                    sector_type=stype, 
                    trade_date=trade_date, 
                    trade_time=trade_time
                )
                saved_total += saved
            print(f"✅ 板块强度统计完成！保存 {saved_total} 个板块快照 (sw_l2 + em_industry)。 (Time: {trade_time})", file=sys.stderr)
        
    except Exception as e:
        print(f"❌ 统计任务出错: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()

def main():
    print("========================================", file=sys.stderr)
    print("📊 板块强度分时采集器已启动", file=sys.stderr)
    print("分析维度: sw_l2 (申万二级) + em_industry (东财行业)", file=sys.stderr)
    print("频率: 每 5 分钟计算一次得分并存盘", file=sys.stderr)
    print("========================================", file=sys.stderr)

    run_sector_strength_analysis()
    schedule.every(5).minutes.do(run_sector_strength_analysis)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 采集器已停止。", file=sys.stderr)
            break
        except Exception as e:
            time.sleep(10)

if __name__ == "__main__":
    main()
