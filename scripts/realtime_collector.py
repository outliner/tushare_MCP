"""
实时成交量/行情采集脚本
功能：每隔5分钟全局拉取一次实时日线数据(rt_k)，并同时保存到日线缓存和分时时间序列数据库。
"""
import os
import sys
import time
import schedule
import pandas as pd
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
from cache import stock_daily_cache_manager
from cache import stock_intraday_cache_manager

def is_market_open():
    """判断当前是否在交易时间内"""
    now = datetime.now()
    # 周六日不交易
    if now.weekday() >= 5:
        return False
    
    current_time = now.time()
    # 交易时间段：09:25 - 11:35, 13:00 - 15:05 (增加前后缓冲)
    morning_start = dt_time(9, 25)
    morning_end = dt_time(11, 35)
    afternoon_start = dt_time(13, 0)
    afternoon_end = dt_time(15, 6)
    
    if morning_start <= current_time <= morning_end:
        return True
    if afternoon_start <= current_time <= afternoon_end:
        return True
        
    return False

def sync_realtime_data():
    """执行同步任务"""
    if not is_market_open():
        print(f"[{datetime.now()}] 😴 非交易时间，跳过本次同步。", file=sys.stderr)
        return

    token = get_tushare_token()
    if not token:
        print("❌ 未配置 Tushare Token", file=sys.stderr)
        return
        
    pro = ts.pro_api()
    # 分批抓取：Tushare rt_k 仅支持 SH/SZ 市场的单字符前缀通配符，不支持北交所 (BJ)
    batches = ['6*.SH', '0*.SZ', '3*.SZ']
    
    now = datetime.now()
    current_time_str = now.strftime("%H:%M:%S")
    trade_date = now.strftime("%Y%m%d")
    
    print(f"[{now}] 🚀 开始同步实时日线数据 (Time: {current_time_str})...", file=sys.stderr)
    total_daily = 0
    total_intraday = 0
    
    # 重试配置
    max_retries = 3
    retry_delays = [5, 10, 20]  # 递增等待时间（秒）
    
    for pattern in batches:
        success = False
        for attempt in range(max_retries):
            try:
                print(f"  📥 正在抓取批次: {pattern}..." + (f" (重试 {attempt})" if attempt > 0 else ""), file=sys.stderr)
                # 显式指定字段，确保包含委比计算需要的 bid_volume1, ask_volume1
                fields = 'ts_code,open,close,high,low,pre_close,pct_chg,vol,amount,num,bid_volume1,ask_volume1'
                df = pro.rt_k(ts_code=pattern, fields=fields)
                
                if df is not None and not df.empty:
                    # 处理 pct_chg 和 change (以便日线分析)
                    if 'pct_chg' not in df.columns and 'pre_close' in df.columns and 'close' in df.columns:
                        df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close'] * 100
                    if 'change' not in df.columns and 'pre_close' in df.columns and 'close' in df.columns:
                        df['change'] = df['close'] - df['pre_close']
                    
                    # rt_k 接口不返回 trade_date，需要手动补充
                    if 'trade_date' not in df.columns:
                        df['trade_date'] = trade_date

                    # 1. 保存到日线表 (INSERT OR REPLACE, 保持当日最新状态)
                    saved_daily = stock_daily_cache_manager.save_stock_daily_data(df)
                    total_daily += saved_daily
                    
                    # 2. 保存到分时时间序列表 (INSERT OR IGNORE, 记录历史时刻快照)
                    saved_intraday = stock_intraday_cache_manager.save_intraday_snapshot(df, current_time_str)
                    total_intraday += saved_intraday
                    
                    print(f"  ✅ {pattern}: 日线更新={saved_daily}, 分时序列={saved_intraday}", file=sys.stderr)
                else:
                    print(f"  ⚠️ {pattern}: 返回数据为空", file=sys.stderr)
                
                success = True
                break  # 成功，跳出重试循环
                
            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    wait_time = retry_delays[attempt]
                    print(f"  ⚠️ 抓取批次 {pattern} 失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}", file=sys.stderr)
                    print(f"     等待 {wait_time} 秒后重试...", file=sys.stderr)
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ 抓取批次 {pattern} 最终失败 ({max_retries}次尝试): {error_msg}", file=sys.stderr)
        
        if not success:
            print(f"  💔 批次 {pattern} 跳过，将在下一轮采集中补充", file=sys.stderr)
            
    print(f"[{datetime.now()}] ✨ 同步完成！累计日线更新 {total_daily} 条，分时序列新增 {total_intraday} 条。", file=sys.stderr)


def main():
    print("========================================", file=sys.stderr)
    print("📈 Tushare 实时行情 & 分时时间序列采集器已启动", file=sys.stderr)
    print("频率: 每 5 分钟执行一次", file=sys.stderr)
    print("存储: 同时同步到 Daily 缓存和 Intraday 序列数据库", file=sys.stderr)
    print("========================================", file=sys.stderr)

    # 首次启动先运行一次
    sync_realtime_data()

    # 安排定时任务
    schedule.every(5).minutes.do(sync_realtime_data)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 采集器已停止。", file=sys.stderr)
            break
        except Exception as e:
            print(f"⚠️ 循环出错: {str(e)}", file=sys.stderr)
            time.sleep(10)

if __name__ == "__main__":
    main()
