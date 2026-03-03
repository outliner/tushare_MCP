import os
import sys
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import numpy as np

# Setup project root
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import CACHE_DB
from cache import stock_intraday_cache_manager

def convert_time(t_int):
    """Convert int time (HHMM or HMM) to HH:MM:00"""
    s = str(t_int).zfill(4)
    hh = s[:2]
    mm = s[2:4]
    return f"{hh}:{mm}:00"

def backfill_date(target_date):
    parquet_path = project_root / "doc" / "data_20250701_20260101.parquet"
    
    print(f"Reading Parquet data for {target_date}...")
    # Use filters to reduce memory usage
    table = pq.read_table(str(parquet_path), filters=[('TradingDay', '==', int(target_date))])
    df = table.to_pandas()
    
    if df.empty:
        print(f"No data found for the date: {target_date}")
        return

    print(f"Loaded {len(df)} records. Mapping codes...")
    # 1. Build Code Mapping
    
    # ======== 获取待补的数据列表 ===============================
    # 优先从 mapping_cache_manager 获取
    from cache import mapping_cache_manager
    mapping_df = mapping_cache_manager.get_all_mapping()
    
    if mapping_df.empty or 'ts_code' not in mapping_df.columns:
        print("警告: 从 stock_sector_mapping 中未找到任何股票记录，回移可能会为空。")
        return
        
    target_stocks = mapping_df['ts_code'].tolist()
    
    # Create numeric to ts_code map
    # ts_code is like 000001.SZ
    code_map = {int(ts_code.split('.')[0]): ts_code for ts_code in target_stocks}
    
    # 2. Map SecuCode to ts_code
    df['ts_code'] = df['SecuCode'].map(code_map)
    df = df.dropna(subset=['ts_code'])
    
    print(f"Mapped {len(df)} records to valid TS codes.")
    
    # 3. Sort and Calculate Cumulative Volume/Turnover
    # Order: ts_code, then Time
    df = df.sort_values(['ts_code', 'Time'])
    
    # 1. Price accumulation (Handle potential 0.0 in HighPrice/LowPrice/ClosePrice)
    # Forward fill prices to avoid gaps or 0.0 values at specific minutes
    price_cols = ['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice']
    for col in price_cols:
        df.loc[df[col] <= 0, col] = np.nan
        df[col] = df.groupby('ts_code')[col].ffill()
    
    # If still NaN at start, use ClosePrice bfill (unlikely but safe)
    df[price_cols] = df.groupby('ts_code')[price_cols].bfill()
    
    # Re-group after price cleanup
    group = df.groupby('ts_code')
    
    df['open'] = group['OpenPrice'].transform('first')
    df['high'] = group['HighPrice'].cummax()
    df['low'] = group['LowPrice'].cummin()
    df['close'] = df['ClosePrice']
    
    # 2. Volume and Turnover accumulation (incremental -> cumulative)
    df['vol'] = group['Volume'].cumsum()
    df['amount'] = group['Turnover'].cumsum()
    
    # 3. Trade count accumulation (incremental -> cumulative)
    df['num'] = group['Nums'].cumsum().fillna(0).astype(int)
    
    # 5. Apply 5-minute sampling strategy
    print(f"Applying 5-minute sampling strategy for {target_date}...")

    # Keep 09:25, 15:00, and points divisible by 5
    df = df[df['Time'].apply(lambda t: t % 5 == 0 or t == 925)]
    print(f"Sampled down to {len(df)} records.")
    
    # 6. Format Time
    df['trade_time'] = df['Time'].apply(convert_time)
    df['trade_date'] = str(target_date)
    
    # 7. Prepare for insertion
    rename_map = {
        'BidPrice1': 'bid_price1',
        'BidVolume1': 'bid_volume1',
        'AskPrice1': 'ask_price1',
        'AskVolume1': 'ask_volume1'
    }
    df_upload = df.rename(columns=rename_map)
    
    print(f"Persisting data via stock_intraday_cache_manager...")
    
    row_count = len(df_upload)
    print(f"Starting batch save for {row_count} rows...")
    
    # 使用 cache manager 代理保存 (兼容远程/本地模式)
    try:
        # 按 trade_time 分组，逐时间点保存
        time_groups = df_upload.groupby('trade_time')
        total_saved = 0
        for trade_time_val, group_df in time_groups:
            # 只选取 cache manager 需要的列
            cols_to_save = [
                'ts_code', 'trade_date', 'open', 'close', 'high', 'low',
                'vol', 'amount', 'num', 'bid_price1', 'bid_volume1',
                'ask_price1', 'ask_volume1'
            ]
            available_cols = [c for c in cols_to_save if c in group_df.columns]
            save_df = group_df[available_cols].copy()
            
            saved = stock_intraday_cache_manager.save_intraday_snapshot(
                save_df, current_time_str=trade_time_val
            )
            total_saved += saved
        
        print(f"Backfill for {target_date} completed successfully. Saved {total_saved} rows.")
        
    except Exception as e:
        print(f"Error during persistence: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Backfill stock intraday data from Parquet.')
    parser.add_argument('--date', type=int, help='Target date (YYYYMMDD)', default=20251230)
    args = parser.parse_args()
    
    backfill_date(args.date)
