import os
import sys
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime

# Setup project root
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.backfill_from_parquet import backfill_date

def run_batch_backfill(start_date, end_date):
    parquet_path = project_root / "doc" / "data_20250701_20260101.parquet"
    
    print(f"Scanning available trading days in Parquet from {start_date} to {end_date}...")
    try:
        # Only read the TradingDay column to find unique dates efficiently
        table = pq.read_table(str(parquet_path), columns=['TradingDay'])
        all_days = sorted(table.to_pandas()['TradingDay'].unique())
        
        # Filter for the target range
        target_days = [int(d) for d in all_days if start_date <= d <= end_date]
        
        print(f"Found {len(target_days)} trading days: {target_days}")
        
        if not target_days:
            print("No trading days found in the specified range.")
            return

        total_days = len(target_days)
        start_time = datetime.now()
        
        for i, date in enumerate(target_days):
            print(f"\n[{i+1}/{total_days}] Starting backfill for {date}...")
            day_start = datetime.now()
            
            try:
                backfill_date(date)
            except Exception as e:
                print(f"FAILED backfill for {date}: {e}")
            
            day_end = datetime.now()
            print(f"[{i+1}/{total_days}] Finished {date} in {day_end - day_start}")
            
        end_time = datetime.now()
        print(f"\nBatch backfill completed in {end_time - start_time}")

    except Exception as e:
        print(f"Error during batch scanning: {e}")

if __name__ == "__main__":
    # Backfill for approximately one month before 20251230
    # The previousMonth is roughly Dec 1 to Dec 29
    run_batch_backfill(20251201, 20251229)
