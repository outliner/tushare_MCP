import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from cache.stock_intraday_cache_manager import stock_intraday_cache_manager
import pandas as pd
import time

def test_persistence():
    # 模拟数据
    test_data = {
        'ts_code': ['TEST.SH'],
        'open': [10.0],
        'close': [10.5],
        'high': [11.0],
        'low': [9.5],
        'vol': [1000],
        'amount': [10000],
        'num': [100],
        'bid_price1': [10.45],
        'bid_volume1': [500],
        'ask_price1': [10.55],
        'ask_volume1': [600],
        'trade_date': ['20260102']
    }
    df = pd.DataFrame(test_data)
    
    # 保存
    time_str = "10:00:00"
    saved = stock_intraday_cache_manager.save_intraday_snapshot(df, time_str)
    print(f"Saved {saved} records.")
    
    # 查询
    snapshot = stock_intraday_cache_manager.get_historical_snapshot('TEST.SH', '20260102', '10:00:00')
    if snapshot:
        print("Retrieved snapshot:")
        for k, v in snapshot.items():
            print(f"  {k}: {v}")
        
        # 验证关键字段
        assert snapshot['bid_price1'] == 10.45
        assert snapshot['ask_price1'] == 10.55
        print("\n Verification SUCCESS: bid_price1 and ask_price1 stored correctly.")
    else:
        print(" Verification FAILED: Could not retrieve snapshot.")

if __name__ == "__main__":
    test_persistence()
