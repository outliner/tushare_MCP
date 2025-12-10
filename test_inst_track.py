
import sys
import os
import time
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from tools.inst_track_tools import inst_track_scan
    from config.token_manager import get_tushare_token
    import tushare as ts
    
    token = get_tushare_token()
    if token:
        ts.set_token(token)
        print(f"Token set: {token[:5]}...")
    else:
        print("No token found")
        sys.exit(1)

    print("Starting inst_track_scan...")
    start_time = time.time()
    
    # Run with defaults
    try:
        # Explicitly call the function wrapped by @mcp.tool() if needed, 
        # but here we imported the clean function if it wasn't decorated yet? 
        # Wait, inside inst_track_tools.py, inst_track_scan is defined INSIDE register_inst_track_tools.
        # So I cannot import it directly!
        pass
    except ImportError:
        pass

except Exception as e:
    print(f"Setup error: {e}")

# Re-reading inst_track_tools.py to see how to invoke it.
# It is defined inside register_inst_track_tools. I cannot import it directly.
# I have to copy the logic or modify the file to expose it, or instantiate the tools.

# Better approach: Copy the logic of _select_golden_tracks and run that, as that's the heavy part.
from tools.inst_track_tools import _select_golden_tracks, _scan_institutional_radar, _generate_stock_pool, _format_inst_track_report, _get_latest_trade_date

def run_test():
    trade_date = _get_latest_trade_date()
    print(f"Trade date: {trade_date}")
    
    print("Running _select_golden_tracks...")
    t0 = time.time()
    tracks_result = _select_golden_tracks(trade_date, 15)
    print(f"_select_golden_tracks took {time.time() - t0:.2f}s")
    
    if not tracks_result['success']:
        print(f"Failed: {tracks_result.get('error')}")
        return

    print("Running _scan_institutional_radar...")
    t1 = time.time()
    radar_result = _scan_institutional_radar(trade_date, tracks_result, 20, 30, 5)
    print(f"_scan_institutional_radar took {time.time() - t1:.2f}s")

    print("Running _generate_stock_pool...")
    pool_result = _generate_stock_pool(radar_result)
    
    print("Formatting report...")
    report = _format_inst_track_report(trade_date, tracks_result, radar_result, pool_result, 20, 30, 5)
    print("Done.")
    # Write to file
    try:
        with open('report.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        print("Report saved to report.txt")
    except Exception as e:
        print(f"Error saving report: {e}")

if __name__ == "__main__":
    run_test()
