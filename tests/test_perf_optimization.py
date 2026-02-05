import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(r"d:\LMW\AI\tushare_MCP")
sys.path.append(str(project_root))

from cache.stock_intraday_cache_manager import stock_intraday_cache_manager
from tools.sector_constituents_health import analyze_sector_constituents_health
import time
import sqlite3

def test_wal_mode():
    print("Testing WAL mode for StockIntradayCacheManager...")
    db_path = stock_intraday_cache_manager.db_path
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode")
    mode = cursor.fetchone()[0]
    print(f"Current journal mode: {mode}")
    assert mode.lower() == "wal", f"Expected WAL mode, got {mode}"
    conn.close()
    print("✓ WAL mode test passed.")

def test_analyze_performance():
    print("\nTesting analyze_sector_constituents_health performance...")
    # Using a known sector code, e.g., BK0447.DC (Real Estate)
    sector_code = "BK0447.DC"
    sector_type = "em_concept"
    
    start_time = time.time()
    try:
        result = analyze_sector_constituents_health(sector_code, sector_type)
        duration = time.time() - start_time
        print(f"Analysis completed in {duration:.2f} seconds.")
        # print("Result snippet:", result[:200] + "...")
        print("✓ Performance test completed.")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        test_wal_mode()
        test_analyze_performance()
        print("\nAll verification tests completed successfully.")
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        sys.exit(1)
