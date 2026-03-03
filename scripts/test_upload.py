import os
import sys
import pandas as pd
from pathlib import Path

# 加载环境变量 (必须在 import cache 之前)
from dotenv import load_dotenv
ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# 将项目根目录加入 sys.path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from cache import stock_daily_cache_manager
from cache import sector_strength_cache_manager
from cache import stock_intraday_cache_manager

def test_upload():
    print(f"========================================")
    print(f"当前服务端地址: {os.getenv('DATA_SERVICE_URL')}")
    print(f"是否开启远程模式: {os.getenv('USE_DATA_SERVICE')}")
    print(f"========================================\n")
    
    # 统一使用一个未来的测试日期，方便之后在数据库中识别和清理
    test_date = "20990101"
    test_time = "10:00:00"
    
    # ==========================================
    # 1. 测试上传：每日行情 (Daily)
    # ==========================================
    print("🚀 [1/3] 正在测试上传: 每日行情 (Daily) ...")
    df_daily = pd.DataFrame([{
        "ts_code": "TEST.SH",
        "trade_date": test_date,
        "open": 10.0,
        "close": 11.0,
        "high": 12.0,
        "low": 9.0,
        "pre_close": 9.5,
        "change": 1.5,
        "pct_chg": 15.7,
        "vol": 1000,
        "amount": 11000,
    }])
    try:
        saved_daily = stock_daily_cache_manager.save_stock_daily_data(df_daily)
        if saved_daily > 0:
            print(f"✅ 成功! 写入了 {saved_daily} 条 Daily 测试数据。\n")
        else:
            print(f"⚠️ 失败或未写入 Daily 数据。\n")
    except Exception as e:
        print(f"❌ 上传 Daily 时出错: {e}\n")

    # ==========================================
    # 2. 测试上传：板块强度 (Sector Strength)
    # ==========================================
    print("🚀 [2/3] 正在测试上传: 板块强度 (Sector Strength) ...")
    df_sector = pd.DataFrame([{
        "板块名称": "测试专用板块",
        "sector_type": "test_type",
        "平均涨幅": 5.0,
        "上涨家数比": 80.0,
        "涨幅中位数": 4.0,
        "日内位阶": 0.8,
        "真阳线比例": 90.0,
        "实时量比": 1.5,
        "单笔成交额": 10000.0,
        "委比": 20.0,
        "stock_count": 10,
        "score": 85.5
    }])
    try:
        saved_sector = sector_strength_cache_manager.save_strength_snapshots(
            df_sector, sector_type="test_type", trade_date=test_date, trade_time=test_time
        )
        if saved_sector > 0:
            print(f"✅ 成功! 写入了 {saved_sector} 条 Sector Strength 测试数据。\n")
        else:
            print(f"⚠️ 失败或未写入 Sector Strength 数据。\n")
    except Exception as e:
        print(f"❌ 上传 Sector Strength 时出错: {e}\n")


    # ==========================================
    # 3. 测试上传：分时快照 (Intraday) 
    # ==========================================
    print("🚀 [3/3] 正在测试上传: 分时快照 (Intraday) ...")
    df_intraday = pd.DataFrame([{
        "ts_code": "TEST.SH",
        "trade_date": test_date,
        "trade_time": test_time,
        "open": 10.0,
        "close": 10.5,
        "high": 10.8,
        "low": 9.9,
        "vol": 500.0,
        "amount": 5200.0,
        "num": 50,
        "bid_price1": 10.4,
        "bid_volume1": 100.0,
        "ask_price1": 10.6,
        "ask_volume1": 200.0,
    }])
    try:
        saved_intraday = stock_intraday_cache_manager.save_intraday_snapshot(df_intraday, current_time_str=test_time)
        if saved_intraday > 0:
            print(f"✅ 成功! 写入了 {saved_intraday} 条 Intraday 测试数据。\n")
        else:
            print(f"⚠️ 失败或未写入 Intraday 数据。\n")
    except Exception as e:
        print(f"❌ 上传 Intraday 时出错: {e}")
        print("   -> (提示: 如果报错 404 Not Found, 说明您的阿里云服务端还没部署我刚刚为您写的 stock_intraday 接口代码)\n")

    print("\n🎉 测试结束。")

if __name__ == '__main__':
    test_upload()
