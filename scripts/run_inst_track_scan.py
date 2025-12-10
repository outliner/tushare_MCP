"""执行机构抱团扫描"""
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts

init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)

# 导入工具函数
from tools.inst_track_tools import (
    _select_golden_tracks,
    _scan_institutional_radar,
    _generate_stock_pool,
    _format_inst_track_report
)

# 使用最新交易日（2025-12-09）
trade_date = "20251209"

print("=" * 60)
print("机构抱团扫描 (Institutional Track)")
print("=" * 60)
print(f"分析日期: {trade_date}\n")

try:
    # 模块1：锁定黄金赛道
    print("[1/3] 锁定黄金赛道...")
    tracks_result = _select_golden_tracks(trade_date, top_sectors=15)
    
    # 模块2：机构雷达扫描
    print("[2/3] 机构雷达扫描...")
    radar_result = _scan_institutional_radar(
        trade_date,
        tracks_result,
        survey_threshold=20,
        survey_days=30,
        top_inst_days=5
    )
    
    # 模块3：生成机构票池
    print("[3/3] 生成机构票池...")
    pool_result = _generate_stock_pool(radar_result)
    
    # 生成完整报告
    print("\n生成报告...\n")
    report = _format_inst_track_report(
        trade_date,
        tracks_result,
        radar_result,
        pool_result,
        survey_threshold=20,
        survey_days=30,
        top_inst_days=5
    )
    
    print(report)
    
except Exception as e:
    print(f"\n扫描失败: {e}")
    import traceback
    traceback.print_exc()


