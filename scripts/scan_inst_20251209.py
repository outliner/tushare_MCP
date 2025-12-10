"""
扫描20251209机构行情
"""
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

test_date = "20251209"

print("正在扫描2025-12-09机构行情...\n")

try:
    # 模块1：锁定黄金赛道
    tracks_result = _select_golden_tracks(test_date, top_sectors=15)
    
    # 模块2：机构雷达扫描
    radar_result = _scan_institutional_radar(
        test_date,
        tracks_result,
        survey_threshold=20,
        survey_days=30,
        top_inst_days=5
    )
    
    # 模块3：生成机构票池
    pool_result = _generate_stock_pool(radar_result)
    
    # 生成完整报告
    report = _format_inst_track_report(
        test_date,
        tracks_result,
        radar_result,
        pool_result,
        survey_threshold=20,
        survey_days=30,
        top_inst_days=5
    )
    
    print(report)
    
except Exception as e:
    print(f"扫描失败: {e}")
    import traceback
    traceback.print_exc()

