"""快速机构行情扫描"""
import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts

init_env_file()
ts.set_token(get_tushare_token())

from tools.inst_track_tools import _select_golden_tracks, _scan_institutional_radar, _generate_stock_pool, _format_inst_track_report

date = "20251209"
print("扫描2025-12-09机构行情...")

tracks = _select_golden_tracks(date, 15)
radar = _scan_institutional_radar(date, tracks, 20, 30, 5)
pool = _generate_stock_pool(radar)
report = _format_inst_track_report(date, tracks, radar, pool, 20, 30, 5)
print(report)

