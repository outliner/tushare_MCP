"""
分析东财热门概念前5的量价异动情况
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts
from datetime import datetime, timedelta
from tools.concept_tools import (
    get_hot_concept_codes,
    analyze_concept_volume_anomaly,
    scan_concept_volume_anomaly
)

# 初始化
init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)
    print("✓ 已加载 Tushare token\n")
else:
    print("⚠️  未找到 Tushare token")
    sys.exit(1)

# 获取昨天的日期

end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
print(f"分析日期: {end_date}\n")

# 获取热门概念板块（前5个）


scan_result = scan_concept_volume_anomaly(
    end_date=end_date,
    vol_ratio_threshold=1.2,
    price_change_5d_min=0.02,
    price_change_5d_max=0.08,
    hot_limit=80
)

if scan_result and 'matches' in scan_result and len(scan_result['matches']) > 0:
    print(f"✓ 扫描结果: 共扫描 {scan_result.get('scanned_count', 0)} 个概念，发现 {scan_result.get('matched_count', 0)} 个异动")
    print()
    
    # 只显示前5个热门概念中的异动
    matched_in_top5 = [m for m in scan_result['matches'] if m.get('concept_code') in concept_codes]
    
    if matched_in_top5:
        print("前5个热门概念中的异动情况：")
        for match in matched_in_top5:
            print(f"  • {match.get('concept_name', match.get('concept_code'))}")
            print(f"    成交量比率: {match.get('volume_ratio', 0):.2f}")
            print(f"    5日涨幅: {match.get('price_change_5d', 0)*100:.2f}%")
            print(f"    信号类型: {match.get('signal_type', 'Unknown')}")
            print()
    else:
        print("前5个热门概念中未发现异动")
else:
    print("⚠️  扫描未发现异动")

