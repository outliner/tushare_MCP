"""
批量采集上周五个交易日的数据
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from newfunction.batch_data_collector import BatchDataCollector

if __name__ == '__main__':
    # 2025-12-21 是周日，上周五个交易日是 12-15 至 12-19
    last_week_dates = ["20251215", "20251216", "20251217", "20251218", "20251219"]
    
    collector = BatchDataCollector()
    
    print(f"准备采集上周数据: {last_week_dates}")
    
    for date in last_week_dates:
        try:
            collector.collect_for_date(date)
        except Exception as e:
            print(f"[CRITICAL] 日期 {date} 采集过程崩溃: {e}")
            
    collector.print_final_report()

