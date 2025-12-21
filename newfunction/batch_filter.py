import sys
from pathlib import Path
import sqlite3

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from newfunction.min15_second_filter import run_second_filter

def main():
    # 需要筛选的日期范围（T日）
    # 20251216 -> T-1 是 20251215
    # 20251217 -> T-1 是 20251216
    # 20251218 -> T-1 是 20251217
    
    tasks = [
        ("20251216", "20251215"),
        ("20251217", "20251216"),
        ("20251218", "20251217")
    ]
    
    for target_date, t1_date in tasks:
        print(f"\n\n{'#'*80}")
        print(f"### 处理任务: Target={target_date}, T-1={t1_date} ###")
        print(f"{'#'*80}")
        try:
            run_second_filter(target_date=target_date, t1_date=t1_date)
        except Exception as e:
            print(f"处理 {target_date} 时出错: {e}")

if __name__ == "__main__":
    main()


