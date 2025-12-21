import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from newfunction.min15_second_filter import run_second_filter

def main():
    dates = [
        ("20251216", "20251215"),
        ("20251217", "20251216"),
        ("20251218", "20251217"),
    ]
    
    for target_date, t1_date in dates:
        print(f"\n\n>>> 正在运行 {target_date} 的筛选 (参考 T-1: {t1_date}) <<<")
        run_second_filter(target_date=target_date, t1_date=t1_date)

if __name__ == "__main__":
    main()

