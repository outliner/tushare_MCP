"""测试特定日期（20251219）的数据采集并分析失败项"""
import sys
from pathlib import Path
import sqlite3
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from newfunction.data_collector import DataCollector
from config.settings import CACHE_DB

def check_failures(ts_codes, t1_date):
    """检查特定日期的各个字段是否采集成功"""
    conn = sqlite3.connect(CACHE_DB)
    # 查询刚插入的数据
    query = f"SELECT * FROM t1_data WHERE ts_code IN ({','.join(['?']*len(ts_codes))})"
    df = pd.read_sql(query, conn, params=ts_codes)
    conn.close()
    
    fields_to_check = [
        'float_share', 'total_mv', 'sum_inst_net', 'list_count',
        'winner_rate', 'cost_concentration', 'margin_cap_ratio',
        'pre_close', 'pre_vol', 'pre_ats'
    ]
    
    print(f"\n{'='*80}")
    print(f"数据采集失败项分析 (日期: {t1_date})")
    print(f"{'='*80}")
    
    if df.empty:
        print("错误：数据库中未找到任何采集记录。")
        return

    for code in ts_codes:
        print(f"\n股票: {code}")
        row = df[df['ts_code'] == code]
        if row.empty:
            print("  [FAIL] 该股票整行记录采集失败。")
            continue
            
        row = row.iloc[0]
        failures = []
        for field in fields_to_check:
            val = row[field]
            if pd.isna(val):
                failures.append(f"{field} (缺失/NaN)")
            elif val == 0 and field in ['float_share', 'total_mv', 'pre_close', 'pre_vol']:
                failures.append(f"{field} (异常值为0)")
        
        if not failures:
            print("  [SUCCESS] 所有核心字段采集成功。")
        else:
            print("  [PARTIAL] 以下字段采集失败或异常:")
            for f in failures:
                print(f"    - {f}")

if __name__ == '__main__':
    collector = DataCollector()
    
    # 我们要采集 20251219 的数据作为 T-1
    # 根据 DataCollector 的逻辑，如果传入 T，它会找 T-1。
    # 今天是 12月21日(周日)，如果传入 12月21日，它会找到上一个交易日 12月19日。
    target_t_day = "20251221"
    t1_date = "20251219"
    test_codes = ['605303.SH', '600519.SH', '000001.SZ']
    
    print(f"目标：搜集 {t1_date} 的数据...")
    
    try:
        # 执行采集
        num = collector.collect_data(test_codes, trade_date=target_t_day)
        print(f"采集完成，影响行数: {num}")
        
        # 分析失败项
        check_failures(test_codes, t1_date)
        
    except Exception as e:
        print(f"采集过程中发生崩溃: {e}")
    finally:
        collector.close()

