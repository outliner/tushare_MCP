"""测试数据采集器"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from newfunction.data_collector import DataCollector
from config.settings import CACHE_DB
import sqlite3
import pandas as pd

def verify_t1_data():
    """验证 t1_data 表中的数据"""
    conn = sqlite3.connect(CACHE_DB)
    df = pd.read_sql("SELECT * FROM t1_data", conn)
    conn.close()
    
    if df.empty:
        print("t1_data 表为空")
    else:
        print("\nt1_data 表数据预览:")
        print(df.to_string(index=False))

if __name__ == '__main__':
    collector = DataCollector()
    
    # 使用一些具有代表性的股票代码
    # 605303 是用户提到的
    test_codes = ['605303.SH', '600519.SH', '000001.SZ']
    
    print(f"开始为以下股票搜集数据: {test_codes}")
    try:
        # 尝试搜集最近一个交易日的 T-1 数据
        num = collector.collect_data(test_codes)
        print(f"\n成功更新/插入 {num} 条记录")
        
        # 验证结果
        verify_t1_data()
        
    except Exception as e:
        print(f"执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        collector.close()

