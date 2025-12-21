"""查询长飞光纤的采集数据"""
import sqlite3
import pandas as pd
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB

def query():
    conn = sqlite3.connect(CACHE_DB)
    df = pd.read_sql("SELECT * FROM t1_data WHERE ts_code='601869.SH'", conn)
    conn.close()
    
    if df.empty:
        print("未找到 601869.SH 的采集数据。")
    else:
        print("\n长飞光纤 (601869.SH) 20251219 采集数据：")
        # 转置显示更清晰
        print(df.iloc[0])

if __name__ == '__main__':
    query()

