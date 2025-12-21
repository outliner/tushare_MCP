"""诊断两融数据缺失问题"""
import sqlite3
import pandas as pd
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from config.settings import CACHE_DB

def diagnose():
    conn = sqlite3.connect(CACHE_DB)
    # 对比 12-18 和 12-19
    df_18 = pd.read_sql("SELECT ts_code, margin_cap_ratio FROM t1_data WHERE trade_date = '20251218'", conn)
    df_19 = pd.read_sql("SELECT ts_code, margin_cap_ratio FROM t1_data WHERE trade_date = '20251219'", conn)
    conn.close()
    
    m18 = df_18[df_18['margin_cap_ratio'] > 0].copy()
    m19 = df_19[df_19['margin_cap_ratio'] > 0].copy()
    
    print(f"2025-12-18 两融标数: {len(m18)}")
    print(f"2025-12-19 两融标数: {len(m19)}")
    
    m18['market'] = m18['ts_code'].str[-2:]
    m19['market'] = m19['ts_code'].str[-2:]
    
    print("\n12-18 交易所分布:")
    print(m18.groupby('market').size())
    print("\n12-19 交易所分布:")
    print(m19.groupby('market').size())

if __name__ == '__main__':
    diagnose()

