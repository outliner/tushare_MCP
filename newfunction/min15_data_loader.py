import sqlite3
import pandas as pd
import zipfile
import io
from pathlib import Path
import os
import sys

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DIR

DB_PATH = CACHE_DIR / "min15_cache.db"
DATA_DIR = Path("newfunction/min15data")

def init_db():
    """初始化数据库并创建表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 启用WAL模式提升并发性能
    conn.execute('PRAGMA journal_mode=WAL')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_min15 (
            trade_time TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            open REAL,
            close REAL,
            high REAL,
            low REAL,
            vol INTEGER,
            amount REAL,
            pct_chg REAL,
            amplitude REAL,
            PRIMARY KEY (trade_time, ts_code)
        )
    ''')
    
    # 增加索引以优化查询
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_min15_ts_code ON stock_min15(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_min15_trade_time ON stock_min15(trade_time)')
    
    conn.commit()
    conn.close()
    print(f"[SUCCESS] 数据库已初始化: {DB_PATH}")

def load_data_from_zip(zip_path: Path):
    """从单个zip文件中读取所有CSV并存入数据库"""
    print(f"正在处理: {zip_path.name}")
    conn = sqlite3.connect(DB_PATH)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            total_files = len(csv_files)
            
            for i, csv_file in enumerate(csv_files):
                if i % 100 == 0:
                    print(f"  进度: {i}/{total_files} ({csv_file})")
                
                with z.open(csv_file) as f:
                    # 读取内容，指定编码（通常是GBK或UTF-8，根据sz000001.csv观察，可能是UTF-8）
                    # sz000001.csv 包含中文 "平安银行"
                    try:
                        df = pd.read_csv(f, encoding='utf-8')
                    except UnicodeDecodeError:
                        # 如果utf-8失败，尝试gbk
                        with z.open(csv_file) as f2:
                            df = pd.read_csv(f2, encoding='gbk')
                    
                    if df.empty:
                        continue
                        
                    # 映射列名到数据库字段
                    df.columns = ['trade_time', 'ts_code', 'name', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg', 'amplitude']
                    
                    # 写入数据库 (使用 INSERT OR REPLACE)
                    # 转换数据为 tuple 列表进行批量插入
                    data_list = df.values.tolist()
                    cursor = conn.cursor()
                    cursor.executemany('''
                        INSERT OR REPLACE INTO stock_min15 
                        (trade_time, ts_code, name, open, close, high, low, vol, amount, pct_chg, amplitude)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', data_list)
                    
        conn.commit()
        print(f"  [DONE] {zip_path.name} 处理完成")
        
    except Exception as e:
        print(f"  [ERROR] 处理 {zip_path.name} 时出错: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    # 1. 初始化数据库
    init_db()
    
    # 2. 获取所有 zip 文件
    zip_files = sorted(list(DATA_DIR.glob("*.zip")))
    
    if not zip_files:
        print(f"未在 {DATA_DIR} 中找到 zip 文件")
        return
    
    print(f"共找到 {len(zip_files)} 个 zip 文件")
    
    # 3. 逐个处理
    for zip_file in zip_files:
        load_data_from_zip(zip_file)
        
    print("\n[FINISH] 所有数据已成功导入数据库。")

if __name__ == "__main__":
    main()

