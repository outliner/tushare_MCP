"""资金流向专用结构化缓存管理器"""
import sqlite3
import pandas as pd
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
from config.settings import CACHE_DB

class MoneyflowCacheManager:
    """资金流向专用结构化缓存管理器"""
    
    def __init__(self, db_path: Path = CACHE_DB):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建结构化数据表"""
        cursor = self.conn.cursor()
        
        # 个股资金流向表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS moneyflow_stock_dc (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                name TEXT,
                pct_change REAL,
                close REAL,
                net_amount REAL,
                net_amount_rate REAL,
                buy_elg_amount REAL,
                buy_elg_amount_rate REAL,
                buy_lg_amount REAL,
                buy_lg_amount_rate REAL,
                buy_md_amount REAL,
                buy_md_amount_rate REAL,
                buy_sm_amount REAL,
                buy_sm_amount_rate REAL,
                updated_at REAL,
                PRIMARY KEY (ts_code, trade_date)
            )
        ''')
        
        # 板块资金流向表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS moneyflow_concept_dc (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                name TEXT,
                content_type TEXT,
                pct_change REAL,
                close REAL,
                net_amount REAL,
                net_amount_rate REAL,
                buy_elg_amount REAL,
                buy_elg_amount_rate REAL,
                buy_lg_amount REAL,
                buy_lg_amount_rate REAL,
                buy_md_amount REAL,
                buy_md_amount_rate REAL,
                buy_sm_amount REAL,
                buy_sm_amount_rate REAL,
                buy_sm_amount_stock TEXT,
                rank INTEGER,
                updated_at REAL,
                PRIMARY KEY (ts_code, trade_date, content_type)
            )
        ''')
        
        # 创建索引以加速查询
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stk_date ON moneyflow_stock_dc(trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cpt_date ON moneyflow_concept_dc(trade_date)')
        
        self.conn.commit()

    def get_stock_data(self, ts_codes: List[str] = None, trade_date: str = "", start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """从结构化表获取个股数据"""
        query = "SELECT * FROM moneyflow_stock_dc WHERE 1=1"
        params = []
        
        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        elif start_date and end_date:
            query += " AND trade_date BETWEEN ? AND ?"
            params.extend([start_date, end_date])
            
        if ts_codes:
            placeholders = ','.join(['?'] * len(ts_codes))
            query += f" AND ts_code IN ({placeholders})"
            params.extend(ts_codes)
            
        return pd.read_sql_query(query, self.conn, params=params)

    def save_stock_data(self, df: pd.DataFrame):
        """保存个股数据到结构化表"""
        if df.empty:
            return
        
        temp_df = df.copy()
        temp_df['updated_at'] = time.time()
        
        # 使用 SQLite UPSERT 逻辑
        cursor = self.conn.cursor()
        for _, row in temp_df.iterrows():
            cols = ', '.join(row.index)
            placeholders = ', '.join(['?'] * len(row))
            update_stmt = ', '.join([f"{c} = excluded.{c}" for c in row.index if c not in ['ts_code', 'trade_date']])
            
            sql = f"""
                INSERT INTO moneyflow_stock_dc ({cols}) 
                VALUES ({placeholders})
                ON CONFLICT(ts_code, trade_date) DO UPDATE SET {update_stmt}
            """
            cursor.execute(sql, tuple(row))
        self.conn.commit()

    def get_concept_data(self, ts_codes: List[str] = None, trade_date: str = "", start_date: str = "", end_date: str = "", content_type: str = "") -> pd.DataFrame:
        """从结构化表获取板块数据"""
        query = "SELECT * FROM moneyflow_concept_dc WHERE 1=1"
        params = []
        
        if trade_date:
            query += " AND trade_date = ?"
            params.append(trade_date)
        elif start_date and end_date:
            query += " AND trade_date BETWEEN ? AND ?"
            params.extend([start_date, end_date])
            
        if ts_codes:
            placeholders = ','.join(['?'] * len(ts_codes))
            query += f" AND ts_code IN ({placeholders})"
            params.extend(ts_codes)
            
        if content_type:
            query += " AND content_type = ?"
            params.append(content_type)
            
        return pd.read_sql_query(query, self.conn, params=params)

    def save_concept_data(self, df: pd.DataFrame):
        """保存板块数据到结构化表"""
        if df.empty:
            return
        
        temp_df = df.copy()
        temp_df['updated_at'] = time.time()
        
        cursor = self.conn.cursor()
        for _, row in temp_df.iterrows():
            cols = ', '.join(row.index)
            placeholders = ', '.join(['?'] * len(row))
            update_stmt = ', '.join([f"{c} = excluded.{c}" for c in row.index if c not in ['ts_code', 'trade_date', 'content_type']])
            
            sql = f"""
                INSERT INTO moneyflow_concept_dc ({cols}) 
                VALUES ({placeholders})
                ON CONFLICT(ts_code, trade_date, content_type) DO UPDATE SET {update_stmt}
            """
            cursor.execute(sql, tuple(row))
        self.conn.commit()

    def check_date_completeness(self, table: str, trade_date: str, expected_min: int = 100) -> bool:
        """检查某天的数据是否大致完整（防止只有部分缓存时误认为全量）"""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE trade_date = ?", (trade_date,))
        count = cursor.fetchone()[0]
        return count >= expected_min

# 单例模式
moneyflow_cache = MoneyflowCacheManager()
