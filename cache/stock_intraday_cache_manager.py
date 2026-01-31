"""股票分时快照数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from config.settings import CACHE_DB


class StockIntradayCacheManager:
    """股票分时快照数据专用缓存管理器
    
    特点：
    1. 存储股票盘中每隔5分钟的快照数据
    2. 主键：(ts_code, trade_date, trade_time)，记录绝对时间序列
    3. 用于量比计算（对比昨日同一时刻累计成交量）
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化分时快照缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建分时快照数据表"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_intraday_data (
                ts_code TEXT NOT NULL,           -- TS股票代码
                trade_date TEXT NOT NULL,        -- 交易日期 (YYYYMMDD)
                trade_time TEXT NOT NULL,        -- 交易时间 (HH:MM:SS)
                open REAL,                        -- 开盘价
                close REAL,                       -- 当前价格
                high REAL,                        -- 最高价
                low REAL,                         -- 最低价
                vol REAL,                         -- 累计成交量(手)
                amount REAL,                      -- 累计成交额(千元)
                num INTEGER,                      -- 累计成交笔数
                bid_price1 REAL,                 -- 买一价
                bid_volume1 REAL,                -- 买一量
                ask_price1 REAL,                 -- 卖一价
                ask_volume1 REAL,                -- 卖一量
                created_at REAL NOT NULL,        -- 记录创建时间戳
                PRIMARY KEY (ts_code, trade_date, trade_time)
            )
        ''')
        
        # 增加字段 (针对旧表)
        for col_name, col_type in [
            ('open', 'REAL'),
            ('high', 'REAL'),
            ('low', 'REAL'),
            ('num', 'INTEGER'),
            ('bid_price1', 'REAL'),
            ('bid_volume1', 'REAL'),
            ('ask_price1', 'REAL'),
            ('ask_volume1', 'REAL')
        ]:
            try:
                cursor.execute(f"ALTER TABLE stock_intraday_data ADD COLUMN {col_name} {col_type}")
            except:
                pass
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intraday_ts_date ON stock_intraday_data(ts_code, trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intraday_time ON stock_intraday_data(trade_time)')
        
        self.conn.commit()
    
    def save_intraday_snapshot(self, df: pd.DataFrame, current_time_str: str = None) -> int:
        """
        保存分时快照数据
        
        参数:
            df: 包含日线快照的DataFrame
            current_time_str: 显式指定时间 (HH:MM:SS)，若不传则从当前系统时间获取
        """
        if df.empty:
            return 0
            
        if not current_time_str:
            current_time_str = time.strftime("%H:%M:%S")
            
        cursor = self.conn.cursor()
        now = time.time()
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                # 获取日期：优先用 trade_date 字段，没有则用今日
                trade_date = str(row.get('trade_date', time.strftime("%Y%m%d")))
                
                cursor.execute('''
                    INSERT OR IGNORE INTO stock_intraday_data (
                        ts_code, trade_date, trade_time, open, close, high, low,
                        vol, amount, num, bid_price1, bid_volume1, ask_price1, ask_volume1, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    trade_date,
                    current_time_str,
                    row.get('open') if pd.notna(row.get('open')) else None,
                    row.get('close') if pd.notna(row.get('close')) else None,
                    row.get('high') if pd.notna(row.get('high')) else None,
                    row.get('low') if pd.notna(row.get('low')) else None,
                    row.get('vol') if pd.notna(row.get('vol')) else None,
                    row.get('amount') if pd.notna(row.get('amount')) else None,
                    int(row.get('num')) if pd.notna(row.get('num')) else None,
                    row.get('bid_price1') if pd.notna(row.get('bid_price1')) else None,
                    row.get('bid_volume1') if pd.notna(row.get('bid_volume1')) else None,
                    row.get('ask_price1') if pd.notna(row.get('ask_price1')) else None,
                    row.get('ask_volume1') if pd.notna(row.get('ask_volume1')) else None,
                    now
                ))
                saved_count += 1
            except Exception as e:
                continue
                
        self.conn.commit()
        return saved_count

    def get_historical_snapshot(self, ts_code: str, trade_date: str, trade_time: str) -> Optional[Dict]:
        """查询特定日期、特定时刻最接近的快照"""
        cursor = self.conn.cursor()
        # 查找小于等于请求时刻的最晚的一条记录
        cursor.execute('''
            SELECT * FROM stock_intraday_data 
            WHERE ts_code = ? AND trade_date = ? AND trade_time <= ?
            ORDER BY trade_time DESC LIMIT 1
        ''', (ts_code, trade_date, trade_time))
        
        row = cursor.fetchone()
        if not row:
            return None
            
        columns = [column[0] for column in cursor.description]
        return dict(zip(columns, row))

    def get_all_snapshots_for_time(self, trade_date: str, trade_time: str) -> pd.DataFrame:
        """
        获取指定日期和时刻的所有股票快照数据（用于历史模拟）
        
        参数:
            trade_date: 交易日期 (YYYYMMDD)
            trade_time: 交易时间 (HH:MM:SS)
        
        返回:
            DataFrame: 包含所有股票在该时刻最接近的快照数据
        """
        # 使用子查询获取每只股票在该时刻之前最近的一条记录
        query = '''
            SELECT s1.* FROM stock_intraday_data s1
            INNER JOIN (
                SELECT ts_code, MAX(trade_time) as max_time
                FROM stock_intraday_data
                WHERE trade_date = ? AND trade_time <= ?
                GROUP BY ts_code
            ) s2 ON s1.ts_code = s2.ts_code AND s1.trade_time = s2.max_time
            WHERE s1.trade_date = ?
        '''
        df = pd.read_sql_query(query, self.conn, params=(trade_date, trade_time, trade_date))
        return df

    def close(self):
        if self.conn:
            self.conn.close()


# 全局实例
stock_intraday_cache_manager = StockIntradayCacheManager()
