"""板块实时强度统计数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from config.settings import CACHE_DB


class SectorStrengthCacheManager:
    """板块强度统计专用缓存管理器
    
    特点：
    1. 存储全市场所有板块在不同时刻的评分快照
    2. 主键：(sector_name, trade_date, trade_time)，记录分时强弱演化
    3. 支持按板块类型 (sw_l2, em_industry, em_concept) 分别存储
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化板块强度缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        # self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建板块强度评分数据表"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sector_strength_data (
                sector_name TEXT NOT NULL,       -- 板块名称
                sector_type TEXT NOT NULL,       -- 板块类型 (sw_l2/em_industry/em_concept)
                trade_date TEXT NOT NULL,        -- 交易日期 (YYYYMMDD)
                trade_time TEXT NOT NULL,        -- 交易时间 (HH:MM:SS)
                -- 广度维度
                avg_pct REAL,                    -- 板块平均涨幅
                rising_ratio REAL,               -- 上涨家数占比
                median_return REAL,              -- 涨幅中位数
                -- 质量维度
                intraday_position REAL,          -- 日内位阶
                real_body_ratio REAL,            -- 真阳线比例
                -- 量能维度
                volume_ratio REAL,               -- 实时量比
                value_per_trade REAL,            -- 单笔成交金额(元)
                -- 盘口维度
                order_imbalance REAL,            -- 委比
                -- 基础信息
                stock_count INTEGER,             -- 成分股总数
                score REAL,                      -- 综合评分
                created_at REAL NOT NULL,        -- 记录创建时间戳
                PRIMARY KEY (sector_name, trade_date, trade_time)
            )
        ''')
        
        # 尝试添加新列（针对旧表兼容性）
        new_columns = [
            ('median_return', 'REAL'),
            ('intraday_position', 'REAL'),
            ('real_body_ratio', 'REAL'),
            ('value_per_trade', 'REAL'),
            ('order_imbalance', 'REAL')
        ]
        for col_name, col_type in new_columns:
            try:
                cursor.execute(f'ALTER TABLE sector_strength_data ADD COLUMN {col_name} {col_type}')
            except:
                pass
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector_date_time ON sector_strength_data(sector_name, trade_date, trade_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector_type_date ON sector_strength_data(sector_type, trade_date)')
        
        self.conn.commit()
    
    def save_strength_snapshots(self, df: pd.DataFrame, sector_type: str, trade_date: str, trade_time: str) -> int:
        """
        保存多个板块的强度快照
        
        参数:
            df: 包含评分结果的DataFrame
            sector_type: 'sw_l2', 'em_industry', 'em_concept'
            trade_date: YYYYMMDD
            trade_time: HH:MM:SS
        """
        if df.empty:
            return 0
            
        cursor = self.conn.cursor()
        now = time.time()
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                # 兼容不同来源的列名映射
                name = row.get('板块名称')
                avg_pct = float(str(row.get('平均涨幅', '0')).replace('%', '')) 
                rising_ratio = float(str(row.get('上涨家数占比', '0')).replace('%', ''))
                median_return = float(row.get('涨幅中位数', 0))
                
                intraday_position = float(row.get('日内位阶', 0.5))
                real_body_ratio = float(str(row.get('真阳线比例', '0')).replace('%', ''))
                
                vr = float(row.get('实时量比', row.get('放量系数', 1.0)))
                value_per_trade = float(row.get('单笔成交额', 0))
                
                order_imbalance = row.get('委比')
                if order_imbalance is not None:
                    order_imbalance = float(str(order_imbalance).replace('%', ''))
                
                count = int(row.get('成分股数', row.get('stock_count', 0)))
                score = float(row.get('score', 0))
                
                cursor.execute('''
                    INSERT OR REPLACE INTO sector_strength_data (
                        sector_name, sector_type, trade_date, trade_time, 
                        avg_pct, rising_ratio, median_return,
                        intraday_position, real_body_ratio,
                        volume_ratio, value_per_trade, order_imbalance,
                        stock_count, score, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    name,
                    sector_type,
                    trade_date,
                    trade_time,
                    avg_pct,
                    rising_ratio,
                    median_return,
                    intraday_position,
                    real_body_ratio,
                    vr,
                    value_per_trade,
                    order_imbalance,
                    count,
                    score,
                    now
                ))
                saved_count += 1
            except Exception as e:
                print(f"Error saving snapshot: {e}")
                continue
                
        self.conn.commit()
        return saved_count

    def get_strength_history(self, sector_name: str, trade_date: str) -> pd.DataFrame:
        """
        获取指定板块在特定日期的历史强度序列
        
        参数:
            sector_name: 板块名称
            trade_date: 交易日期 (YYYYMMDD)
            
        返回:
            包含历史数据的DataFrame
        """
        query = '''
            SELECT trade_time, avg_pct, rising_ratio, median_return,
                   intraday_position, real_body_ratio,
                   volume_ratio, value_per_trade, order_imbalance,
                   score, stock_count
            FROM sector_strength_data
            WHERE sector_name = ? AND trade_date = ?
            ORDER BY trade_time ASC
        '''
        return pd.read_sql_query(query, self.conn, params=(sector_name, trade_date))

    def close(self):
        if self.conn:
            self.conn.close()



# 全局实例
sector_strength_cache_manager = SectorStrengthCacheManager()
