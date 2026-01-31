"""国际指数数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from config.settings import CACHE_DB


class IndexCacheManager:
    """国际指数数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储指数数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 主键：(ts_code, trade_date)，确保同一指数的同一天只有一条数据
    4. 支持按指数代码、交易日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化指数缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建指数数据表"""
        cursor = self.conn.cursor()
        
        # 创建指数数据表
        # 根据 Tushare index_global 接口的数据格式设计
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_index_data (
                ts_code TEXT NOT NULL,           -- TS指数代码（如：XIN9、HSI、DJI等）
                trade_date TEXT NOT NULL,        -- 交易日期（YYYYMMDD格式）
                open REAL,                       -- 开盘点位
                close REAL,                      -- 收盘点位
                high REAL,                       -- 最高点位
                low REAL,                        -- 最低点位
                pre_close REAL,                  -- 昨日收盘点
                change REAL,                     -- 涨跌点位
                pct_chg REAL,                    -- 涨跌幅（百分比）
                swing REAL,                      -- 振幅（百分比）
                vol REAL,                       -- 成交量（大部分无此项数据）
                amount REAL,                    -- 成交额（大部分无此项数据）
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, trade_date)
            )
        ''')
        
        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_ts_code ON global_index_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_trade_date ON global_index_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_ts_code_date ON global_index_data(ts_code, trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_created_at ON global_index_data(created_at)
        ''')
        
        self.conn.commit()
    
    def save_index_data(self, df: pd.DataFrame) -> int:
        """
        保存指数数据到数据库
        
        参数:
            df: 包含指数数据的DataFrame，必须包含 ts_code 和 trade_date 列
        
        返回:
            保存的记录数量
        """
        if df.empty:
            return 0
        
        # 确保必要的列存在
        required_columns = ['ts_code', 'trade_date']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame必须包含 '{col}' 列")
        
        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0
        
        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                # 使用 INSERT OR REPLACE 确保数据唯一性（基于主键）
                cursor.execute('''
                    INSERT OR REPLACE INTO global_index_data (
                        ts_code, trade_date, open, close, high, low,
                        pre_close, change, pct_chg, swing, vol, amount, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('trade_date', '')),
                    row.get('open') if pd.notna(row.get('open')) else None,
                    row.get('close') if pd.notna(row.get('close')) else None,
                    row.get('high') if pd.notna(row.get('high')) else None,
                    row.get('low') if pd.notna(row.get('low')) else None,
                    row.get('pre_close') if pd.notna(row.get('pre_close')) else None,
                    row.get('change') if pd.notna(row.get('change')) else None,
                    row.get('pct_chg') if pd.notna(row.get('pct_chg')) else None,
                    row.get('swing') if pd.notna(row.get('swing')) else None,
                    row.get('vol') if pd.notna(row.get('vol')) else None,
                    row.get('amount') if pd.notna(row.get('amount')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存指数数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_index_data(
        self,
        ts_code: str,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询指数数据
        
        参数:
            ts_code: 指数代码（如：XIN9、HSI、DJI等）
            trade_date: 特定交易日期（YYYYMMDD格式），如果提供则只查询该日期
            start_date: 开始日期（YYYYMMDD格式），与end_date配合使用
            end_date: 结束日期（YYYYMMDD格式），与start_date配合使用
            limit: 限制返回的记录数
            order_by: 排序方式，'DESC'（降序，最新的在前）或 'ASC'（升序）
        
        返回:
            DataFrame，如果未找到数据则返回None
        """
        cursor = self.conn.cursor()
        
        # 构建查询条件
        conditions = ['ts_code = ?']
        params = [ts_code]
        
        if trade_date:
            conditions.append('trade_date = ?')
            params.append(trade_date)
        elif start_date or end_date:
            if start_date:
                conditions.append('trade_date >= ?')
                params.append(start_date)
            if end_date:
                conditions.append('trade_date <= ?')
                params.append(end_date)
        
        where_clause = ' AND '.join(conditions)
        order_clause = f'ORDER BY trade_date {order_by}'
        limit_clause = f'LIMIT {limit}' if limit else ''
        
        query = f'''
            SELECT 
                ts_code, trade_date, open, close, high, low,
                pre_close, change, pct_chg, swing, vol, amount, created_at
            FROM global_index_data
            WHERE {where_clause}
            {order_clause}
            {limit_clause}
        '''
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if not rows:
            return None
        
        # 转换为DataFrame
        columns = [
            'ts_code', 'trade_date', 'open', 'close', 'high', 'low',
            'pre_close', 'change', 'pct_chg', 'swing', 'vol', 'amount', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = ['open', 'close', 'high', 'low', 'pre_close', 
                          'change', 'pct_chg', 'swing', 'vol', 'amount', 'created_at']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        """
        检查是否存在指定指数的数据
        
        参数:
            ts_code: 指数代码
            trade_date: 交易日期（可选），如果提供则检查特定日期
        
        返回:
            如果存在数据返回True，否则返回False
        """
        cursor = self.conn.cursor()
        
        if trade_date:
            cursor.execute('''
                SELECT COUNT(*) FROM global_index_data
                WHERE ts_code = ? AND trade_date = ?
            ''', (ts_code, trade_date))
        else:
            cursor.execute('''
                SELECT COUNT(*) FROM global_index_data
                WHERE ts_code = ?
            ''', (ts_code,))
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        """
        获取指定指数的数据日期范围
        
        参数:
            ts_code: 指数代码
        
        返回:
            包含 'start_date' 和 'end_date' 的字典，如果无数据则返回None
        """
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT MIN(trade_date) as start_date, MAX(trade_date) as end_date
            FROM global_index_data
            WHERE ts_code = ?
        ''', (ts_code,))
        
        row = cursor.fetchone()
        if row and row[0] and row[1]:
            return {
                'start_date': row[0],
                'end_date': row[1]
            }
        return None
    
    def get_stats(self) -> Dict:
        """获取指数数据统计信息"""
        cursor = self.conn.cursor()
        
        # 按指数代码统计
        cursor.execute('''
            SELECT 
                ts_code,
                COUNT(*) as record_count,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM global_index_data
            GROUP BY ts_code
            ORDER BY ts_code
        ''')
        
        stats = {
            'by_index': {}
        }
        
        for row in cursor.fetchall():
            ts_code, count, earliest, latest = row
            stats['by_index'][ts_code] = {
                'record_count': count,
                'earliest_date': earliest,
                'latest_date': latest
            }
        
        # 总统计
        cursor.execute('SELECT COUNT(*) FROM global_index_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM global_index_data')
        index_count = cursor.fetchone()[0]
        
        stats['total'] = {
            'total_records': total_count,
            'index_count': index_count
        }
        
        return stats
    
    def clear_index_data(self, ts_code: Optional[str] = None) -> int:
        """
        清理指数数据
        
        参数:
            ts_code: 如果指定，只清理该指数的数据；否则清理所有数据
        
        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        
        if ts_code:
            cursor.execute('DELETE FROM global_index_data WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM global_index_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局指数缓存管理器实例
index_cache_manager = IndexCacheManager()

