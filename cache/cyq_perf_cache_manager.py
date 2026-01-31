"""每日筹码及胜率数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB


class CyqPerfCacheManager:
    """每日筹码及胜率数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储每日筹码及胜率数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 主键：(ts_code, trade_date)，确保同一股票的同一天只有一条数据
    4. 支持按股票代码、交易日期、日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化每日筹码及胜率缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建每日筹码及胜率数据表"""
        cursor = self.conn.cursor()
        
        # 创建每日筹码及胜率数据表
        # 根据 Tushare cyq_perf 接口的数据格式设计
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cyq_perf_data (
                ts_code TEXT NOT NULL,          -- 股票代码（如：600000.SH）
                trade_date TEXT NOT NULL,       -- 交易日期（YYYYMMDD格式）
                his_low REAL,                   -- 历史最低价
                his_high REAL,                  -- 历史最高价
                cost_5pct REAL,                 -- 5分位成本
                cost_15pct REAL,                -- 15分位成本
                cost_50pct REAL,                -- 50分位成本
                cost_85pct REAL,                -- 85分位成本
                cost_95pct REAL,                -- 95分位成本
                weight_avg REAL,                -- 加权平均成本
                winner_rate REAL,               -- 胜率
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, trade_date)
            )
        ''')
        
        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_cyq_perf_ts_code ON cyq_perf_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_cyq_perf_trade_date ON cyq_perf_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_cyq_perf_ts_code_date ON cyq_perf_data(ts_code, trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_cyq_perf_created_at ON cyq_perf_data(created_at)
        ''')
        
        self.conn.commit()
    
    def save_cyq_perf_data(self, df: pd.DataFrame) -> int:
        """
        保存每日筹码及胜率数据到数据库
        
        参数:
            df: 包含每日筹码及胜率数据的DataFrame，必须包含 ts_code 和 trade_date 列
        
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
                    INSERT OR REPLACE INTO cyq_perf_data (
                        ts_code, trade_date, his_low, his_high, cost_5pct,
                        cost_15pct, cost_50pct, cost_85pct, cost_95pct,
                        weight_avg, winner_rate, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('trade_date', '')),
                    row.get('his_low') if pd.notna(row.get('his_low')) else None,
                    row.get('his_high') if pd.notna(row.get('his_high')) else None,
                    row.get('cost_5pct') if pd.notna(row.get('cost_5pct')) else None,
                    row.get('cost_15pct') if pd.notna(row.get('cost_15pct')) else None,
                    row.get('cost_50pct') if pd.notna(row.get('cost_50pct')) else None,
                    row.get('cost_85pct') if pd.notna(row.get('cost_85pct')) else None,
                    row.get('cost_95pct') if pd.notna(row.get('cost_95pct')) else None,
                    row.get('weight_avg') if pd.notna(row.get('weight_avg')) else None,
                    row.get('winner_rate') if pd.notna(row.get('winner_rate')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存每日筹码及胜率数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_cyq_perf_data(
        self,
        ts_code: str,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询每日筹码及胜率数据
        
        参数:
            ts_code: 股票代码（如：600000.SH）
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
                ts_code, trade_date, his_low, his_high, cost_5pct,
                cost_15pct, cost_50pct, cost_85pct, cost_95pct,
                weight_avg, winner_rate, created_at
            FROM cyq_perf_data
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
            'ts_code', 'trade_date', 'his_low', 'his_high', 'cost_5pct',
            'cost_15pct', 'cost_50pct', 'cost_85pct', 'cost_95pct',
            'weight_avg', 'winner_rate', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = ['his_low', 'his_high', 'cost_5pct', 'cost_15pct', 
                          'cost_50pct', 'cost_85pct', 'cost_95pct', 
                          'weight_avg', 'winner_rate', 'created_at']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def is_cache_data_complete(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> bool:
        """
        检查缓存中的数据是否完整覆盖了请求的日期范围
        
        参数:
            ts_code: 股票代码
            start_date: 请求的开始日期（YYYYMMDD格式）
            end_date: 请求的结束日期（YYYYMMDD格式）
        
        返回:
            如果数据完整返回True，否则返回False
        """
        if not start_date and not end_date:
            # 如果没有日期范围要求，认为数据完整
            return True
        
        # 获取该股票在缓存中的日期范围
        date_range = self.get_date_range(ts_code)
        if date_range is None:
            # 缓存中没有该股票的数据
            return False
        
        cache_min_date = date_range['start_date']
        cache_max_date = date_range['end_date']
        
        # 检查缓存数据是否完全覆盖请求范围
        if start_date and end_date:
            # 缓存的最小日期应该 <= 请求的开始日期，最大日期应该 >= 请求的结束日期
            if cache_min_date > start_date or cache_max_date < end_date:
                return False
        elif start_date:
            # 检查缓存中是否有从start_date开始的数据
            if cache_min_date > start_date:
                return False
        elif end_date:
            # 检查缓存中是否有到end_date结束的数据
            if cache_max_date < end_date:
                return False
        
        return True
    
    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        """
        检查是否存在指定股票的数据
        
        参数:
            ts_code: 股票代码
            trade_date: 交易日期（可选），如果提供则检查特定日期
        
        返回:
            如果存在数据返回True，否则返回False
        """
        cursor = self.conn.cursor()
        
        if trade_date:
            cursor.execute('''
                SELECT COUNT(*) FROM cyq_perf_data
                WHERE ts_code = ? AND trade_date = ?
            ''', (ts_code, trade_date))
        else:
            cursor.execute('''
                SELECT COUNT(*) FROM cyq_perf_data
                WHERE ts_code = ?
            ''', (ts_code,))
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        """
        获取指定股票的数据日期范围
        
        参数:
            ts_code: 股票代码
        
        返回:
            包含 'start_date' 和 'end_date' 的字典，如果无数据则返回None
        """
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT MIN(trade_date) as start_date, MAX(trade_date) as end_date
            FROM cyq_perf_data
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
        """获取每日筹码及胜率数据统计信息"""
        cursor = self.conn.cursor()
        
        # 按股票代码统计
        cursor.execute('''
            SELECT 
                ts_code,
                COUNT(*) as record_count,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM cyq_perf_data
            GROUP BY ts_code
            ORDER BY ts_code
        ''')
        
        stats = {
            'by_stock': {}
        }
        
        for row in cursor.fetchall():
            ts_code, count, earliest, latest = row
            stats['by_stock'][ts_code] = {
                'record_count': count,
                'earliest_date': earliest,
                'latest_date': latest
            }
        
        # 总统计
        cursor.execute('SELECT COUNT(*) FROM cyq_perf_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM cyq_perf_data')
        stock_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT trade_date) FROM cyq_perf_data')
        date_count = cursor.fetchone()[0]
        
        stats['total'] = {
            'total_records': total_count,
            'stock_count': stock_count,
            'date_count': date_count
        }
        
        return stats
    
    def clear_cyq_perf_data(self, ts_code: Optional[str] = None) -> int:
        """
        清理每日筹码及胜率数据
        
        参数:
            ts_code: 如果指定，只清理该股票的数据；否则清理所有数据
        
        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        
        if ts_code:
            cursor.execute('DELETE FROM cyq_perf_data WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM cyq_perf_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局每日筹码及胜率缓存管理器实例
cyq_perf_cache_manager = CyqPerfCacheManager()

