"""每日指标数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB


class DailyBasicCacheManager:
    """每日指标数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储每日指标数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 主键：(ts_code, trade_date)，确保同一股票的同一天只有一条数据
    4. 支持按股票代码、交易日期、日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化每日指标缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建每日指标数据表"""
        cursor = self.conn.cursor()
        
        # 创建每日指标数据表
        # 根据 Tushare daily_basic 接口的数据格式设计
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_basic_data (
                ts_code TEXT NOT NULL,          -- 股票代码（如：600230.SH）
                trade_date TEXT NOT NULL,       -- 交易日期（YYYYMMDD格式）
                close REAL,                     -- 当日收盘价
                turnover_rate REAL,             -- 换手率（%）
                turnover_rate_f REAL,          -- 换手率（自由流通股）
                volume_ratio REAL,              -- 量比
                pe REAL,                        -- 市盈率（总市值/净利润）
                pe_ttm REAL,                    -- 市盈率（TTM）
                pb REAL,                        -- 市净率（总市值/净资产）
                ps REAL,                        -- 市销率
                ps_ttm REAL,                    -- 市销率（TTM）
                dv_ratio REAL,                  -- 股息率（%）
                dv_ttm REAL,                   -- 股息率（TTM）（%）
                total_share REAL,               -- 总股本（万股）
                float_share REAL,               -- 流通股本（万股）
                free_share REAL,                -- 自由流通股本（万）
                total_mv REAL,                  -- 总市值（万元）
                circ_mv REAL,                   -- 流通市值（万元）
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, trade_date)
            )
        ''')
        
        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_basic_ts_code ON daily_basic_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_basic_trade_date ON daily_basic_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_basic_ts_code_date ON daily_basic_data(ts_code, trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_basic_created_at ON daily_basic_data(created_at)
        ''')
        
        self.conn.commit()
    
    def save_daily_basic_data(self, df: pd.DataFrame) -> int:
        """
        保存每日指标数据到数据库
        
        参数:
            df: 包含每日指标数据的DataFrame，必须包含 ts_code 和 trade_date 列
        
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
                    INSERT OR REPLACE INTO daily_basic_data (
                        ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                        volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
                        dv_ratio, dv_ttm, total_share, float_share, free_share,
                        total_mv, circ_mv, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('trade_date', '')),
                    row.get('close') if pd.notna(row.get('close')) else None,
                    row.get('turnover_rate') if pd.notna(row.get('turnover_rate')) else None,
                    row.get('turnover_rate_f') if pd.notna(row.get('turnover_rate_f')) else None,
                    row.get('volume_ratio') if pd.notna(row.get('volume_ratio')) else None,
                    row.get('pe') if pd.notna(row.get('pe')) else None,
                    row.get('pe_ttm') if pd.notna(row.get('pe_ttm')) else None,
                    row.get('pb') if pd.notna(row.get('pb')) else None,
                    row.get('ps') if pd.notna(row.get('ps')) else None,
                    row.get('ps_ttm') if pd.notna(row.get('ps_ttm')) else None,
                    row.get('dv_ratio') if pd.notna(row.get('dv_ratio')) else None,
                    row.get('dv_ttm') if pd.notna(row.get('dv_ttm')) else None,
                    row.get('total_share') if pd.notna(row.get('total_share')) else None,
                    row.get('float_share') if pd.notna(row.get('float_share')) else None,
                    row.get('free_share') if pd.notna(row.get('free_share')) else None,
                    row.get('total_mv') if pd.notna(row.get('total_mv')) else None,
                    row.get('circ_mv') if pd.notna(row.get('circ_mv')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存每日指标数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_daily_basic_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询每日指标数据
        
        参数:
            ts_code: 股票代码（如：600230.SH，支持多个股票，逗号分隔）
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
        conditions = []
        params = []
        
        if ts_code:
            # 支持多个股票代码（逗号分隔）
            codes = [code.strip() for code in ts_code.split(',')]
            if len(codes) == 1:
                conditions.append('ts_code = ?')
                params.append(codes[0])
            else:
                # 多个股票代码，使用 IN 子句
                placeholders = ','.join(['?'] * len(codes))
                conditions.append(f'ts_code IN ({placeholders})')
                params.extend(codes)
        
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
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        order_clause = f'ORDER BY trade_date {order_by}'
        limit_clause = f'LIMIT {limit}' if limit else ''
        
        query = f'''
            SELECT 
                ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
                dv_ratio, dv_ttm, total_share, float_share, free_share,
                total_mv, circ_mv, created_at
            FROM daily_basic_data
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
            'ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f',
            'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
            'dv_ratio', 'dv_ttm', 'total_share', 'float_share', 'free_share',
            'total_mv', 'circ_mv', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = ['close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio',
                          'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',
                          'total_share', 'float_share', 'free_share', 'total_mv', 
                          'circ_mv', 'created_at']
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
            ts_code: 股票代码（支持多个，逗号分隔）
            start_date: 请求的开始日期（YYYYMMDD格式）
            end_date: 请求的结束日期（YYYYMMDD格式）
        
        返回:
            如果数据完整返回True，否则返回False
        """
        if not start_date and not end_date:
            # 如果没有日期范围要求，认为数据完整
            return True
        
        # 获取每个股票的数据日期范围
        codes = [code.strip() for code in ts_code.split(',')]
        
        for code in codes:
            # 获取该股票在缓存中的日期范围
            date_range = self.get_date_range(code)
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
    
    def has_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None) -> bool:
        """
        检查是否存在指定股票的数据
        
        参数:
            ts_code: 股票代码（可选），如果提供则检查特定股票
            trade_date: 交易日期（可选），如果提供则检查特定日期
        
        返回:
            如果存在数据返回True，否则返回False
        """
        cursor = self.conn.cursor()
        
        conditions = []
        params = []
        
        if ts_code:
            codes = [code.strip() for code in ts_code.split(',')]
            if len(codes) == 1:
                conditions.append('ts_code = ?')
                params.append(codes[0])
            else:
                placeholders = ','.join(['?'] * len(codes))
                conditions.append(f'ts_code IN ({placeholders})')
                params.extend(codes)
        
        if trade_date:
            conditions.append('trade_date = ?')
            params.append(trade_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        
        cursor.execute(f'''
            SELECT COUNT(*) FROM daily_basic_data
            WHERE {where_clause}
        ''', params)
        
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
            FROM daily_basic_data
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
        """获取每日指标数据统计信息"""
        cursor = self.conn.cursor()
        
        # 按股票代码统计
        cursor.execute('''
            SELECT 
                ts_code,
                COUNT(*) as record_count,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM daily_basic_data
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
        cursor.execute('SELECT COUNT(*) FROM daily_basic_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM daily_basic_data')
        stock_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT trade_date) FROM daily_basic_data')
        date_count = cursor.fetchone()[0]
        
        stats['total'] = {
            'total_records': total_count,
            'stock_count': stock_count,
            'date_count': date_count
        }
        
        return stats
    
    def clear_daily_basic_data(self, ts_code: Optional[str] = None) -> int:
        """
        清理每日指标数据
        
        参数:
            ts_code: 如果指定，只清理该股票的数据；否则清理所有数据
        
        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        
        if ts_code:
            cursor.execute('DELETE FROM daily_basic_data WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM daily_basic_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局每日指标缓存管理器实例
daily_basic_cache_manager = DailyBasicCacheManager()

