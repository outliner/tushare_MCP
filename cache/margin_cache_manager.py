"""融资融券交易汇总数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB


class MarginCacheManager:
    """融资融券交易汇总数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储融资融券交易汇总数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 主键：(trade_date, exchange_id)，确保同一交易所的同一天只有一条数据
    4. 支持按交易日期、交易所、日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化融资融券缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建融资融券交易汇总数据表"""
        cursor = self.conn.cursor()
        
        # 创建融资融券交易汇总数据表
        # 根据 Tushare margin 接口的数据格式设计
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS margin_data (
                trade_date TEXT NOT NULL,        -- 交易日期（YYYYMMDD格式）
                exchange_id TEXT NOT NULL,      -- 交易所代码（SSE上交所SZSE深交所BSE北交所）
                rzye REAL,                      -- 融资余额(元)
                rzmre REAL,                     -- 融资买入额(元)
                rzche REAL,                     -- 融资偿还额(元)
                rqye REAL,                      -- 融券余额(元)
                rqmcl REAL,                     -- 融券卖出量(股,份,手)
                rzrqye REAL,                    -- 融资融券余额(元)
                rqyl REAL,                      -- 融券余量(股,份,手)
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (trade_date, exchange_id)
            )
        ''')
        
        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_margin_trade_date ON margin_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_margin_exchange_id ON margin_data(exchange_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_margin_trade_date_exchange ON margin_data(trade_date, exchange_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_margin_created_at ON margin_data(created_at)
        ''')
        
        self.conn.commit()
    
    def save_margin_data(self, df: pd.DataFrame) -> int:
        """
        保存融资融券交易汇总数据到数据库
        
        参数:
            df: 包含融资融券交易汇总数据的DataFrame，必须包含 trade_date 和 exchange_id 列
        
        返回:
            保存的记录数量
        """
        if df.empty:
            return 0
        
        # 确保必要的列存在
        required_columns = ['trade_date', 'exchange_id']
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
                    INSERT OR REPLACE INTO margin_data (
                        trade_date, exchange_id, rzye, rzmre, rzche,
                        rqye, rqmcl, rzrqye, rqyl, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('trade_date', '')),
                    str(row.get('exchange_id', '')),
                    row.get('rzye') if pd.notna(row.get('rzye')) else None,
                    row.get('rzmre') if pd.notna(row.get('rzmre')) else None,
                    row.get('rzche') if pd.notna(row.get('rzche')) else None,
                    row.get('rqye') if pd.notna(row.get('rqye')) else None,
                    row.get('rqmcl') if pd.notna(row.get('rqmcl')) else None,
                    row.get('rzrqye') if pd.notna(row.get('rzrqye')) else None,
                    row.get('rqyl') if pd.notna(row.get('rqyl')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存融资融券数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_margin_data(
        self,
        trade_date: Optional[str] = None,
        exchange_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询融资融券交易汇总数据
        
        参数:
            trade_date: 特定交易日期（YYYYMMDD格式），如果提供则只查询该日期
            exchange_id: 交易所代码（SSE上交所SZSE深交所BSE北交所），如果提供则只查询该交易所
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
        
        if exchange_id:
            conditions.append('exchange_id = ?')
            params.append(exchange_id)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        order_clause = f'ORDER BY trade_date {order_by}'
        limit_clause = f'LIMIT {limit}' if limit else ''
        
        query = f'''
            SELECT 
                trade_date, exchange_id, rzye, rzmre, rzche,
                rqye, rqmcl, rzrqye, rqyl, created_at
            FROM margin_data
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
            'trade_date', 'exchange_id', 'rzye', 'rzmre', 'rzche',
            'rqye', 'rqmcl', 'rzrqye', 'rqyl', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = ['rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 
                          'rzrqye', 'rqyl', 'created_at']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def is_cache_data_complete(
        self,
        exchange_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> bool:
        """
        检查缓存中的数据是否完整覆盖了请求的日期范围
        
        参数:
            exchange_id: 交易所代码（可选）
            start_date: 请求的开始日期（YYYYMMDD格式）
            end_date: 请求的结束日期（YYYYMMDD格式）
        
        返回:
            如果数据完整返回True，否则返回False
        """
        if not start_date and not end_date:
            # 如果没有日期范围要求，认为数据完整
            return True
        
        # 获取数据日期范围
        date_range = self.get_date_range(exchange_id)
        if date_range is None:
            # 缓存中没有数据
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
    
    def has_data(self, trade_date: Optional[str] = None, exchange_id: Optional[str] = None) -> bool:
        """
        检查是否存在指定日期的数据
        
        参数:
            trade_date: 交易日期（可选），如果提供则检查特定日期
            exchange_id: 交易所代码（可选），如果提供则检查特定交易所
        
        返回:
            如果存在数据返回True，否则返回False
        """
        cursor = self.conn.cursor()
        
        conditions = []
        params = []
        
        if trade_date:
            conditions.append('trade_date = ?')
            params.append(trade_date)
        
        if exchange_id:
            conditions.append('exchange_id = ?')
            params.append(exchange_id)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        
        cursor.execute(f'''
            SELECT COUNT(*) FROM margin_data
            WHERE {where_clause}
        ''', params)
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def get_date_range(self, exchange_id: Optional[str] = None) -> Optional[Dict[str, str]]:
        """
        获取数据的日期范围
        
        参数:
            exchange_id: 交易所代码（可选），如果提供则只查询该交易所
        
        返回:
            包含 'start_date' 和 'end_date' 的字典，如果无数据则返回None
        """
        cursor = self.conn.cursor()
        
        if exchange_id:
            cursor.execute('''
                SELECT MIN(trade_date) as start_date, MAX(trade_date) as end_date
                FROM margin_data
                WHERE exchange_id = ?
            ''', (exchange_id,))
        else:
            cursor.execute('''
                SELECT MIN(trade_date) as start_date, MAX(trade_date) as end_date
                FROM margin_data
            ''')
        
        row = cursor.fetchone()
        if row and row[0] and row[1]:
            return {
                'start_date': row[0],
                'end_date': row[1]
            }
        return None
    
    def get_stats(self) -> Dict:
        """获取融资融券数据统计信息"""
        cursor = self.conn.cursor()
        
        # 按交易所统计
        cursor.execute('''
            SELECT 
                exchange_id,
                COUNT(*) as record_count,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM margin_data
            GROUP BY exchange_id
            ORDER BY exchange_id
        ''')
        
        stats = {
            'by_exchange': {}
        }
        
        for row in cursor.fetchall():
            exchange_id, count, earliest, latest = row
            stats['by_exchange'][exchange_id] = {
                'record_count': count,
                'earliest_date': earliest,
                'latest_date': latest
            }
        
        # 总统计
        cursor.execute('SELECT COUNT(*) FROM margin_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT trade_date) FROM margin_data')
        date_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT exchange_id) FROM margin_data')
        exchange_count = cursor.fetchone()[0]
        
        stats['total'] = {
            'total_records': total_count,
            'date_count': date_count,
            'exchange_count': exchange_count
        }
        
        return stats
    
    def clear_margin_data(self, exchange_id: Optional[str] = None) -> int:
        """
        清理融资融券数据
        
        参数:
            exchange_id: 如果指定，只清理该交易所的数据；否则清理所有数据
        
        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        
        if exchange_id:
            cursor.execute('DELETE FROM margin_data WHERE exchange_id = ?', (exchange_id,))
        else:
            cursor.execute('DELETE FROM margin_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局融资融券缓存管理器实例
margin_cache_manager = MarginCacheManager()

