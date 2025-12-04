"""东财概念板块数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd
from config.settings import CACHE_DB


class ConceptCacheManager:
    """东财概念板块数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储东财概念板块数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 支持三种数据类型：
       - dc_index: 概念板块列表（主键：trade_date, ts_code）
       - dc_daily: 概念板块日线行情（主键：ts_code, trade_date）
       - dc_member: 概念板块成分（主键：ts_code, trade_date, con_code）
    4. 支持按板块代码、交易日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化概念板块缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建概念板块数据表"""
        cursor = self.conn.cursor()
        
        # 创建概念板块列表表（dc_index）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS concept_index_data (
                trade_date TEXT NOT NULL,        -- 交易日期（YYYYMMDD格式）
                ts_code TEXT NOT NULL,           -- 板块代码（如：BK1184.DC）
                name TEXT,                      -- 板块名称
                pct_change REAL,                 -- 涨跌幅（百分比）
                leading TEXT,                   -- 领涨股票代码
                leading_pct REAL,                -- 领涨股票涨跌幅
                total_mv REAL,                   -- 总市值（万元）
                turnover_rate REAL,              -- 换手率（百分比）
                up_num INTEGER,                 -- 上涨家数
                down_num INTEGER,               -- 下跌家数
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (trade_date, ts_code)
            )
        ''')
        
        # 创建概念板块日线行情表（dc_daily）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS concept_daily_data (
                ts_code TEXT NOT NULL,           -- 板块代码（如：BK1184.DC）
                trade_date TEXT NOT NULL,        -- 交易日期（YYYYMMDD格式）
                close REAL,                      -- 收盘点位
                open REAL,                       -- 开盘点位
                high REAL,                       -- 最高点位
                low REAL,                        -- 最低点位
                pre_close REAL,                  -- 昨收点位
                change REAL,                     -- 涨跌点位
                pct_change REAL,                 -- 涨跌幅（百分比）
                vol REAL,                       -- 成交量
                amount REAL,                     -- 成交额
                swing REAL,                      -- 振幅（百分比）
                turnover_rate REAL,              -- 换手率（百分比）
                idx_type TEXT,                  -- 板块类型（概念板块、行业板块、地域板块）
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, trade_date)
            )
        ''')
        
        # 创建概念板块成分表（dc_member）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS concept_member_data (
                ts_code TEXT NOT NULL,           -- 板块代码（如：BK1184.DC）
                trade_date TEXT NOT NULL,        -- 交易日期（YYYYMMDD格式）
                con_code TEXT NOT NULL,          -- 成分股票代码（如：002117.SZ）
                name TEXT,                      -- 成分股名称
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, trade_date, con_code)
            )
        ''')
        
        # 创建索引以提升查询性能
        # concept_index_data 索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_index_trade_date 
            ON concept_index_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_index_ts_code 
            ON concept_index_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_index_trade_date_ts_code 
            ON concept_index_data(trade_date, ts_code)
        ''')
        
        # concept_daily_data 索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_daily_ts_code 
            ON concept_daily_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_daily_trade_date 
            ON concept_daily_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_daily_ts_code_date 
            ON concept_daily_data(ts_code, trade_date)
        ''')
        
        # concept_member_data 索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_member_ts_code 
            ON concept_member_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_member_trade_date 
            ON concept_member_data(trade_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_member_con_code 
            ON concept_member_data(con_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_concept_member_ts_code_date 
            ON concept_member_data(ts_code, trade_date)
        ''')
        
        self.conn.commit()
    
    def save_concept_index_data(self, df: pd.DataFrame) -> int:
        """
        保存概念板块列表数据到数据库（dc_index）
        
        参数:
            df: 包含概念板块列表数据的DataFrame，必须包含 trade_date 和 ts_code 列
        
        返回:
            保存的记录数量
        """
        if df.empty:
            return 0
        
        # 确保必要的列存在
        required_columns = ['trade_date', 'ts_code']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame必须包含 '{col}' 列")
        
        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0
        
        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO concept_index_data (
                        trade_date, ts_code, name, pct_change, leading, leading_pct,
                        total_mv, turnover_rate, up_num, down_num, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('trade_date', '')),
                    str(row.get('ts_code', '')),
                    row.get('name') if pd.notna(row.get('name')) else None,
                    row.get('pct_change') if pd.notna(row.get('pct_change')) else None,
                    row.get('leading') if pd.notna(row.get('leading')) else None,
                    row.get('leading_pct') if pd.notna(row.get('leading_pct')) else None,
                    row.get('total_mv') if pd.notna(row.get('total_mv')) else None,
                    row.get('turnover_rate') if pd.notna(row.get('turnover_rate')) else None,
                    int(row.get('up_num')) if pd.notna(row.get('up_num')) else None,
                    int(row.get('down_num')) if pd.notna(row.get('down_num')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存概念板块列表数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_concept_index_data(
        self,
        ts_code: Optional[str] = None,
        name: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询概念板块列表数据
        
        参数:
            ts_code: 板块代码（支持多个，逗号分隔）
            name: 板块名称（模糊匹配）
            trade_date: 特定交易日期（YYYYMMDD格式）
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
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
            # 支持多个板块代码（逗号分隔）
            codes = [code.strip() for code in ts_code.split(',')]
            if len(codes) == 1:
                conditions.append('ts_code = ?')
                params.append(codes[0])
            else:
                placeholders = ','.join(['?'] * len(codes))
                conditions.append(f'ts_code IN ({placeholders})')
                params.extend(codes)
        
        if name:
            conditions.append('name LIKE ?')
            params.append(f'%{name}%')
        
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
                trade_date, ts_code, name, pct_change, leading, leading_pct,
                total_mv, turnover_rate, up_num, down_num, created_at
            FROM concept_index_data
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
            'trade_date', 'ts_code', 'name', 'pct_change', 'leading', 'leading_pct',
            'total_mv', 'turnover_rate', 'up_num', 'down_num', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = ['pct_change', 'leading_pct', 'total_mv', 'turnover_rate', 'created_at']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        integer_columns = ['up_num', 'down_num']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        return df
    
    def save_concept_daily_data(self, df: pd.DataFrame) -> int:
        """
        保存概念板块日线行情数据到数据库（dc_daily）
        
        参数:
            df: 包含概念板块日线行情数据的DataFrame，必须包含 ts_code 和 trade_date 列
        
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
                cursor.execute('''
                    INSERT OR REPLACE INTO concept_daily_data (
                        ts_code, trade_date, close, open, high, low, pre_close,
                        change, pct_change, vol, amount, swing, turnover_rate,
                        idx_type, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('trade_date', '')),
                    row.get('close') if pd.notna(row.get('close')) else None,
                    row.get('open') if pd.notna(row.get('open')) else None,
                    row.get('high') if pd.notna(row.get('high')) else None,
                    row.get('low') if pd.notna(row.get('low')) else None,
                    row.get('pre_close') if pd.notna(row.get('pre_close')) else None,
                    row.get('change') if pd.notna(row.get('change')) else None,
                    row.get('pct_change') if pd.notna(row.get('pct_change')) else None,
                    row.get('vol') if pd.notna(row.get('vol')) else None,
                    row.get('amount') if pd.notna(row.get('amount')) else None,
                    row.get('swing') if pd.notna(row.get('swing')) else None,
                    row.get('turnover_rate') if pd.notna(row.get('turnover_rate')) else None,
                    row.get('idx_type') if pd.notna(row.get('idx_type')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存概念板块日线行情数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_concept_daily_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        idx_type: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询概念板块日线行情数据
        
        参数:
            ts_code: 板块代码（支持多个，逗号分隔）
            trade_date: 特定交易日期（YYYYMMDD格式）
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            idx_type: 板块类型（概念板块、行业板块、地域板块）
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
            # 支持多个板块代码（逗号分隔）
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
        elif start_date or end_date:
            if start_date:
                conditions.append('trade_date >= ?')
                params.append(start_date)
            if end_date:
                conditions.append('trade_date <= ?')
                params.append(end_date)
        
        if idx_type:
            conditions.append('idx_type = ?')
            params.append(idx_type)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        order_clause = f'ORDER BY trade_date {order_by}'
        limit_clause = f'LIMIT {limit}' if limit else ''
        
        query = f'''
            SELECT 
                ts_code, trade_date, close, open, high, low, pre_close,
                change, pct_change, vol, amount, swing, turnover_rate,
                idx_type, created_at
            FROM concept_daily_data
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
            'ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'pre_close',
            'change', 'pct_change', 'vol', 'amount', 'swing', 'turnover_rate',
            'idx_type', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = [
            'close', 'open', 'high', 'low', 'pre_close', 'change', 'pct_change',
            'vol', 'amount', 'swing', 'turnover_rate', 'created_at'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def save_concept_member_data(self, df: pd.DataFrame) -> int:
        """
        保存概念板块成分数据到数据库（dc_member）
        
        参数:
            df: 包含概念板块成分数据的DataFrame，必须包含 ts_code, trade_date 和 con_code 列
        
        返回:
            保存的记录数量
        """
        if df.empty:
            return 0
        
        # 确保必要的列存在
        required_columns = ['ts_code', 'trade_date', 'con_code']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame必须包含 '{col}' 列")
        
        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0
        
        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO concept_member_data (
                        ts_code, trade_date, con_code, name, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('trade_date', '')),
                    str(row.get('con_code', '')),
                    row.get('name') if pd.notna(row.get('name')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存概念板块成分数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_concept_member_data(
        self,
        ts_code: Optional[str] = None,
        con_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询概念板块成分数据
        
        参数:
            ts_code: 板块代码（支持多个，逗号分隔）
            con_code: 成分股票代码（支持多个，逗号分隔）
            trade_date: 特定交易日期（YYYYMMDD格式）
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
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
            # 支持多个板块代码（逗号分隔）
            codes = [code.strip() for code in ts_code.split(',')]
            if len(codes) == 1:
                conditions.append('ts_code = ?')
                params.append(codes[0])
            else:
                placeholders = ','.join(['?'] * len(codes))
                conditions.append(f'ts_code IN ({placeholders})')
                params.extend(codes)
        
        if con_code:
            # 支持多个成分股票代码（逗号分隔）
            codes = [code.strip() for code in con_code.split(',')]
            if len(codes) == 1:
                conditions.append('con_code = ?')
                params.append(codes[0])
            else:
                placeholders = ','.join(['?'] * len(codes))
                conditions.append(f'con_code IN ({placeholders})')
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
                ts_code, trade_date, con_code, name, created_at
            FROM concept_member_data
            WHERE {where_clause}
            {order_clause}
            {limit_clause}
        '''
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if not rows:
            return None
        
        # 转换为DataFrame
        columns = ['ts_code', 'trade_date', 'con_code', 'name', 'created_at']
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_numeric(df['created_at'], errors='coerce')
        
        return df
    
    def has_concept_index_data(self, trade_date: Optional[str] = None) -> bool:
        """检查是否存在概念板块列表数据"""
        cursor = self.conn.cursor()
        if trade_date:
            cursor.execute('SELECT COUNT(*) FROM concept_index_data WHERE trade_date = ?', (trade_date,))
        else:
            cursor.execute('SELECT COUNT(*) FROM concept_index_data')
        count = cursor.fetchone()[0]
        return count > 0
    
    def has_concept_daily_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None
    ) -> bool:
        """检查是否存在概念板块日线行情数据"""
        cursor = self.conn.cursor()
        conditions = []
        params = []
        
        if ts_code:
            conditions.append('ts_code = ?')
            params.append(ts_code)
        if trade_date:
            conditions.append('trade_date = ?')
            params.append(trade_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        cursor.execute(f'SELECT COUNT(*) FROM concept_daily_data WHERE {where_clause}', params)
        count = cursor.fetchone()[0]
        return count > 0
    
    def has_concept_member_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None
    ) -> bool:
        """检查是否存在概念板块成分数据"""
        cursor = self.conn.cursor()
        conditions = []
        params = []
        
        if ts_code:
            conditions.append('ts_code = ?')
            params.append(ts_code)
        if trade_date:
            conditions.append('trade_date = ?')
            params.append(trade_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        cursor.execute(f'SELECT COUNT(*) FROM concept_member_data WHERE {where_clause}', params)
        count = cursor.fetchone()[0]
        return count > 0
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局概念板块缓存管理器实例
concept_cache_manager = ConceptCacheManager()

