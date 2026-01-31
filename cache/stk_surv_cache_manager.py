"""机构调研数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB


class StkSurvCacheManager:
    """机构调研数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储机构调研数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 主键：(ts_code, surv_date, fund_visitors, rece_org)，确保同一条调研记录只有一条数据
    4. 支持按股票代码、调研日期、日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化机构调研缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建机构调研数据表"""
        cursor = self.conn.cursor()
        
        # 创建机构调研数据表
        # 根据 Tushare stk_surv 接口的数据格式设计
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stk_surv_data (
                ts_code TEXT NOT NULL,          -- 股票代码（如：002223.SZ）
                name TEXT,                      -- 股票名称
                surv_date TEXT NOT NULL,        -- 调研日期（YYYYMMDD格式）
                fund_visitors TEXT,             -- 机构参与人员
                rece_place TEXT,                -- 接待地点
                rece_mode TEXT,                 -- 接待方式
                rece_org TEXT,                  -- 接待的公司
                org_type TEXT,                  -- 接待公司类型
                comp_rece TEXT,                 -- 上市公司接待人员
                content TEXT,                   -- 调研内容
                created_at REAL NOT NULL,        -- 数据创建时间戳
                PRIMARY KEY (ts_code, surv_date, fund_visitors, rece_org)
            )
        ''')
        
        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stk_surv_ts_code ON stk_surv_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stk_surv_surv_date ON stk_surv_data(surv_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stk_surv_ts_code_date ON stk_surv_data(ts_code, surv_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stk_surv_created_at ON stk_surv_data(created_at)
        ''')
        
        self.conn.commit()
    
    def save_stk_surv_data(self, df: pd.DataFrame) -> int:
        """
        保存机构调研数据到数据库
        
        参数:
            df: 包含机构调研数据的DataFrame，必须包含 ts_code 和 surv_date 列
        
        返回:
            保存的记录数量
        """
        if df.empty:
            return 0
        
        # 确保必要的列存在
        required_columns = ['ts_code', 'surv_date']
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
                # 如果 fund_visitors 或 rece_org 为空，使用空字符串作为主键的一部分
                fund_visitors = str(row.get('fund_visitors', '')) if pd.notna(row.get('fund_visitors')) else ''
                rece_org = str(row.get('rece_org', '')) if pd.notna(row.get('rece_org')) else ''
                
                cursor.execute('''
                    INSERT OR REPLACE INTO stk_surv_data (
                        ts_code, name, surv_date, fund_visitors, rece_place,
                        rece_mode, rece_org, org_type, comp_rece, content, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('name', '')) if pd.notna(row.get('name')) else None,
                    str(row.get('surv_date', '')),
                    fund_visitors,
                    str(row.get('rece_place', '')) if pd.notna(row.get('rece_place')) else None,
                    str(row.get('rece_mode', '')) if pd.notna(row.get('rece_mode')) else None,
                    rece_org,
                    str(row.get('org_type', '')) if pd.notna(row.get('org_type')) else None,
                    str(row.get('comp_rece', '')) if pd.notna(row.get('comp_rece')) else None,
                    str(row.get('content', '')) if pd.notna(row.get('content')) else None,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存机构调研数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_stk_surv_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询机构调研数据
        
        参数:
            ts_code: 股票代码（如：002223.SZ，支持多个股票，逗号分隔）
            trade_date: 特定调研日期（YYYYMMDD格式），如果提供则只查询该日期
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
            conditions.append('surv_date = ?')
            params.append(trade_date)
        elif start_date or end_date:
            if start_date:
                conditions.append('surv_date >= ?')
                params.append(start_date)
            if end_date:
                conditions.append('surv_date <= ?')
                params.append(end_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        order_clause = f'ORDER BY surv_date {order_by}'
        limit_clause = f'LIMIT {limit}' if limit else ''
        
        query = f'''
            SELECT 
                ts_code, name, surv_date, fund_visitors, rece_place,
                rece_mode, rece_org, org_type, comp_rece, content, created_at
            FROM stk_surv_data
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
            'ts_code', 'name', 'surv_date', 'fund_visitors', 'rece_place',
            'rece_mode', 'rece_org', 'org_type', 'comp_rece', 'content', 'created_at'
        ]
        df = pd.DataFrame(rows, columns=columns)
        
        # 转换数据类型
        numeric_columns = ['created_at']
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
        检查是否存在指定股票的调研数据
        
        参数:
            ts_code: 股票代码（可选），如果提供则检查特定股票
            trade_date: 调研日期（可选），如果提供则检查特定日期
        
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
            conditions.append('surv_date = ?')
            params.append(trade_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        
        cursor.execute(f'''
            SELECT COUNT(*) FROM stk_surv_data
            WHERE {where_clause}
        ''', params)
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        """
        获取指定股票的调研数据日期范围
        
        参数:
            ts_code: 股票代码
        
        返回:
            包含 'start_date' 和 'end_date' 的字典，如果无数据则返回None
        """
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT MIN(surv_date) as start_date, MAX(surv_date) as end_date
            FROM stk_surv_data
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
        """获取机构调研数据统计信息"""
        cursor = self.conn.cursor()
        
        # 按股票代码统计
        cursor.execute('''
            SELECT 
                ts_code,
                COUNT(*) as record_count,
                MIN(surv_date) as earliest_date,
                MAX(surv_date) as latest_date
            FROM stk_surv_data
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
        cursor.execute('SELECT COUNT(*) FROM stk_surv_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM stk_surv_data')
        stock_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT surv_date) FROM stk_surv_data')
        date_count = cursor.fetchone()[0]
        
        stats['total'] = {
            'total_records': total_count,
            'stock_count': stock_count,
            'date_count': date_count
        }
        
        return stats
    
    def clear_stk_surv_data(self, ts_code: Optional[str] = None) -> int:
        """
        清理机构调研数据
        
        参数:
            ts_code: 如果指定，只清理该股票的数据；否则清理所有数据
        
        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        
        if ts_code:
            cursor.execute('DELETE FROM stk_surv_data WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM stk_surv_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局机构调研缓存管理器实例
stk_surv_cache_manager = StkSurvCacheManager()

