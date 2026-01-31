"""股票-板块映射数据专用缓存管理器"""
import sqlite3
import time
import json
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from config.settings import CACHE_DB


class MappingCacheManager:
    """股票-板块映射数据专用缓存管理器
    
    特点：
    1. 存储股票与申万行业、东财行业、东财概念的对应关系
    2. 支持增量更新（Upsert策略）
    3. 主键：ts_code
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化映射缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        try:
            self.conn.execute('PRAGMA journal_mode=DELETE')
            self.conn.execute('PRAGMA synchronous=OFF')
        except Exception:
            pass
        self._init_database()
    
    def _init_database(self):
        """创建股票-板块映射数据表"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_sector_mapping (
                ts_code TEXT PRIMARY KEY,        -- TS股票代码
                name TEXT,                        -- 股票名称
                sw_l2_code TEXT,                 -- 申万二级行业代码
                sw_l2_name TEXT,                 -- 申万二级行业名称
                em_industry_code TEXT,           -- 东财行业代码
                em_industry_name TEXT,           -- 东财行业名称
                em_concept_codes TEXT,           -- 东财概念代码列表 (JSON数组)
                em_concept_names TEXT,           -- 东财概念名称列表 (JSON数组)
                updated_at REAL NOT NULL         -- 更新时间戳
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mapping_sw_l2 ON stock_sector_mapping(sw_l2_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mapping_em_industry ON stock_sector_mapping(em_industry_code)')
        
        self.conn.commit()
    
    def save_mapping(self, df: pd.DataFrame) -> int:
        """
        保存或更新映射数据
        
        参数:
            df: 包含映射数据的DataFrame
        """
        if df.empty:
            return 0
            
        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                # 处理概念列表
                concept_codes = row.get('em_concept_codes', [])
                concept_names = row.get('em_concept_names', [])
                
                if isinstance(concept_codes, list):
                    concept_codes_json = json.dumps(concept_codes, ensure_ascii=False)
                else:
                    concept_codes_json = str(concept_codes)
                    
                if isinstance(concept_names, list):
                    concept_names_json = json.dumps(concept_names, ensure_ascii=False)
                else:
                    concept_names_json = str(concept_names)

                cursor.execute('''
                    INSERT OR REPLACE INTO stock_sector_mapping (
                        ts_code, name, sw_l2_code, sw_l2_name,
                        em_industry_code, em_industry_name,
                        em_concept_codes, em_concept_names,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row.get('ts_code', '')),
                    str(row.get('name', '')),
                    str(row.get('sw_l2_code', '')),
                    str(row.get('sw_l2_name', '')),
                    str(row.get('em_industry_code', '')),
                    str(row.get('em_industry_name', '')),
                    concept_codes_json,
                    concept_names_json,
                    current_time
                ))
                saved_count += 1
            except Exception as e:
                print(f"保存映射数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
                
        self.conn.commit()
        return saved_count

    def get_mapping_by_code(self, ts_code: str) -> Optional[Dict]:
        """获取单只股票的映射关系"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM stock_sector_mapping WHERE ts_code = ?', (ts_code,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        # 获取列名
        columns = [column[0] for column in cursor.description]
        result = dict(zip(columns, row))
        
        # 解析 JSON
        try:
            result['em_concept_codes'] = json.loads(result['em_concept_codes'])
            result['em_concept_names'] = json.loads(result['em_concept_names'])
        except:
            pass
            
        return result

    def search_by_sector(self, sector_type: str, sector_code: str) -> pd.DataFrame:
        """按板块代码查询股票列表"""
        cursor = self.conn.cursor()
        
        if sector_type == 'sw_l2':
            query = "SELECT * FROM stock_sector_mapping WHERE sw_l2_code = ?"
        elif sector_type == 'em_industry':
            query = "SELECT * FROM stock_sector_mapping WHERE em_industry_code = ?"
        elif sector_type == 'em_concept':
            query = "SELECT * FROM stock_sector_mapping WHERE em_concept_codes LIKE ?"
            sector_code = f'%"{sector_code}"%'
        else:
            return pd.DataFrame()
            
        cursor.execute(query, (sector_code,))
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame(rows, columns=columns)

    def get_mapping_count(self) -> int:
        """获取映射数据总数，用于检查是否已初始化"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM stock_sector_mapping')
        return cursor.fetchone()[0]

    def close(self):
        if self.conn:
            self.conn.close()


# 全局实例
mapping_cache_manager = MappingCacheManager()
