"""SQLite缓存管理器"""
import sqlite3
import json
import hashlib
import time
from pathlib import Path
from typing import Optional
import pandas as pd
from config.settings import CACHE_DB, CACHE_TTL

class CacheManager:
    """SQLite缓存管理器"""
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化缓存管理器，连接SQLite数据库"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建表和索引，支持版本管理"""
        cursor = self.conn.cursor()
        
        # 检查表是否存在
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='cache_data'
        ''')
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # 创建新表（支持版本管理）
            cursor.execute('''
                CREATE TABLE cache_data (
                    cache_key TEXT NOT NULL,
                    cache_type TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    params_json TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    data_hash TEXT,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    last_updated REAL,
                    is_expired INTEGER DEFAULT 0,
                    access_count INTEGER DEFAULT 0,
                    last_accessed REAL,
                    PRIMARY KEY (cache_key, version)
                )
            ''')
        else:
            # 表已存在，检查是否需要添加新字段
            cursor.execute('PRAGMA table_info(cache_data)')
            columns = [col[1] for col in cursor.fetchall()]
            
            # 添加新字段（如果不存在）
            if 'version' not in columns:
                cursor.execute('ALTER TABLE cache_data ADD COLUMN version INTEGER DEFAULT 1')
                # 更新现有记录的version为1
                cursor.execute('UPDATE cache_data SET version = 1 WHERE version IS NULL')
            
            if 'data_hash' not in columns:
                cursor.execute('ALTER TABLE cache_data ADD COLUMN data_hash TEXT')
            
            if 'last_updated' not in columns:
                cursor.execute('ALTER TABLE cache_data ADD COLUMN last_updated REAL')
            
            if 'is_expired' not in columns:
                cursor.execute('ALTER TABLE cache_data ADD COLUMN is_expired INTEGER DEFAULT 0')
        
        # 创建索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_cache_key ON cache_data(cache_key)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_cache_type ON cache_data(cache_type)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_expires_at ON cache_data(expires_at)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_data_hash ON cache_data(data_hash)
        ''')
        
        self.conn.commit()
    
    def _generate_cache_key(self, cache_type: str, **params) -> str:
        """生成缓存键"""
        # 将参数排序后序列化，确保相同参数生成相同键
        param_str = json.dumps(params, sort_keys=True, ensure_ascii=False)
        key_str = f"{cache_type}_{param_str}"
        # 使用 MD5 生成短键名
        return hashlib.md5(key_str.encode('utf-8')).hexdigest()
    
    def _calculate_data_hash(self, data: pd.DataFrame) -> str:
        """计算DataFrame的哈希值，用于检测重复数据"""
        try:
            # 将DataFrame转为JSON字符串后计算哈希
            data_str = json.dumps(data.to_dict('records'), sort_keys=True, ensure_ascii=False, default=str)
            return hashlib.md5(data_str.encode('utf-8')).hexdigest()
        except Exception:
            return ""
    
    def _get_latest_version(self, cache_key: str) -> Optional[int]:
        """获取指定缓存键的最新版本号"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT MAX(version) FROM cache_data WHERE cache_key = ?
        ''', (cache_key,))
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else None
    
    def _get_data_hash(self, cache_key: str, version: int) -> Optional[str]:
        """获取指定版本的数据哈希"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT data_hash FROM cache_data WHERE cache_key = ? AND version = ?
        ''', (cache_key, version))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def is_expired(self, cache_type: str, **params) -> bool:
        """检查缓存是否过期"""
        cache_key = self._generate_cache_key(cache_type, **params)
        current_time = time.time()
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT expires_at FROM cache_data 
            WHERE cache_key = ? 
            ORDER BY version DESC 
            LIMIT 1
        ''', (cache_key,))
        
        result = cursor.fetchone()
        if result is None:
            return True  # 未找到数据，视为过期
        
        return current_time > result[0]
    
    def get_dataframe(self, cache_type: str, **params) -> Optional[pd.DataFrame]:
        """获取缓存的DataFrame，即使过期也返回（数据永久保留）"""
        cache_key = self._generate_cache_key(cache_type, **params)
        current_time = time.time()
        
        cursor = self.conn.cursor()
        # 查询最新版本的数据（不过滤过期时间，数据永久保留）
        cursor.execute('''
            SELECT data_json, expires_at, version FROM cache_data 
            WHERE cache_key = ? 
            ORDER BY version DESC 
            LIMIT 1
        ''', (cache_key,))
        
        row = cursor.fetchone()
        if row is None:
            return None
        
        data_json, expires_at, version = row
        
        # 更新访问统计（更新最新版本）
        cursor.execute('''
            UPDATE cache_data 
            SET access_count = access_count + 1, last_accessed = ?
            WHERE cache_key = ? AND version = ?
        ''', (current_time, cache_key, version))
        self.conn.commit()
        
        # 解析JSON并恢复DataFrame
        try:
            data = json.loads(data_json)
            return pd.DataFrame(data)
        except (json.JSONDecodeError, ValueError):
            # 数据损坏，但不删除（保留历史记录）
            return None
    
    def set(self, cache_type: str, data: pd.DataFrame, **params) -> bool:
        """保存DataFrame到缓存，创建新版本（保留所有历史版本）"""
        if data.empty:
            return False
        
        cache_key = self._generate_cache_key(cache_type, **params)
        current_time = time.time()
        ttl = CACHE_TTL.get(cache_type, 3600)
        expires_at = current_time + ttl
        
        # DataFrame转为JSON
        try:
            data_json = json.dumps(data.to_dict('records'), ensure_ascii=False, default=str)
            params_json = json.dumps(params, sort_keys=True, ensure_ascii=False)
            data_hash = self._calculate_data_hash(data)
        except (TypeError, ValueError) as e:
            return False
        
        # 检查是否与最新版本重复
        latest_version = self._get_latest_version(cache_key)
        if latest_version is not None:
            latest_hash = self._get_data_hash(cache_key, latest_version)
            if data_hash == latest_hash:
                # 数据完全相同，只更新过期时间和访问统计，不创建新版本
                cursor = self.conn.cursor()
                cursor.execute('''
                    UPDATE cache_data 
                    SET expires_at = ?, last_updated = ?, is_expired = 0, 
                        access_count = 0, last_accessed = ?
                    WHERE cache_key = ? AND version = ?
                ''', (expires_at, current_time, current_time, cache_key, latest_version))
                self.conn.commit()
                return True
        
        # 创建新版本
        new_version = (latest_version + 1) if latest_version is not None else 1
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO cache_data 
            (cache_key, cache_type, version, params_json, data_json, data_hash,
             created_at, expires_at, last_updated, is_expired, access_count, last_accessed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
        ''', (cache_key, cache_type, new_version, params_json, data_json, data_hash,
              current_time, expires_at, current_time, current_time))
        
        self.conn.commit()
        return True
    
    def clear(self, cache_type: Optional[str] = None) -> int:
        """清理缓存
        参数:
            cache_type: 如果指定，只清理该类型的缓存；否则清理所有缓存
        返回:
            清理的记录数量
        """
        cursor = self.conn.cursor()
        if cache_type:
            cursor.execute('DELETE FROM cache_data WHERE cache_type = ?', (cache_type,))
        else:
            cursor.execute('DELETE FROM cache_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def cleanup_expired(self) -> int:
        """标记过期缓存（不再删除，数据永久保留）"""
        current_time = time.time()
        cursor = self.conn.cursor()
        # 只标记为过期，不删除数据
        cursor.execute('''
            UPDATE cache_data 
            SET is_expired = 1 
            WHERE expires_at <= ? AND is_expired = 0
        ''', (current_time,))
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def cleanup_duplicates(self) -> int:
        """清理明显重复的数据（保留最新版本）"""
        cursor = self.conn.cursor()
        
        # 查找重复数据：相同cache_key和data_hash，但版本不是最新的
        cursor.execute('''
            DELETE FROM cache_data
            WHERE data_hash IS NOT NULL
            AND (cache_key, data_hash, version) NOT IN (
                SELECT cache_key, data_hash, MAX(version) as max_version
                FROM cache_data
                WHERE data_hash IS NOT NULL
                GROUP BY cache_key, data_hash
            )
            AND (cache_key, data_hash) IN (
                SELECT cache_key, data_hash
                FROM cache_data
                WHERE data_hash IS NOT NULL
                GROUP BY cache_key, data_hash
                HAVING COUNT(*) > 1
            )
        ''')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        cursor = self.conn.cursor()
        
        # 按类型统计
        cursor.execute('''
            SELECT cache_type, COUNT(*) as count, SUM(access_count) as total_access
            FROM cache_data
            GROUP BY cache_type
        ''')
        
        stats = {}
        for row in cursor.fetchall():
            cache_type, count, total_access = row
            stats[cache_type] = {
                'count': count,
                'total_access': total_access or 0
            }
        
        # 总统计
        cursor.execute('SELECT COUNT(*) FROM cache_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(access_count) FROM cache_data')
        total_access = cursor.fetchone()[0] or 0
        
        stats['_total'] = {
            'count': total_count,
            'total_access': total_access
        }
        
        return stats
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()

# 创建全局缓存管理器实例
cache_manager = CacheManager()

