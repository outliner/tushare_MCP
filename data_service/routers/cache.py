"""通用缓存路由 — 对应 CacheManager"""
import json
import hashlib
import time
from typing import Optional
from fastapi import APIRouter, HTTPException
from data_service.db.connection import db_manager
from data_service.config import CACHE_TTL
from data_service.models.common import CacheResponse, CacheStatsResponse

router = APIRouter(prefix="/api/cache", tags=["cache"])


def _init_cache_table():
    """初始化通用缓存表（与原 cache_manager.py 的表结构完全一致）"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cache_data'")
    table_exists = cursor.fetchone() is not None

    if not table_exists:
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
        cursor.execute('PRAGMA table_info(cache_data)')
        columns = [col[1] for col in cursor.fetchall()]
        if 'version' not in columns:
            cursor.execute('ALTER TABLE cache_data ADD COLUMN version INTEGER DEFAULT 1')
            cursor.execute('UPDATE cache_data SET version = 1 WHERE version IS NULL')
        if 'data_hash' not in columns:
            cursor.execute('ALTER TABLE cache_data ADD COLUMN data_hash TEXT')
        if 'last_updated' not in columns:
            cursor.execute('ALTER TABLE cache_data ADD COLUMN last_updated REAL')
        if 'is_expired' not in columns:
            cursor.execute('ALTER TABLE cache_data ADD COLUMN is_expired INTEGER DEFAULT 0')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_key ON cache_data(cache_key)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_type ON cache_data(cache_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_expires_at ON cache_data(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_hash ON cache_data(data_hash)')
    conn.commit()


def _generate_cache_key(cache_type: str, params: dict) -> str:
    param_str = json.dumps(params, sort_keys=True, ensure_ascii=False)
    key_str = f"{cache_type}_{param_str}"
    return hashlib.md5(key_str.encode('utf-8')).hexdigest()


def _calculate_data_hash(data: list) -> str:
    try:
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    except Exception:
        return ""


@router.get("/{cache_type}", response_model=CacheResponse)
def get_cache(cache_type: str, params_json: str = "{}"):
    """获取缓存数据"""
    try:
        params = json.loads(params_json)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid params_json")

    conn = db_manager.get_connection()
    cache_key = _generate_cache_key(cache_type, params)
    current_time = time.time()

    cursor = conn.cursor()
    cursor.execute('''
        SELECT data_json, expires_at, version FROM cache_data
        WHERE cache_key = ?
        ORDER BY version DESC LIMIT 1
    ''', (cache_key,))

    row = cursor.fetchone()
    if row is None:
        return CacheResponse(success=False, message="未命中缓存")

    data_json, expires_at, version = row[0], row[1], row[2]
    is_expired = current_time > expires_at

    # 更新访问统计
    cursor.execute('''
        UPDATE cache_data
        SET access_count = access_count + 1, last_accessed = ?
        WHERE cache_key = ? AND version = ?
    ''', (current_time, cache_key, version))
    conn.commit()

    try:
        data = json.loads(data_json)
    except (json.JSONDecodeError, ValueError):
        return CacheResponse(success=False, message="缓存数据损坏")

    return CacheResponse(success=True, data=data, is_expired=is_expired)


@router.post("/{cache_type}", response_model=CacheResponse)
def set_cache(cache_type: str, params_json: str = "{}", data: list = []):
    """写入缓存数据"""
    try:
        params = json.loads(params_json) if isinstance(params_json, str) else params_json
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid params_json")

    if not data:
        return CacheResponse(success=False, message="数据为空")

    conn = db_manager.get_connection()
    cache_key = _generate_cache_key(cache_type, params)
    current_time = time.time()
    ttl = CACHE_TTL.get(cache_type, 3600)
    expires_at = current_time + ttl

    try:
        data_json = json.dumps(data, ensure_ascii=False, default=str)
        params_json_str = json.dumps(params, sort_keys=True, ensure_ascii=False)
        data_hash = _calculate_data_hash(data)
    except (TypeError, ValueError):
        return CacheResponse(success=False, message="数据序列化失败")

    cursor = conn.cursor()

    # 获取最新版本
    cursor.execute('SELECT MAX(version) FROM cache_data WHERE cache_key = ?', (cache_key,))
    result = cursor.fetchone()
    latest_version = result[0] if result and result[0] is not None else None

    if latest_version is not None:
        # 检查是否重复
        cursor.execute('SELECT data_hash FROM cache_data WHERE cache_key = ? AND version = ?',
                       (cache_key, latest_version))
        hash_row = cursor.fetchone()
        if hash_row and hash_row[0] == data_hash:
            cursor.execute('''
                UPDATE cache_data
                SET expires_at = ?, last_updated = ?, is_expired = 0, access_count = 0, last_accessed = ?
                WHERE cache_key = ? AND version = ?
            ''', (expires_at, current_time, current_time, cache_key, latest_version))
            conn.commit()
            return CacheResponse(success=True, message="数据未变化，已刷新过期时间")

    new_version = (latest_version + 1) if latest_version is not None else 1

    cursor.execute('''
        INSERT INTO cache_data
        (cache_key, cache_type, version, params_json, data_json, data_hash,
         created_at, expires_at, last_updated, is_expired, access_count, last_accessed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
    ''', (cache_key, cache_type, new_version, params_json_str, data_json, data_hash,
          current_time, expires_at, current_time, current_time))
    conn.commit()

    return CacheResponse(success=True, message=f"缓存已写入（版本 {new_version}）")


@router.get("/{cache_type}/expired")
def check_expired(cache_type: str, params_json: str = "{}"):
    """检查缓存是否过期"""
    try:
        params = json.loads(params_json)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid params_json")

    conn = db_manager.get_connection()
    cache_key = _generate_cache_key(cache_type, params)
    current_time = time.time()

    cursor = conn.cursor()
    cursor.execute('''
        SELECT expires_at FROM cache_data
        WHERE cache_key = ? ORDER BY version DESC LIMIT 1
    ''', (cache_key,))
    result = cursor.fetchone()

    if result is None:
        return {"expired": True}
    return {"expired": current_time > result[0]}


@router.get("/stats/all", response_model=CacheStatsResponse)
def get_stats():
    """获取缓存统计信息"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT cache_type, COUNT(*) as count, SUM(access_count) as total_access
        FROM cache_data GROUP BY cache_type
    ''')

    stats = {}
    for row in cursor.fetchall():
        stats[row[0]] = {'count': row[1], 'total_access': row[2] or 0}

    cursor.execute('SELECT COUNT(*) FROM cache_data')
    total_count = cursor.fetchone()[0]
    cursor.execute('SELECT SUM(access_count) FROM cache_data')
    total_access = cursor.fetchone()[0] or 0

    stats['_total'] = {'count': total_count, 'total_access': total_access}
    return CacheStatsResponse(stats=stats)


@router.post("/cleanup/expired")
def cleanup_expired():
    """标记过期缓存"""
    conn = db_manager.get_connection()
    current_time = time.time()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE cache_data SET is_expired = 1
        WHERE expires_at <= ? AND is_expired = 0
    ''', (current_time,))
    count = cursor.rowcount
    conn.commit()
    return {"marked_expired": count}


@router.post("/cleanup/duplicates")
def cleanup_duplicates():
    """清理重复数据"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM cache_data
        WHERE data_hash IS NOT NULL
        AND (cache_key, data_hash, version) NOT IN (
            SELECT cache_key, data_hash, MAX(version) as max_version
            FROM cache_data WHERE data_hash IS NOT NULL
            GROUP BY cache_key, data_hash
        )
        AND (cache_key, data_hash) IN (
            SELECT cache_key, data_hash FROM cache_data
            WHERE data_hash IS NOT NULL
            GROUP BY cache_key, data_hash HAVING COUNT(*) > 1
        )
    ''')
    count = cursor.rowcount
    conn.commit()
    return {"cleaned": count}
