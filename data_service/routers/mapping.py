"""股票-板块映射路由 — 对应 MappingCacheManager"""
import time
import json
from typing import Optional
from fastapi import APIRouter
from data_service.db.connection import db_manager
from data_service.models.common import MappingSaveRequest, MappingResponse, StockDataResponse

router = APIRouter(prefix="/api/mapping", tags=["mapping"])


def _init_mapping_table():
    """初始化映射表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_sector_mapping (
            ts_code TEXT PRIMARY KEY,
            name TEXT,
            sw_l2_code TEXT, sw_l2_name TEXT,
            em_industry_code TEXT, em_industry_name TEXT,
            em_concept_codes TEXT, em_concept_names TEXT,
            updated_at REAL NOT NULL
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_mapping_sw_l2 ON stock_sector_mapping(sw_l2_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_mapping_em_industry ON stock_sector_mapping(em_industry_code)')
    conn.commit()


@router.get("/{ts_code}")
def get_mapping(ts_code: str):
    """获取单只股票的映射关系"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM stock_sector_mapping WHERE ts_code = ?', (ts_code,))
    row = cursor.fetchone()
    if not row:
        return MappingResponse(success=False, count=0)
    columns = [col[0] for col in cursor.description]
    result = dict(zip(columns, row))
    try:
        result['em_concept_codes'] = json.loads(result['em_concept_codes'])
        result['em_concept_names'] = json.loads(result['em_concept_names'])
    except:
        pass
    return MappingResponse(success=True, data=result, count=1)


@router.get("/sector/{sector_type}/{sector_code}")
def search_by_sector(sector_type: str, sector_code: str):
    """按板块查询股票列表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    if sector_type == 'sw_l2':
        cursor.execute("SELECT * FROM stock_sector_mapping WHERE sw_l2_code = ?", (sector_code,))
    elif sector_type == 'em_industry':
        cursor.execute("SELECT * FROM stock_sector_mapping WHERE em_industry_code = ?", (sector_code,))
    elif sector_type == 'em_concept':
        cursor.execute("SELECT * FROM stock_sector_mapping WHERE em_concept_codes LIKE ?",
                       (f'%"{sector_code}"%',))
    else:
        return StockDataResponse(success=False, data=[], count=0, message="无效的板块类型")

    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, r)) for r in rows]
    return StockDataResponse(success=True, data=data, count=len(data))


@router.post("")
def save_mapping(request: MappingSaveRequest):
    """保存映射数据"""
    if not request.data:
        return MappingResponse(success=False, count=0)

    conn = db_manager.get_connection()
    cursor = conn.cursor()
    current_time = time.time()
    saved = 0

    for row in request.data:
        try:
            concept_codes = row.get('em_concept_codes', [])
            concept_names = row.get('em_concept_names', [])
            concept_codes_json = json.dumps(concept_codes, ensure_ascii=False) if isinstance(concept_codes, list) else str(concept_codes)
            concept_names_json = json.dumps(concept_names, ensure_ascii=False) if isinstance(concept_names, list) else str(concept_names)

            cursor.execute('''
                INSERT OR REPLACE INTO stock_sector_mapping (
                    ts_code, name, sw_l2_code, sw_l2_name,
                    em_industry_code, em_industry_name,
                    em_concept_codes, em_concept_names, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('name', '')),
                str(row.get('sw_l2_code', '')), str(row.get('sw_l2_name', '')),
                str(row.get('em_industry_code', '')), str(row.get('em_industry_name', '')),
                concept_codes_json, concept_names_json, current_time
            ))
            saved += 1
        except Exception:
            continue

    conn.commit()
    return MappingResponse(success=True, count=saved)


@router.get("/all/list")
def get_all_mapping():
    """获取所有映射数据的列表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM stock_sector_mapping')
    rows = cursor.fetchall()
    if not rows:
        return StockDataResponse(success=True, data=[], count=0)
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, r)) for r in rows]
    return StockDataResponse(success=True, data=data, count=len(data))


@router.get("/count/all")
def get_mapping_count():
    """获取映射数据总数"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM stock_sector_mapping')
    return {"count": cursor.fetchone()[0]}
