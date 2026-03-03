"""指数与概念板块数据路由 — 对应 Index(Daily)与Concept CacheManager"""
import time
from typing import Optional, List, Dict, Any
from fastapi import APIRouter
from pydantic import BaseModel
from data_service.db.connection import db_manager
from data_service.models.common import StockDataSaveRequest, StockDataResponse

router = APIRouter(prefix="/api/index_market", tags=["index_market"])

# ==========================================
# Schema Models
# ==========================================

class BoardNameMapSaveRequest(BaseModel):
    name_map: Dict[str, str]
    board_type: str

class BoardNameMapResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, str]] = None
    count: int = 0

class DateRangeResponse(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    has_data: bool = False

# ==========================================
# Table Initialization
# ==========================================

def _init_index_market_tables():
    """初始化指数与概念相关的所有表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    
    # 1. 国际指数 (IndexCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS global_index_data (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            open REAL, close REAL, high REAL, low REAL,
            pre_close REAL, change REAL, pct_chg REAL, swing REAL,
            vol REAL, amount REAL, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_gi_ts_code ON global_index_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_gi_trade_date ON global_index_data(trade_date)')
    
    # 2. A股指数日线 (IndexDailyCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_daily_data (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            close REAL, open REAL, high REAL, low REAL, pre_close REAL,
            change REAL, pct_chg REAL, vol REAL, amount REAL, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_id_ts_code ON index_daily_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_id_trade_date ON index_daily_data(trade_date)')
    
    # 3. 概念板块列表 (ConceptCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS concept_index_data (
            trade_date TEXT NOT NULL, ts_code TEXT NOT NULL,
            name TEXT, pct_change REAL, leading TEXT, leading_pct REAL,
            total_mv REAL, turnover_rate REAL, up_num INTEGER, down_num INTEGER,
            idx_type TEXT, created_at REAL NOT NULL,
            PRIMARY KEY (trade_date, ts_code)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ci_ts_code ON concept_index_data(ts_code)')

    # 4. 概念日线行情 (ConceptCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS concept_daily_data (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            close REAL, open REAL, high REAL, low REAL, pre_close REAL,
            change REAL, pct_change REAL, vol REAL, amount REAL, swing REAL,
            turnover_rate REAL, idx_type TEXT, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cd_ts_code ON concept_daily_data(ts_code)')

    # 5. 概念成分 (ConceptCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS concept_member_data (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL, con_code TEXT NOT NULL,
            name TEXT, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date, con_code)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cm_ts_code ON concept_member_data(ts_code)')

    # 6. 板块代码名称映射 (ConceptCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS board_code_name_map (
            ts_code TEXT NOT NULL, board_type TEXT NOT NULL,
            name TEXT, updated_at REAL NOT NULL,
            PRIMARY KEY (ts_code, board_type)
        )
    ''')
    
    conn.commit()


# ==========================================
# Endpoints: 国际指数 (global_index_data)
# ==========================================

@router.post("/global_index")
def save_global_index(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO global_index_data (
                    ts_code, trade_date, open, close, high, low,
                    pre_close, change, pct_chg, swing, vol, amount, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                row.get('open'), row.get('close'), row.get('high'), row.get('low'),
                row.get('pre_close'), row.get('change'), row.get('pct_chg'),
                row.get('swing'), row.get('vol'), row.get('amount'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/global_index")
def get_global_index(ts_code: str, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = ['ts_code = ?']; params = [ts_code]
    if trade_date: conds.append('trade_date = ?'); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append('trade_date >= ?'); params.append(start_date)
        if end_date: conds.append('trade_date <= ?'); params.append(end_date)
    
    q = f"SELECT * FROM global_index_data WHERE {' AND '.join(conds)} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/global_index/has_data")
def global_index_has_data(ts_code: str, trade_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    if trade_date:
        cursor.execute("SELECT COUNT(*) FROM global_index_data WHERE ts_code=? AND trade_date=?", (ts_code, trade_date))
    else:
        cursor.execute("SELECT COUNT(*) FROM global_index_data WHERE ts_code=?", (ts_code,))
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/global_index/date_range")
def global_index_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM global_index_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/global_index/stats")
def global_index_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM global_index_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM global_index_data")
    idx = cursor.fetchone()[0]
    return {"total_records": t, "index_count": idx}


# ==========================================
# Endpoints: A股指数日线 (index_daily_data)
# ==========================================

@router.post("/index_daily")
def save_index_daily(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO index_daily_data (
                    ts_code, trade_date, close, open, high, low,
                    pre_close, change, pct_chg, vol, amount, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                row.get('close'), row.get('open'), row.get('high'), row.get('low'),
                row.get('pre_close'), row.get('change'), row.get('pct_chg'),
                row.get('vol'), row.get('amount'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/index_daily")
def get_index_daily(ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
        if len(codes) == 1:
            conds.append("ts_code = ?"); params.append(codes[0])
        else:
            conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("trade_date >= ?"); params.append(start_date)
        if end_date: conds.append("trade_date <= ?"); params.append(end_date)
    
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM index_daily_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/index_daily/has_data")
def index_daily_has_data(ts_code: str, trade_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    codes = [c.strip() for c in ts_code.split(',')]
    p = list(codes)
    if trade_date:
        p.append(trade_date)
        cursor.execute(f"SELECT COUNT(*) FROM index_daily_data WHERE ts_code IN ({','.join(['?'] * len(codes))}) AND trade_date = ?", p)
    else:
        cursor.execute(f"SELECT COUNT(*) FROM index_daily_data WHERE ts_code IN ({','.join(['?'] * len(codes))})", p)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/index_daily/date_range")
def index_daily_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM index_daily_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/index_daily/stats")
def index_daily_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM index_daily_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM index_daily_data")
    idx = cursor.fetchone()[0]
    return {"total_records": t, "index_count": idx}


# ==========================================
# Endpoints: Concept (concept_...)
# ==========================================

@router.post("/concept/index")
def save_concept_index(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO concept_index_data (
                    trade_date, ts_code, name, pct_change, leading, leading_pct,
                    total_mv, turnover_rate, up_num, down_num, idx_type, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('trade_date', '')), str(row.get('ts_code', '')),
                row.get('name'), row.get('pct_change'), row.get('leading'), row.get('leading_pct'),
                row.get('total_mv'), row.get('turnover_rate'), row.get('up_num'), row.get('down_num'),
                row.get('idx_type'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/concept/index")
def get_concept_index(ts_code: Optional[str] = None, name: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, idx_type: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
        conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if name: conds.append("name LIKE ?"); params.append(f"%{name}%")
    if idx_type: conds.append("idx_type = ?"); params.append(idx_type)
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("trade_date >= ?"); params.append(start_date)
        if end_date: conds.append("trade_date <= ?"); params.append(end_date)
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM concept_index_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.post("/concept/daily")
def save_concept_daily(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO concept_daily_data (
                    ts_code, trade_date, close, open, high, low, pre_close,
                    change, pct_change, vol, amount, swing, turnover_rate,
                    idx_type, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                row.get('close'), row.get('open'), row.get('high'), row.get('low'), row.get('pre_close'),
                row.get('change'), row.get('pct_change'), row.get('vol'), row.get('amount'),
                row.get('swing'), row.get('turnover_rate'), row.get('idx_type'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/concept/daily")
def get_concept_daily(ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, idx_type: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
        conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("trade_date >= ?"); params.append(start_date)
        if end_date: conds.append("trade_date <= ?"); params.append(end_date)
    if idx_type: conds.append("idx_type = ?"); params.append(idx_type)
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM concept_daily_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.post("/concept/member")
def save_concept_member(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO concept_member_data (
                    ts_code, trade_date, con_code, name, created_at
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                str(row.get('con_code', '')), row.get('name'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/concept/member")
def get_concept_member(ts_code: Optional[str] = None, con_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
        conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if con_code:
        codes = [c.strip() for c in con_code.split(',')]
        conds.append(f"con_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("trade_date >= ?"); params.append(start_date)
        if end_date: conds.append("trade_date <= ?"); params.append(end_date)
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM concept_member_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/concept/has_data")
def concept_has_data(layer: str, ts_code: Optional[str] = None, trade_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code: conds.append("ts_code = ?"); params.append(ts_code)
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    where = " AND ".join(conds) if conds else "1=1"
    
    table = "concept_index_data" if layer == "index" else "concept_daily_data" if layer == "daily" else "concept_member_data"
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.post("/concept/board_map")
def save_board_name_map(request: BoardNameMapSaveRequest):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for code, name in request.name_map.items():
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO board_code_name_map (ts_code, board_type, name, updated_at) 
                VALUES (?, ?, ?, ?)
            ''', (str(code), request.board_type, str(name) if name else None, now_ts))
            saved += 1
        except Exception: continue
    conn.commit()
    return BoardNameMapResponse(success=True, count=saved)

@router.get("/concept/board_map")
def get_board_name_map(ts_codes: str, board_type: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    codes = [c.strip() for c in ts_codes.split(',')]
    cursor.execute(f"SELECT ts_code, name FROM board_code_name_map WHERE ts_code IN ({','.join(['?'] * len(codes))}) AND board_type = ?", codes + [board_type])
    rows = cursor.fetchall()
    m = {r[0]: (r[1] if r[1] else r[0]) for r in rows}
    return BoardNameMapResponse(success=True, data=m, count=len(m))
