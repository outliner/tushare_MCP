"""情绪资金类数据路由 — 对应 Margin, MarginDetail, StkSurv, CyqPerf CacheManager"""
import time
from typing import Optional, List, Dict, Any
from fastapi import APIRouter
from pydantic import BaseModel
from data_service.db.connection import db_manager
from data_service.models.common import StockDataSaveRequest, StockDataResponse

router = APIRouter(prefix="/api/sentiment_funds", tags=["sentiment_funds"])

class DateRangeResponse(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    has_data: bool = False

# ==========================================
# Table Initialization
# ==========================================

def _init_sentiment_funds_tables():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    
    # 1. 融资融券汇总 (MarginCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS margin_data (
            trade_date TEXT NOT NULL, exchange_id TEXT NOT NULL,
            rzye REAL, rzmre REAL, rzche REAL, rqye REAL,
            rqmcl REAL, rzrqye REAL, rqyl REAL, created_at REAL NOT NULL,
            PRIMARY KEY (trade_date, exchange_id)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_mg_trade_date ON margin_data(trade_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_mg_exchange_id ON margin_data(exchange_id)')
    
    # 2. 融资融券明细 (MarginDetailCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS margin_detail_data (
            trade_date TEXT NOT NULL, ts_code TEXT NOT NULL,
            rzye REAL, rzmre REAL, rzche REAL, rqye REAL, rqchl REAL,
            rqmcl REAL, rzrqye REAL, created_at REAL NOT NULL,
            PRIMARY KEY (trade_date, ts_code)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_md_trade_date ON margin_detail_data(trade_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_md_ts_code ON margin_detail_data(ts_code)')
    
    # 3. 机构调研 (StkSurvCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stk_surv_data (
            ts_code TEXT NOT NULL, name TEXT, surv_date TEXT NOT NULL,
            fund_visitors TEXT, rece_place TEXT, rece_mode TEXT,
            rece_org TEXT, org_type TEXT, comp_rece TEXT, content TEXT, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, surv_date, fund_visitors, rece_org)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ss_ts_code ON stk_surv_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ss_surv_date ON stk_surv_data(surv_date)')
    
    # 4. 每日筹码及胜率 (CyqPerfCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cyq_perf_data (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            his_low REAL, his_high REAL, cost_5pct REAL, cost_15pct REAL,
            cost_50pct REAL, cost_85pct REAL, cost_95pct REAL,
            weight_avg REAL, winner_rate REAL, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cp_ts_code ON cyq_perf_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cp_trade_date ON cyq_perf_data(trade_date)')
    
    conn.commit()


# ==========================================
# Endpoints: 融资融券汇总 (margin_data)
# ==========================================

@router.post("/margin")
def save_margin(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO margin_data (
                    trade_date, exchange_id, rzye, rzmre, rzche,
                    rqye, rqmcl, rzrqye, rqyl, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('trade_date', '')), str(row.get('exchange_id', '')),
                row.get('rzye'), row.get('rzmre'), row.get('rzche'),
                row.get('rqye'), row.get('rqmcl'), row.get('rzrqye'), row.get('rqyl'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/margin")
def get_margin(trade_date: Optional[str] = None, exchange_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("trade_date >= ?"); params.append(start_date)
        if end_date: conds.append("trade_date <= ?"); params.append(end_date)
    if exchange_id: conds.append("exchange_id = ?"); params.append(exchange_id)
    
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM margin_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/margin/has_data")
def margin_has_data(trade_date: Optional[str] = None, exchange_id: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    if exchange_id: conds.append("exchange_id = ?"); params.append(exchange_id)
    where = " AND ".join(conds) if conds else "1=1"
    cursor.execute(f"SELECT COUNT(*) FROM margin_data WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/margin/date_range")
def margin_date_range(exchange_id: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    if exchange_id:
        cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM margin_data WHERE exchange_id=?", (exchange_id,))
    else:
        cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM margin_data")
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/margin/stats")
def margin_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM margin_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT exchange_id) FROM margin_data")
    ec = cursor.fetchone()[0]
    return {"total_records": t, "exchange_count": ec}


# ==========================================
# Endpoints: 融资融券明细 (margin_detail_data)
# ==========================================

@router.post("/margin_detail")
def save_margin_detail(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO margin_detail_data (
                    trade_date, ts_code, rzye, rzmre, rzche,
                    rqye, rqchl, rqmcl, rzrqye, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('trade_date', '')), str(row.get('ts_code', '')),
                row.get('rzye'), row.get('rzmre'), row.get('rzche'),
                row.get('rqye'), row.get('rqchl'), row.get('rqmcl'), row.get('rzrqye'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/margin_detail")
def get_margin_detail(ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
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
    q = f"SELECT * FROM margin_detail_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/margin_detail/has_data")
def margin_detail_has_data(ts_code: Optional[str] = None, trade_date: Optional[str] = None):
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
    where = " AND ".join(conds) if conds else "1=1"
    cursor.execute(f"SELECT COUNT(*) FROM margin_detail_data WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/margin_detail/date_range")
def margin_detail_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM margin_detail_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/margin_detail/stats")
def margin_detail_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM margin_detail_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM margin_detail_data")
    sc = cursor.fetchone()[0]
    return {"total_records": t, "stock_count": sc}


# ==========================================
# Endpoints: 机构调研 (stk_surv_data)
# ==========================================

@router.post("/stk_surv")
def save_stk_surv(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            fund_v = str(row.get('fund_visitors', '')) if row.get('fund_visitors') is not None else ''
            rece_o = str(row.get('rece_org', '')) if row.get('rece_org') is not None else ''
            cursor.execute('''
                INSERT OR REPLACE INTO stk_surv_data (
                    ts_code, name, surv_date, fund_visitors, rece_place,
                    rece_mode, rece_org, org_type, comp_rece, content, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), row.get('name'), str(row.get('surv_date', '')),
                fund_v, row.get('rece_place'), row.get('rece_mode'),
                rece_o, row.get('org_type'), row.get('comp_rece'),
                row.get('content'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/stk_surv")
def get_stk_surv(ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
        if len(codes) == 1:
            conds.append("ts_code = ?"); params.append(codes[0])
        else:
            conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if trade_date: conds.append("surv_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("surv_date >= ?"); params.append(start_date)
        if end_date: conds.append("surv_date <= ?"); params.append(end_date)
    
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM stk_surv_data WHERE {where} ORDER BY surv_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/stk_surv/has_data")
def stk_surv_has_data(ts_code: Optional[str] = None, trade_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
        if len(codes) == 1:
            conds.append("ts_code = ?"); params.append(codes[0])
        else:
            conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    if trade_date: conds.append("surv_date = ?"); params.append(trade_date)
    where = " AND ".join(conds) if conds else "1=1"
    cursor.execute(f"SELECT COUNT(*) FROM stk_surv_data WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/stk_surv/date_range")
def stk_surv_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(surv_date), MAX(surv_date) FROM stk_surv_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/stk_surv/stats")
def stk_surv_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stk_surv_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM stk_surv_data")
    sc = cursor.fetchone()[0]
    return {"total_records": t, "stock_count": sc}


# ==========================================
# Endpoints: 每日筹码及胜率 (cyq_perf_data)
# ==========================================

@router.post("/cyq_perf")
def save_cyq_perf(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO cyq_perf_data (
                    ts_code, trade_date, his_low, his_high, cost_5pct,
                    cost_15pct, cost_50pct, cost_85pct, cost_95pct,
                    weight_avg, winner_rate, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                row.get('his_low'), row.get('his_high'), row.get('cost_5pct'),
                row.get('cost_15pct'), row.get('cost_50pct'), row.get('cost_85pct'),
                row.get('cost_95pct'), row.get('weight_avg'), row.get('winner_rate'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/cyq_perf")
def get_cyq_perf(ts_code: str, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = ["ts_code = ?"]; params = [ts_code]
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    elif start_date or end_date:
        if start_date: conds.append("trade_date >= ?"); params.append(start_date)
        if end_date: conds.append("trade_date <= ?"); params.append(end_date)
    
    where = " AND ".join(conds)
    q = f"SELECT * FROM cyq_perf_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/cyq_perf/has_data")
def cyq_perf_has_data(ts_code: str, trade_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    if trade_date:
        cursor.execute("SELECT COUNT(*) FROM cyq_perf_data WHERE ts_code=? AND trade_date=?", (ts_code, trade_date))
    else:
        cursor.execute("SELECT COUNT(*) FROM cyq_perf_data WHERE ts_code=?", (ts_code,))
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/cyq_perf/date_range")
def cyq_perf_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM cyq_perf_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/cyq_perf/stats")
def cyq_perf_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM cyq_perf_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM cyq_perf_data")
    sc = cursor.fetchone()[0]
    return {"total_records": t, "stock_count": sc}
