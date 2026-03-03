"""股票周线行情数据路由 — 对应 StockWeeklyCacheManager"""
import time
from typing import Optional, List, Dict, Any
from fastapi import APIRouter
from pydantic import BaseModel
from data_service.db.connection import db_manager
from data_service.models.common import StockDataSaveRequest, StockDataResponse

router = APIRouter(prefix="/api/stock_weekly", tags=["stock_weekly"])

class DateRangeResponse(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    has_data: bool = False

# ==========================================
# Table Initialization
# ==========================================

def _init_stock_weekly_tables():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_weekly_data (
            ts_code TEXT NOT NULL,           -- TS股票代码（如：000001.SZ）
            trade_date TEXT NOT NULL,        -- 交易日期（YYYYMMDD格式，周的最后交易日）
            open REAL,                       -- 周开盘价
            close REAL,                      -- 周收盘价
            high REAL,                       -- 周最高价
            low REAL,                        -- 周最低价
            pre_close REAL,                  -- 上周收盘价
            change REAL,                     -- 涨跌额
            pct_chg REAL,                    -- 涨跌幅（百分比）
            vol REAL,                        -- 周成交量（手）
            amount REAL,                     -- 周成交额（千元）
            created_at REAL NOT NULL,        -- 数据创建时间戳
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_wk_ts_code ON stock_weekly_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_wk_trade_date ON stock_weekly_data(trade_date)')
    
    conn.commit()


# ==========================================
# Endpoints: 股票周线行情 (stock_weekly_data)
# ==========================================

@router.post("")
def save_stock_weekly(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_weekly_data (
                    ts_code, trade_date, open, close, high, low,
                    pre_close, change, pct_chg, vol, amount, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                row.get('open'), row.get('close'), row.get('high'), row.get('low'),
                row.get('pre_close'), row.get('change'), row.get('pct_chg'),
                row.get('vol'), row.get('amount'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("")
def get_stock_weekly(ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
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
    q = f"SELECT * FROM stock_weekly_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/has_data")
def stock_weekly_has_data(ts_code: str, trade_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    codes = [c.strip() for c in ts_code.split(',')]
    conds = []; params = []
    if len(codes) == 1:
        conds.append("ts_code = ?"); params.append(codes[0])
    else:
        conds.append(f"ts_code IN ({','.join(['?'] * len(codes))})"); params.extend(codes)
    
    if trade_date: conds.append("trade_date = ?"); params.append(trade_date)
    where = " AND ".join(conds) if conds else "1=1"
    cursor.execute(f"SELECT COUNT(*) FROM stock_weekly_data WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/date_range")
def stock_weekly_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM stock_weekly_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/stats")
def stock_weekly_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_weekly_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM stock_weekly_data")
    sc = cursor.fetchone()[0]
    return {"total_records": t, "stock_count": sc}
