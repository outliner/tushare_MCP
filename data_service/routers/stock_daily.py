"""股票日线行情数据路由 — 对应 StockDailyCacheManager"""
import time
from typing import Optional
from fastapi import APIRouter
import pandas as pd
from data_service.db.connection import db_manager
from data_service.models.common import StockDataQuery, StockDataSaveRequest, StockDataResponse, DateRangeResponse

router = APIRouter(prefix="/api/stock/daily", tags=["stock_daily"])


def _init_stock_daily_table():
    """初始化股票日线行情数据表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily_data (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL, close REAL, high REAL, low REAL,
            pre_close REAL, change REAL, pct_chg REAL,
            vol REAL, amount REAL,
            created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_daily_ts_code ON stock_daily_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_daily_trade_date ON stock_daily_data(trade_date)')
    conn.commit()


@router.get("", response_model=StockDataResponse)
def get_stock_daily(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    order_by: str = "DESC"
):
    """查询股票日线行情数据"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    conditions, params = [], []
    if ts_code:
        codes = [c.strip() for c in ts_code.split(',')]
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
    else:
        if start_date:
            conditions.append('trade_date >= ?')
            params.append(start_date)
        if end_date:
            conditions.append('trade_date <= ?')
            params.append(end_date)

    where = ' AND '.join(conditions) if conditions else '1=1'
    limit_clause = f'LIMIT {limit}' if limit else ''
    query = f'''
        SELECT ts_code, trade_date, open, close, high, low,
               pre_close, change, pct_chg, vol, amount, created_at
        FROM stock_daily_data WHERE {where}
        ORDER BY trade_date {order_by} {limit_clause}
    '''
    cursor.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        return StockDataResponse(success=True, data=[], count=0)

    columns = ['ts_code', 'trade_date', 'open', 'close', 'high', 'low',
               'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'created_at']
    data = [dict(zip(columns, row)) for row in rows]
    return StockDataResponse(success=True, data=data, count=len(data))


@router.post("", response_model=StockDataResponse)
def save_stock_daily(request: StockDataSaveRequest):
    """保存股票日线行情数据"""
    if not request.data:
        return StockDataResponse(success=False, message="数据为空", count=0)

    conn = db_manager.get_connection()
    cursor = conn.cursor()
    current_time = time.time()
    saved = 0

    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_daily_data (
                    ts_code, trade_date, open, close, high, low,
                    pre_close, change, pct_chg, vol, amount, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')),
                str(row.get('trade_date', '')),
                row.get('open'), row.get('close'), row.get('high'), row.get('low'),
                row.get('pre_close'), row.get('change'), row.get('pct_chg'),
                row.get('vol'), row.get('amount'),
                current_time
            ))
            saved += 1
        except Exception:
            continue

    conn.commit()
    return StockDataResponse(success=True, count=saved, message=f"已保存 {saved} 条记录")


@router.get("/has_data")
def has_data(ts_code: str, trade_date: Optional[str] = None):
    """检查是否存在数据"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    codes = [c.strip() for c in ts_code.split(',')]
    placeholders = ','.join(['?'] * len(codes))

    if trade_date:
        cursor.execute(f'SELECT COUNT(*) FROM stock_daily_data WHERE ts_code IN ({placeholders}) AND trade_date = ?',
                       codes + [trade_date])
    else:
        cursor.execute(f'SELECT COUNT(*) FROM stock_daily_data WHERE ts_code IN ({placeholders})', codes)

    return {"has_data": cursor.fetchone()[0] > 0}


@router.get("/date_range", response_model=DateRangeResponse)
def get_date_range(ts_code: str):
    """获取数据日期范围"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily_data WHERE ts_code = ?', (ts_code,))
    row = cursor.fetchone()
    if row and row[0] and row[1]:
        return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse(has_data=False)


@router.get("/stats")
def get_stats():
    """获取股票日线数据统计"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT ts_code, COUNT(*) as cnt, MIN(trade_date), MAX(trade_date)
        FROM stock_daily_data GROUP BY ts_code ORDER BY ts_code
    ''')
    by_stock = {}
    for row in cursor.fetchall():
        by_stock[row[0]] = {'record_count': row[1], 'earliest_date': row[2], 'latest_date': row[3]}

    cursor.execute('SELECT COUNT(*) FROM stock_daily_data')
    total = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM stock_daily_data')
    stock_count = cursor.fetchone()[0]

    return {"by_stock": by_stock, "total": {"total_records": total, "stock_count": stock_count}}
