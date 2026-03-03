"""股票分时快照数据路由 — 对应 StockIntradayCacheManager"""
import time
from typing import Optional
from fastapi import APIRouter
from data_service.db.connection import db_manager
from data_service.models.common import StockDataSaveRequest, StockDataResponse

router = APIRouter(prefix="/api/stock/intraday", tags=["stock_intraday"])


def _init_stock_intraday_table():
    """初始化股票分时快照数据表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_intraday_data (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            trade_time TEXT NOT NULL,
            open REAL, close REAL, high REAL, low REAL,
            vol REAL, amount REAL, num INTEGER,
            bid_price1 REAL, bid_volume1 REAL, ask_price1 REAL, ask_volume1 REAL,
            created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date, trade_time)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_intraday_ts_date ON stock_intraday_data(ts_code, trade_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_intraday_time ON stock_intraday_data(trade_time)')
    conn.commit()


@router.post("")
def save_intraday_snapshot(request: StockDataSaveRequest, current_time_str: Optional[str] = None):
    """保存分时快照数据"""
    if not request.data:
        return StockDataResponse(success=False, count=0)
        
    if not current_time_str:
        current_time_str = time.strftime("%H:%M:%S")

    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0

    for row in request.data:
        try:
            trade_date = str(row.get('trade_date', time.strftime("%Y%m%d")))
            cursor.execute('''
                INSERT OR IGNORE INTO stock_intraday_data (
                    ts_code, trade_date, trade_time, open, close, high, low,
                    vol, amount, num, bid_price1, bid_volume1, ask_price1, ask_volume1, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), trade_date, current_time_str,
                row.get('open'), row.get('close'), row.get('high'), row.get('low'),
                row.get('vol'), row.get('amount'), row.get('num'),
                row.get('bid_price1'), row.get('bid_volume1'),
                row.get('ask_price1'), row.get('ask_volume1'), now_ts
            ))
            saved += 1
        except Exception:
            continue

    conn.commit()
    return StockDataResponse(success=True, count=saved)


@router.get("/historical_snapshot")
def get_historical_snapshot(ts_code: str, trade_date: str, trade_time: str):
    """查询特定日期、特定时刻最接近的快照"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM stock_intraday_data 
        WHERE ts_code = ? AND trade_date = ? AND trade_time <= ?
        ORDER BY trade_time DESC LIMIT 1
    ''', (ts_code, trade_date, trade_time))
    
    row = cursor.fetchone()
    if not row:
        return StockDataResponse(success=True, data=[], count=0)
        
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, row))]
    return StockDataResponse(success=True, data=data, count=1)


@router.get("/all_snapshots_for_time")
def get_all_snapshots_for_time(trade_date: str, trade_time: str):
    """获取指定日期和时刻的所有股票快照数据"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    query = '''
        SELECT s1.* FROM stock_intraday_data s1
        INNER JOIN (
            SELECT ts_code, MAX(trade_time) as max_time
            FROM stock_intraday_data
            WHERE trade_date = ? AND trade_time <= ?
            GROUP BY ts_code
        ) s2 ON s1.ts_code = s2.ts_code AND s1.trade_time = s2.max_time
        WHERE s1.trade_date = ?
    '''
    cursor.execute(query, (trade_date, trade_time, trade_date))
    rows = cursor.fetchall()
    
    if not rows:
        return StockDataResponse(success=True, data=[], count=0)
        
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, r)) for r in rows]
    return StockDataResponse(success=True, data=data, count=len(data))
