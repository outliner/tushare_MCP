"""板块强度数据路由 — 对应 SectorStrengthCacheManager"""
import time
from fastapi import APIRouter
from data_service.db.connection import db_manager
from data_service.models.common import SectorStrengthSaveRequest, SectorStrengthResponse

router = APIRouter(prefix="/api/sector-strength", tags=["sector_strength"])


def _init_sector_strength_table():
    """初始化板块强度表"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_strength_data (
            sector_name TEXT NOT NULL,
            sector_type TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            trade_time TEXT NOT NULL,
            avg_pct REAL, rising_ratio REAL, median_return REAL,
            intraday_position REAL, real_body_ratio REAL,
            volume_ratio REAL, value_per_trade REAL, order_imbalance REAL,
            stock_count INTEGER, score REAL,
            created_at REAL NOT NULL,
            PRIMARY KEY (sector_name, trade_date, trade_time)
        )
    ''')
    # 兼容旧表
    new_cols = [('median_return', 'REAL'), ('intraday_position', 'REAL'),
                ('real_body_ratio', 'REAL'), ('value_per_trade', 'REAL'), ('order_imbalance', 'REAL')]
    for col_name, col_type in new_cols:
        try:
            cursor.execute(f'ALTER TABLE sector_strength_data ADD COLUMN {col_name} {col_type}')
        except:
            pass
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector_date_time ON sector_strength_data(sector_name, trade_date, trade_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector_type_date ON sector_strength_data(sector_type, trade_date)')
    conn.commit()


@router.get("/history", response_model=SectorStrengthResponse)
def get_history(sector_name: str, trade_date: str):
    """获取指定板块在特定日期的强度序列"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT trade_time, avg_pct, rising_ratio, median_return,
               intraday_position, real_body_ratio,
               volume_ratio, value_per_trade, order_imbalance,
               score, stock_count
        FROM sector_strength_data
        WHERE sector_name = ? AND trade_date = ?
        ORDER BY trade_time ASC
    ''', (sector_name, trade_date))
    rows = cursor.fetchall()
    columns = ['trade_time', 'avg_pct', 'rising_ratio', 'median_return',
               'intraday_position', 'real_body_ratio', 'volume_ratio',
               'value_per_trade', 'order_imbalance', 'score', 'stock_count']
    data = [dict(zip(columns, r)) for r in rows]
    return SectorStrengthResponse(success=True, data=data, count=len(data))


@router.post("", response_model=SectorStrengthResponse)
def save_snapshots(request: SectorStrengthSaveRequest):
    """保存板块强度快照"""
    if not request.data:
        return SectorStrengthResponse(success=False, count=0)

    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now = time.time()
    saved = 0

    for row in request.data:
        try:
            name = row.get('板块名称', row.get('sector_name', ''))
            avg_pct = float(str(row.get('平均涨幅', row.get('avg_pct', 0))).replace('%', ''))
            rising_ratio = float(str(row.get('上涨家数占比', row.get('rising_ratio', 0))).replace('%', ''))
            median_return = float(row.get('涨幅中位数', row.get('median_return', 0)))
            intraday_position = float(row.get('日内位阶', row.get('intraday_position', 0.5)))
            real_body_ratio = float(str(row.get('真阳线比例', row.get('real_body_ratio', 0))).replace('%', ''))
            vr = float(row.get('实时量比', row.get('volume_ratio', row.get('放量系数', 1.0))))
            value_per_trade = float(row.get('单笔成交额', row.get('value_per_trade', 0)))
            oi = row.get('委比', row.get('order_imbalance'))
            if oi is not None:
                oi = float(str(oi).replace('%', ''))
            count = int(row.get('成分股数', row.get('stock_count', 0)))
            score = float(row.get('score', 0))

            cursor.execute('''
                INSERT OR REPLACE INTO sector_strength_data (
                    sector_name, sector_type, trade_date, trade_time,
                    avg_pct, rising_ratio, median_return,
                    intraday_position, real_body_ratio,
                    volume_ratio, value_per_trade, order_imbalance,
                    stock_count, score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                name, request.sector_type, request.trade_date, request.trade_time,
                avg_pct, rising_ratio, median_return,
                intraday_position, real_body_ratio,
                vr, value_per_trade, oi,
                count, score, now
            ))
            saved += 1
        except Exception:
            continue

    conn.commit()
    return SectorStrengthResponse(success=True, count=saved)
