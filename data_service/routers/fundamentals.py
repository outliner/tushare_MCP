"""基本面数据路由 — 对应 DailyBasic 和 FinaIndicator CacheManager"""
import time
from typing import Optional, List, Dict, Any
from fastapi import APIRouter
from pydantic import BaseModel
from data_service.db.connection import db_manager
from data_service.models.common import StockDataSaveRequest, StockDataResponse

router = APIRouter(prefix="/api/fundamentals", tags=["fundamentals"])

class DateRangeResponse(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    has_data: bool = False

# ==========================================
# Table Initialization
# ==========================================
def _init_fundamentals_tables():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    
    # 1. 每日指标 (DailyBasicCacheManager)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_basic_data (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            close REAL, turnover_rate REAL, turnover_rate_f REAL, volume_ratio REAL,
            pe REAL, pe_ttm REAL, pb REAL, ps REAL, ps_ttm REAL,
            dv_ratio REAL, dv_ttm REAL, total_share REAL, float_share REAL,
            free_share REAL, total_mv REAL, circ_mv REAL, created_at REAL NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_db_ts_code ON daily_basic_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_db_trade_date ON daily_basic_data(trade_date)')
    
    # 2. 财务指标 (FinaIndicatorCacheManager)
    # 我们预建立所有的字段，方便插入
    fina_columns = [
        "ts_code TEXT NOT NULL", "ann_date TEXT", "end_date TEXT NOT NULL",
        "eps REAL", "dt_eps REAL", "total_revenue_ps REAL", "revenue_ps REAL",
        "capital_rese_ps REAL", "surplus_rese_ps REAL", "undist_profit_ps REAL",
        "extra_item REAL", "profit_dedt REAL", "gross_margin REAL", "current_ratio REAL",
        "quick_ratio REAL", "cash_ratio REAL", "invturn_days REAL", "arturn_days REAL",
        "inv_turn REAL", "ar_turn REAL", "ca_turn REAL", "fa_turn REAL", "assets_turn REAL",
        "op_income REAL", "valuechange_income REAL", "interst_income REAL", "daa REAL",
        "ebit REAL", "ebitda REAL", "fcff REAL", "fcfe REAL", "current_exint REAL",
        "noncurrent_exint REAL", "interestdebt REAL", "netdebt REAL", "tangible_asset REAL",
        "working_capital REAL", "networking_capital REAL", "invest_capital REAL",
        "retained_earnings REAL", "diluted2_eps REAL", "bps REAL", "ocfps REAL",
        "retainedps REAL", "cfps REAL", "ebit_ps REAL", "fcff_ps REAL", "fcfe_ps REAL",
        "netprofit_margin REAL", "grossprofit_margin REAL", "cost_of_sales REAL",
        "profit_of_sales REAL", "roe REAL", "roa REAL", "roic REAL", "roe_yearly REAL",
        "roa2_yearly REAL", "roe_avg REAL", "opincome_of_ebt REAL", "investincome_of_ebt REAL",
        "n_op_profit_of_ebt REAL", "tax_to_ebt REAL", "dtprofit_to_profit REAL",
        "salescash_to_or REAL", "ocf_to_or REAL", "ocf_to_opincome REAL", "capitalized_to_da REAL",
        "debt_to_assets REAL", "assets_to_eqt REAL", "dp_assets_to_eqt REAL", "ca_to_assets REAL",
        "nca_to_assets REAL", "tbassets_to_totalassets REAL", "int_to_talcap REAL",
        "eqt_to_talcapital REAL", "currentdebt_to_debt REAL", "longdeb_to_debt REAL",
        "ocf_to_shortdebt REAL", "debtturn_days REAL", "or_yoy REAL", "q_gr_yoy REAL",
        "q_gr_qoq REAL", "q_sales_yoy REAL", "q_sales_qoq REAL", "q_op_yoy REAL",
        "q_op_qoq REAL", "q_profit_yoy REAL", "q_profit_qoq REAL", "q_netprofit_yoy REAL",
        "q_netprofit_qoq REAL", "equity_yoy REAL", "rd_exp REAL", "update_flag TEXT",
        "created_at REAL NOT NULL",
        "PRIMARY KEY (ts_code, end_date, ann_date)"
    ]
    cursor.execute(f"CREATE TABLE IF NOT EXISTS fina_indicator_data ({', '.join(fina_columns)})")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fi_ts_code ON fina_indicator_data(ts_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fi_end_date ON fina_indicator_data(end_date)')
    conn.commit()


# ==========================================
# Endpoints: 每日指标 (daily_basic_data)
# ==========================================

@router.post("/daily_basic")
def save_daily_basic(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    for row in request.data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO daily_basic_data (
                    ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                    volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
                    dv_ratio, dv_ttm, total_share, float_share, free_share,
                    total_mv, circ_mv, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(row.get('ts_code', '')), str(row.get('trade_date', '')),
                row.get('close'), row.get('turnover_rate'), row.get('turnover_rate_f'),
                row.get('volume_ratio'), row.get('pe'), row.get('pe_ttm'), row.get('pb'),
                row.get('ps'), row.get('ps_ttm'), row.get('dv_ratio'), row.get('dv_ttm'),
                row.get('total_share'), row.get('float_share'), row.get('free_share'),
                row.get('total_mv'), row.get('circ_mv'), now_ts
            ))
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/daily_basic")
def get_daily_basic(ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
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
    q = f"SELECT * FROM daily_basic_data WHERE {where} ORDER BY trade_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/daily_basic/has_data")
def daily_basic_has_data(ts_code: Optional[str] = None, trade_date: Optional[str] = None):
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
    cursor.execute(f"SELECT COUNT(*) FROM daily_basic_data WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/daily_basic/date_range")
def daily_basic_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_basic_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/daily_basic/stats")
def daily_basic_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM daily_basic_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_basic_data")
    sc = cursor.fetchone()[0]
    return {"total_records": t, "stock_count": sc}


# ==========================================
# Endpoints: 财务指标 (fina_indicator_data)
# ==========================================

@router.post("/fina_indicator")
def save_fina_indicator(request: StockDataSaveRequest):
    if not request.data: return StockDataResponse(success=False, count=0)
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    now_ts = time.time()
    saved = 0
    all_columns = [
        'ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps', 'total_revenue_ps', 'revenue_ps',
        'capital_rese_ps', 'surplus_rese_ps', 'undist_profit_ps', 'extra_item', 'profit_dedt',
        'gross_margin', 'current_ratio', 'quick_ratio', 'cash_ratio', 'invturn_days', 'arturn_days',
        'inv_turn', 'ar_turn', 'ca_turn', 'fa_turn', 'assets_turn', 'op_income', 'valuechange_income',
        'interst_income', 'daa', 'ebit', 'ebitda', 'fcff', 'fcfe', 'current_exint', 'noncurrent_exint',
        'interestdebt', 'netdebt', 'tangible_asset', 'working_capital', 'networking_capital',
        'invest_capital', 'retained_earnings', 'diluted2_eps', 'bps', 'ocfps', 'retainedps', 'cfps',
        'ebit_ps', 'fcff_ps', 'fcfe_ps', 'netprofit_margin', 'grossprofit_margin', 'cost_of_sales',
        'profit_of_sales', 'roe', 'roa', 'roic', 'roe_yearly', 'roa2_yearly', 'roe_avg',
        'opincome_of_ebt', 'investincome_of_ebt', 'n_op_profit_of_ebt', 'tax_to_ebt',
        'dtprofit_to_profit', 'salescash_to_or', 'ocf_to_or', 'ocf_to_opincome', 'capitalized_to_da',
        'debt_to_assets', 'assets_to_eqt', 'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets',
        'tbassets_to_totalassets', 'int_to_talcap', 'eqt_to_talcapital', 'currentdebt_to_debt',
        'longdeb_to_debt', 'ocf_to_shortdebt', 'debtturn_days', 'or_yoy', 'q_gr_yoy', 'q_gr_qoq',
        'q_sales_yoy', 'q_sales_qoq', 'q_op_yoy', 'q_op_qoq', 'q_profit_yoy', 'q_profit_qoq',
        'q_netprofit_yoy', 'q_netprofit_qoq', 'equity_yoy', 'rd_exp', 'update_flag'
    ]
    for row in request.data:
        if 'ts_code' not in row or 'end_date' not in row: continue
        try:
            # 建立要插入的实际列
            cols = ['ts_code', 'end_date', 'created_at']
            vals = [str(row['ts_code']), str(row['end_date']), now_ts]
            
            for col in all_columns:
                if col in ['ts_code', 'end_date']: continue
                cols.append(col)
                if col in row and row[col] is not None:
                    # ann_date e.g. might be string, others float
                    vals.append(str(row[col]) if col in ['ann_date', 'update_flag'] else row[col])
                else:
                    vals.append(None)
            
            p = ','.join(['?']*len(cols))
            c = ','.join(cols)
            cursor.execute(f"INSERT OR REPLACE INTO fina_indicator_data ({c}) VALUES ({p})", vals)
            saved += 1
        except Exception: continue
    conn.commit()
    return StockDataResponse(success=True, count=saved)

@router.get("/fina_indicator")
def get_fina_indicator(ts_code: Optional[str] = None, ann_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC'):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code: conds.append("ts_code = ?"); params.append(ts_code)
    if ann_date: conds.append("ann_date = ?"); params.append(ann_date)
    if start_date or end_date:
        if start_date: conds.append("end_date >= ?"); params.append(start_date)
        if end_date: conds.append("end_date <= ?"); params.append(end_date)
    where = " AND ".join(conds) if conds else "1=1"
    q = f"SELECT * FROM fina_indicator_data WHERE {where} ORDER BY end_date {order_by} " + (f"LIMIT {limit}" if limit else "")
    cursor.execute(q, params)
    rows = cursor.fetchall()
    if not rows: return StockDataResponse(success=True, data=[], count=0)
    cols = [c[0] for c in cursor.description]
    return StockDataResponse(success=True, data=[dict(zip(cols, r)) for r in rows], count=len(rows))

@router.get("/fina_indicator/has_data")
def fina_indicator_has_data(ts_code: Optional[str] = None, end_date: Optional[str] = None):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    conds = []; params = []
    if ts_code: conds.append("ts_code = ?"); params.append(ts_code)
    if end_date: conds.append("end_date = ?"); params.append(end_date)
    where = " AND ".join(conds) if conds else "1=1"
    cursor.execute(f"SELECT COUNT(*) FROM fina_indicator_data WHERE {where}", params)
    return {"has_data": cursor.fetchone()[0] > 0}

@router.get("/fina_indicator/date_range")
def fina_indicator_date_range(ts_code: str):
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(end_date), MAX(end_date) FROM fina_indicator_data WHERE ts_code=?", (ts_code,))
    row = cursor.fetchone()
    if row and row[0]: return DateRangeResponse(start_date=row[0], end_date=row[1], has_data=True)
    return DateRangeResponse()

@router.get("/fina_indicator/stats")
def fina_indicator_stats():
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM fina_indicator_data")
    t = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM fina_indicator_data")
    sc = cursor.fetchone()[0]
    return {"total_records": t, "stock_count": sc}
