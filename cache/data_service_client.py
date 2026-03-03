"""数据服务客户端适配层

对上层 Tools 暴露与现有 Cache Manager 完全相同的接口，
内部改为 HTTP 调用远程 Data Service。

使用方式：
    通过环境变量 USE_DATA_SERVICE=true 启用，
    在 cache/__init__.py 中根据开关决定导入本地 Manager 还是此客户端。
"""
import os
import json
import logging
from typing import Optional, Dict, List, Any
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# 数据服务基地址
DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8002")


# 共享 HTTP 客户端实例，避免重复创建连接池和初始化 SSL
_shared_client = httpx.Client(timeout=60.0)


class _BaseClient:
    """HTTP 客户端基类"""

    def __init__(self, base_url: str = DATA_SERVICE_URL):
        self.base_url = base_url.rstrip('/')
        self._client = _shared_client

    def _get(self, path: str, params: dict = None):
        resp = self._client.get(f"{self.base_url}{path}", params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json_data: dict = None):
        resp = self._client.post(f"{self.base_url}{path}", json=json_data)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        # 共享客户端不建议单个实例关闭，如果是全局关闭，调用 close_all()
        pass


def close_all():
    """关闭共享 HTTP 客户端"""
    _shared_client.close()


# ============================================================
# CacheManager 客户端 — 替代 cache.cache_manager
# ============================================================

class CacheManagerClient(_BaseClient):
    """替代 CacheManager，接口完全一致"""

    def is_expired(self, cache_type: str, **params) -> bool:
        params_json = json.dumps(params, sort_keys=True, ensure_ascii=False)
        result = self._get(f"/api/cache/{cache_type}/expired", {"params_json": params_json})
        return result.get("expired", True)

    def get_dataframe(self, cache_type: str, **params) -> Optional[pd.DataFrame]:
        params_json = json.dumps(params, sort_keys=True, ensure_ascii=False)
        result = self._get(f"/api/cache/{cache_type}", {"params_json": params_json})
        if result.get("success") and result.get("data"):
            return pd.DataFrame(result["data"])
        return None

    def set(self, cache_type: str, data: pd.DataFrame, **params) -> bool:
        if data.empty:
            return False
        params_json = json.dumps(params, sort_keys=True, ensure_ascii=False)
        records = json.loads(data.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post(f"/api/cache/{cache_type}", {
            "params_json": params_json,
            "data": records
        })
        return result.get("success", False)

    def clear(self, cache_type: Optional[str] = None) -> int:
        # 暂不实现远程清理
        return 0

    def cleanup_expired(self) -> int:
        result = self._post("/api/cache/cleanup/expired")
        return result.get("marked_expired", 0)

    def cleanup_duplicates(self) -> int:
        result = self._post("/api/cache/cleanup/duplicates")
        return result.get("cleaned", 0)

    def get_stats(self) -> dict:
        result = self._get("/api/cache/stats/all")
        return result.get("stats", {})


# ============================================================
# StockDailyCacheManager 客户端
# ============================================================

class StockDailyCacheManagerClient(_BaseClient):
    """替代 StockDailyCacheManager，接口完全一致"""

    def save_stock_daily_data(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/stock/daily", {"data": records})
        return result.get("count", 0)

    def get_stock_daily_data(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        params = {}
        if ts_code: params['ts_code'] = ts_code
        if trade_date: params['trade_date'] = trade_date
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date
        if limit: params['limit'] = limit
        params['order_by'] = order_by

        result = self._get("/api/stock/daily", params)
        if result.get("success") and result.get("data"):
            df = pd.DataFrame(result["data"])
            # 保持与原实现一致的数据类型
            numeric_cols = ['open', 'close', 'high', 'low', 'pre_close',
                            'change', 'pct_chg', 'vol', 'amount', 'created_at']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        return None

    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        params = {'ts_code': ts_code}
        if trade_date:
            params['trade_date'] = trade_date
        result = self._get("/api/stock/daily/has_data", params)
        return result.get("has_data", False)

    def is_cache_data_complete(self, ts_code: str, start_date=None, end_date=None) -> bool:
        if not start_date and not end_date:
            return True
        codes = [c.strip() for c in ts_code.split(',')]
        for code in codes:
            dr = self._get("/api/stock/daily/date_range", {"ts_code": code})
            if not dr.get("has_data"):
                return False
            if start_date and dr.get("start_date", "") > start_date:
                return False
            if end_date and dr.get("end_date", "") < end_date:
                return False
        return True

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/stock/daily/date_range", {"ts_code": ts_code})
        if result.get("has_data"):
            return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/stock/daily/stats")

    def clear_stock_daily_data(self, ts_code: Optional[str] = None) -> int:
        return 0  # 暂不实现远程清理


# ============================================================
# MappingCacheManager 客户端
# ============================================================

class MappingCacheManagerClient(_BaseClient):
    """替代 MappingCacheManager，接口完全一致"""

    def save_mapping(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/mapping", {"data": records})
        return result.get("count", 0)

    def get_mapping_by_code(self, ts_code: str) -> Optional[Dict]:
        result = self._get(f"/api/mapping/{ts_code}")
        if result.get("success") and result.get("data"):
            return result["data"]
        return None

    def search_by_sector(self, sector_type: str, sector_code: str) -> pd.DataFrame:
        result = self._get(f"/api/mapping/sector/{sector_type}/{sector_code}")
        if result.get("success") and result.get("data"):
            return pd.DataFrame(result["data"])
        return pd.DataFrame()

    def get_all_mapping(self) -> pd.DataFrame:
        result = self._get(f"/api/mapping/all/list")
        if result.get("success") and result.get("data"):
            return pd.DataFrame(result["data"])
        return pd.DataFrame()

    def get_mapping_count(self) -> int:
        result = self._get("/api/mapping/count/all")
        return result.get("count", 0)


# ============================================================
# StockIntradayCacheManager 客户端
# ============================================================

class StockIntradayCacheManagerClient(_BaseClient):
    """替代 StockIntradayCacheManager，接口完全一致"""

    def save_intraday_snapshot(self, df: pd.DataFrame, current_time_str: str = None) -> int:
        if df.empty:
            return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        params = {}
        if current_time_str:
            params['current_time_str'] = current_time_str
        result = self._post("/api/stock/intraday" + (f"?current_time_str={current_time_str}" if current_time_str else ""), {"data": records})
        return result.get("count", 0)

    def get_historical_snapshot(self, ts_code: str, trade_date: str, trade_time: str) -> Optional[Dict]:
        result = self._get("/api/stock/intraday/historical_snapshot", {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "trade_time": trade_time
        })
        if result.get("success") and result.get("data"):
            return result["data"][0]
        return None

    def get_all_snapshots_for_time(self, trade_date: str, trade_time: str) -> pd.DataFrame:
        result = self._get("/api/stock/intraday/all_snapshots_for_time", {
            "trade_date": trade_date,
            "trade_time": trade_time
        })
        if result.get("success") and result.get("data"):
            return pd.DataFrame(result["data"])
        return pd.DataFrame()


# ============================================================
# IndexCacheManager 客户端
# ============================================================

class IndexCacheManagerClient(_BaseClient):
    """替代 IndexCacheManager，接口完全一致"""

    def save_index_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/index_market/global_index", {"data": records})
        return result.get("count", 0)

    def get_index_data(self, ts_code: str, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"ts_code": ts_code, "order_by": order_by}
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/index_market/global_index", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        params = {"ts_code": ts_code}
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/index_market/global_index/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/index_market/global_index/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/index_market/global_index/stats")

    def clear_index_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# IndexDailyCacheManager 客户端
# ============================================================

class IndexDailyCacheManagerClient(_BaseClient):
    """替代 IndexDailyCacheManager"""

    def save_index_daily_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/index_market/index_daily", {"data": records})
        return result.get("count", 0)

    def get_index_daily_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/index_market/index_daily", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        codes = [c.strip() for c in ts_code.split(',')]
        for code in codes:
            dr = self.get_date_range(code)
            if not dr: return False
            if start_date and start_date < dr.get("start_date", ""): return False
            if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        params = {"ts_code": ts_code}
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/index_market/index_daily/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/index_market/index_daily/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/index_market/index_daily/stats")

    def clear_index_daily_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# ConceptCacheManager 客户端
# ============================================================

class ConceptCacheManagerClient(_BaseClient):
    """替代 ConceptCacheManager"""

    def save_concept_index_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/index_market/concept/index", {"data": records})
        return result.get("count", 0)

    def get_concept_index_data(self, ts_code: Optional[str] = None, name: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, idx_type: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if name: params["name"] = name
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if idx_type: params["idx_type"] = idx_type
        if limit: params["limit"] = limit
        result = self._get("/api/index_market/concept/index", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def save_concept_daily_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/index_market/concept/daily", {"data": records})
        return result.get("count", 0)

    def get_concept_daily_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, idx_type: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if idx_type: params["idx_type"] = idx_type
        if limit: params["limit"] = limit
        result = self._get("/api/index_market/concept/daily", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def save_concept_member_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/index_market/concept/member", {"data": records})
        return result.get("count", 0)

    def get_concept_member_data(self, ts_code: Optional[str] = None, con_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if con_code: params["con_code"] = con_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit: params["limit"] = limit
        result = self._get("/api/index_market/concept/member", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def has_concept_index_data(self, trade_date: Optional[str] = None) -> bool:
        params = {"layer": "index"}
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/index_market/concept/has_data", params)
        return result.get("has_data", False)

    def has_concept_daily_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None) -> bool:
        params = {"layer": "daily"}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/index_market/concept/has_data", params)
        return result.get("has_data", False)

    def has_concept_member_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None) -> bool:
        params = {"layer": "member"}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/index_market/concept/has_data", params)
        return result.get("has_data", False)

    def get_board_name_map(self, ts_codes: List[str], board_type: str) -> Dict[str, str]:
        if not ts_codes: return {}
        result = self._get("/api/index_market/concept/board_map", {"ts_codes": ",".join(ts_codes), "board_type": board_type})
        if result.get("success") and result.get("data"): return result["data"]
        return {}

    def save_board_name_map(self, name_map: Dict[str, str], board_type: str) -> int:
        if not name_map: return 0
        result = self._post("/api/index_market/concept/board_map", {"name_map": name_map, "board_type": board_type})
        return result.get("count", 0)


# ============================================================
# SectorStrengthCacheManager 客户端
# ============================================================

class SectorStrengthCacheManagerClient(_BaseClient):
    """替代 SectorStrengthCacheManager，接口完全一致"""

    def save_strength_snapshots(self, df: pd.DataFrame, sector_type: str,
                                 trade_date: str, trade_time: str) -> int:
        if df.empty:
            return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/sector-strength", {
            "data": records,
            "sector_type": sector_type,
            "trade_date": trade_date,
            "trade_time": trade_time
        })
        return result.get("count", 0)

    def get_strength_history(self, sector_name: str, trade_date: str) -> pd.DataFrame:
        result = self._get("/api/sector-strength/history", {
            "sector_name": sector_name,
            "trade_date": trade_date
        })
        if result.get("success") and result.get("data"):
            return pd.DataFrame(result["data"])
        return pd.DataFrame()


# ============================================================
# DailyBasicCacheManager 客户端
# ============================================================

class DailyBasicCacheManagerClient(_BaseClient):
    """替代 DailyBasicCacheManager"""

    def save_daily_basic_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/fundamentals/daily_basic", {"data": records})
        return result.get("count", 0)

    def get_daily_basic_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/fundamentals/daily_basic", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        codes = [c.strip() for c in ts_code.split(',')]
        for code in codes:
            dr = self.get_date_range(code)
            if not dr: return False
            if start_date and start_date < dr.get("start_date", ""): return False
            if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None) -> bool:
        params = {}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/fundamentals/daily_basic/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/fundamentals/daily_basic/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/fundamentals/daily_basic/stats")

    def clear_daily_basic_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# FinaIndicatorCacheManager 客户端
# ============================================================

class FinaIndicatorCacheManagerClient(_BaseClient):
    """替代 FinaIndicatorCacheManager"""

    def save_fina_indicator_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/fundamentals/fina_indicator", {"data": records})
        return result.get("count", 0)

    def get_fina_indicator_data(self, ts_code: Optional[str] = None, ann_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if ann_date: params["ann_date"] = ann_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/fundamentals/fina_indicator", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def has_data(self, ts_code: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        params = {}
        if ts_code: params["ts_code"] = ts_code
        if end_date: params["end_date"] = end_date
        result = self._get("/api/fundamentals/fina_indicator/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/fundamentals/fina_indicator/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/fundamentals/fina_indicator/stats")

    def clear_fina_indicator_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# MarginCacheManager 客户端
# ============================================================

class MarginCacheManagerClient(_BaseClient):
    """替代 MarginCacheManager"""

    def save_margin_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/sentiment_funds/margin", {"data": records})
        return result.get("count", 0)

    def get_margin_data(self, trade_date: Optional[str] = None, exchange_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if trade_date: params["trade_date"] = trade_date
        if exchange_id: params["exchange_id"] = exchange_id
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/sentiment_funds/margin", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, exchange_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        dr = self.get_date_range(exchange_id)
        if not dr: return False
        if start_date and start_date < dr.get("start_date", ""): return False
        if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, trade_date: Optional[str] = None, exchange_id: Optional[str] = None) -> bool:
        params = {}
        if trade_date: params["trade_date"] = trade_date
        if exchange_id: params["exchange_id"] = exchange_id
        result = self._get("/api/sentiment_funds/margin/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, exchange_id: Optional[str] = None) -> Optional[Dict[str, str]]:
        params = {"exchange_id": exchange_id} if exchange_id else {}
        result = self._get("/api/sentiment_funds/margin/date_range", params)
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/sentiment_funds/margin/stats")

    def clear_margin_data(self, exchange_id: Optional[str] = None) -> int:
        return 0


# ============================================================
# MarginDetailCacheManager 客户端
# ============================================================

class MarginDetailCacheManagerClient(_BaseClient):
    """替代 MarginDetailCacheManager"""

    def save_margin_detail_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/sentiment_funds/margin_detail", {"data": records})
        return result.get("count", 0)

    def get_margin_detail_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/sentiment_funds/margin_detail", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        codes = [c.strip() for c in ts_code.split(',')]
        for code in codes:
            dr = self.get_date_range(code)
            if not dr: return False
            if start_date and start_date < dr.get("start_date", ""): return False
            if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None) -> bool:
        params = {}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/sentiment_funds/margin_detail/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/sentiment_funds/margin_detail/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/sentiment_funds/margin_detail/stats")

    def clear_margin_detail_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# StkSurvCacheManager 客户端
# ============================================================

class StkSurvCacheManagerClient(_BaseClient):
    """替代 StkSurvCacheManager"""

    def save_stk_surv_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/sentiment_funds/stk_surv", {"data": records})
        return result.get("count", 0)

    def get_stk_surv_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/sentiment_funds/stk_surv", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        codes = [c.strip() for c in ts_code.split(',')]
        for code in codes:
            dr = self.get_date_range(code)
            if not dr: return False
            if start_date and start_date < dr.get("start_date", ""): return False
            if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None) -> bool:
        params = {}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/sentiment_funds/stk_surv/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/sentiment_funds/stk_surv/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/sentiment_funds/stk_surv/stats")

    def clear_stk_surv_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# CyqPerfCacheManager 客户端
# ============================================================

class CyqPerfCacheManagerClient(_BaseClient):
    """替代 CyqPerfCacheManager"""

    def save_cyq_perf_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/sentiment_funds/cyq_perf", {"data": records})
        return result.get("count", 0)

    def get_cyq_perf_data(self, ts_code: str, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"ts_code": ts_code, "order_by": order_by}
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/sentiment_funds/cyq_perf", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        dr = self.get_date_range(ts_code)
        if not dr: return False
        if start_date and start_date < dr.get("start_date", ""): return False
        if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        params = {"ts_code": ts_code}
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/sentiment_funds/cyq_perf/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/sentiment_funds/cyq_perf/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/sentiment_funds/cyq_perf/stats")

    def clear_cyq_perf_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# StockWeeklyCacheManager 客户端
# ============================================================

class StockWeeklyCacheManagerClient(_BaseClient):
    """替代 StockWeeklyCacheManager"""

    def save_stock_weekly_data(self, df: pd.DataFrame) -> int:
        if df.empty: return 0
        records = json.loads(df.to_json(orient='records', force_ascii=False, default_handler=str))
        result = self._post("/api/stock_weekly", {"data": records})
        return result.get("count", 0)

    def get_stock_weekly_data(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None, order_by: str = 'DESC') -> Optional[pd.DataFrame]:
        params = {"order_by": order_by}
        if ts_code: params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date: params["end_date"] = end_date
        if limit is not None: params["limit"] = limit
        result = self._get("/api/stock_weekly", params)
        if result.get("success") and result.get("data"): return pd.DataFrame(result["data"])
        return None

    def is_cache_data_complete(self, ts_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        if not start_date and not end_date: return True
        codes = [c.strip() for c in ts_code.split(',')]
        for code in codes:
            dr = self.get_date_range(code)
            if not dr: return False
            if start_date and start_date < dr.get("start_date", ""): return False
            if end_date and end_date > dr.get("end_date", ""): return False
        return True

    def has_data(self, ts_code: str, trade_date: Optional[str] = None) -> bool:
        params = {"ts_code": ts_code}
        if trade_date: params["trade_date"] = trade_date
        result = self._get("/api/stock_weekly/has_data", params)
        return result.get("has_data", False)

    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        result = self._get("/api/stock_weekly/date_range", {"ts_code": ts_code})
        if result.get("has_data"): return {"start_date": result["start_date"], "end_date": result["end_date"]}
        return None

    def get_stats(self) -> Dict:
        return self._get("/api/stock_weekly/stats")

    def clear_stock_weekly_data(self, ts_code: Optional[str] = None) -> int:
        return 0


# ============================================================
# 全局实例（模块级常量，可被 cache/__init__.py 导入）
# ============================================================

cache_manager = CacheManagerClient()
stock_daily_cache_manager = StockDailyCacheManagerClient()
mapping_cache_manager = MappingCacheManagerClient()
sector_strength_cache_manager = SectorStrengthCacheManagerClient()
stock_intraday_cache_manager = StockIntradayCacheManagerClient()
index_cache_manager = IndexCacheManagerClient()
index_daily_cache_manager = IndexDailyCacheManagerClient()
concept_cache_manager = ConceptCacheManagerClient()
daily_basic_cache_manager = DailyBasicCacheManagerClient()
fina_indicator_cache_manager = FinaIndicatorCacheManagerClient()
margin_cache_manager = MarginCacheManagerClient()
margin_detail_cache_manager = MarginDetailCacheManagerClient()
stk_surv_cache_manager = StkSurvCacheManagerClient()
cyq_perf_cache_manager = CyqPerfCacheManagerClient()
stock_weekly_cache_manager = StockWeeklyCacheManagerClient()
