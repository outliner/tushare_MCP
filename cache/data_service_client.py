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
DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8001")


class _BaseClient:
    """HTTP 客户端基类"""

    def __init__(self, base_url: str = DATA_SERVICE_URL):
        self.base_url = base_url.rstrip('/')
        self._client = httpx.Client(timeout=60.0)

    def _get(self, path: str, params: dict = None):
        resp = self._client.get(f"{self.base_url}{path}", params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json_data: dict = None):
        resp = self._client.post(f"{self.base_url}{path}", json=json_data)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._client.close()


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

    def get_mapping_count(self) -> int:
        result = self._get("/api/mapping/count/all")
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
# 全局实例（模块级常量，可被 cache/__init__.py 导入）
# ============================================================

cache_manager = CacheManagerClient()
stock_daily_cache_manager = StockDailyCacheManagerClient()
mapping_cache_manager = MappingCacheManagerClient()
sector_strength_cache_manager = SectorStrengthCacheManagerClient()
