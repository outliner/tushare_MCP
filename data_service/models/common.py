"""通用请求/响应模型"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel


# ========== 通用缓存 ==========

class CacheGetRequest(BaseModel):
    """通用缓存查询请求"""
    cache_type: str
    params: Dict[str, Any] = {}


class CacheSetRequest(BaseModel):
    """通用缓存写入请求"""
    cache_type: str
    data: List[Dict[str, Any]]  # DataFrame records 格式
    params: Dict[str, Any] = {}


class CacheResponse(BaseModel):
    """通用缓存响应"""
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    is_expired: bool = False
    message: str = ""


class CacheStatsResponse(BaseModel):
    """缓存统计响应"""
    stats: Dict[str, Any]


# ========== Stock Daily / Weekly ==========

class StockDataQuery(BaseModel):
    """股票行情数据查询参数"""
    ts_code: Optional[str] = None
    trade_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: Optional[int] = None
    order_by: str = "DESC"


class StockDataSaveRequest(BaseModel):
    """股票行情数据写入请求"""
    data: List[Dict[str, Any]]


class StockDataResponse(BaseModel):
    """股票行情数据响应"""
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    count: int = 0
    message: str = ""


class DateRangeResponse(BaseModel):
    """日期范围响应"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    has_data: bool = False


# ========== Mapping ==========

class MappingSaveRequest(BaseModel):
    """映射数据写入请求"""
    data: List[Dict[str, Any]]


class MappingResponse(BaseModel):
    """映射数据响应"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    count: int = 0


# ========== Sector Strength ==========

class SectorStrengthSaveRequest(BaseModel):
    """板块强度数据写入请求"""
    data: List[Dict[str, Any]]
    sector_type: str
    trade_date: str
    trade_time: str


class SectorStrengthResponse(BaseModel):
    """板块强度数据响应"""
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    count: int = 0


# ========== Health ==========

class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str = "healthy"
    service: str = "tushare-data-service"
    version: str = "1.0.0"
    db_path: str = ""
    tables: List[str] = []
