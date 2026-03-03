"""缓存模块

通过环境变量 USE_DATA_SERVICE 控制数据来源：
  - USE_DATA_SERVICE=false (默认): 使用本地 SQLite 缓存管理器
  - USE_DATA_SERVICE=true: 使用远程 Data Service HTTP 客户端
"""
import os

_use_data_service = os.getenv("USE_DATA_SERVICE", "false").lower() == "true"

if _use_data_service:
    # ── 远程模式：通过 HTTP 访问独立的 Data Service ──
    from cache.data_service_client import (
        CacheManagerClient as CacheManager,
        cache_manager,
        StockDailyCacheManagerClient as StockDailyCacheManager,
        stock_daily_cache_manager,
        MappingCacheManagerClient as MappingCacheManager,
        mapping_cache_manager,
        SectorStrengthCacheManagerClient as SectorStrengthCacheManager,
        sector_strength_cache_manager,
        StockIntradayCacheManagerClient as StockIntradayCacheManager,
        stock_intraday_cache_manager,
    )
    # 以下管理器暂未适配远程模式，仍使用本地 SQLite
    from cache.index_cache_manager import IndexCacheManager, index_cache_manager
    from cache.index_daily_cache_manager import IndexDailyCacheManager, index_daily_cache_manager
    from cache.concept_cache_manager import ConceptCacheManager, concept_cache_manager
    from cache.margin_cache_manager import MarginCacheManager, margin_cache_manager
    from cache.margin_detail_cache_manager import MarginDetailCacheManager, margin_detail_cache_manager
    from cache.stk_surv_cache_manager import StkSurvCacheManager, stk_surv_cache_manager
    from cache.cyq_perf_cache_manager import CyqPerfCacheManager, cyq_perf_cache_manager
    from cache.daily_basic_cache_manager import DailyBasicCacheManager, daily_basic_cache_manager
    from cache.fina_indicator_cache_manager import FinaIndicatorCacheManager, fina_indicator_cache_manager
    from cache.stock_weekly_cache_manager import StockWeeklyCacheManager, stock_weekly_cache_manager

    import sys
    print("✓ 缓存模式: 远程 Data Service (USE_DATA_SERVICE=true)", file=sys.stderr)
else:
    # ── 本地模式：使用本地 SQLite（现有行为，完全不变） ──
    from cache.cache_manager import CacheManager, cache_manager
    from cache.index_cache_manager import IndexCacheManager, index_cache_manager
    from cache.index_daily_cache_manager import IndexDailyCacheManager, index_daily_cache_manager
    from cache.concept_cache_manager import ConceptCacheManager, concept_cache_manager
    from cache.margin_cache_manager import MarginCacheManager, margin_cache_manager
    from cache.margin_detail_cache_manager import MarginDetailCacheManager, margin_detail_cache_manager
    from cache.stk_surv_cache_manager import StkSurvCacheManager, stk_surv_cache_manager
    from cache.cyq_perf_cache_manager import CyqPerfCacheManager, cyq_perf_cache_manager
    from cache.daily_basic_cache_manager import DailyBasicCacheManager, daily_basic_cache_manager
    from cache.fina_indicator_cache_manager import FinaIndicatorCacheManager, fina_indicator_cache_manager
    from cache.stock_daily_cache_manager import StockDailyCacheManager, stock_daily_cache_manager
    from cache.stock_weekly_cache_manager import StockWeeklyCacheManager, stock_weekly_cache_manager


__all__ = [
    'CacheManager', 'cache_manager',
    'IndexCacheManager', 'index_cache_manager',
    'IndexDailyCacheManager', 'index_daily_cache_manager',
    'ConceptCacheManager', 'concept_cache_manager',
    'MarginCacheManager', 'margin_cache_manager',
    'MarginDetailCacheManager', 'margin_detail_cache_manager',
    'StkSurvCacheManager', 'stk_surv_cache_manager',
    'CyqPerfCacheManager', 'cyq_perf_cache_manager',
    'DailyBasicCacheManager', 'daily_basic_cache_manager',
    'FinaIndicatorCacheManager', 'fina_indicator_cache_manager',
    'StockDailyCacheManager', 'stock_daily_cache_manager',
    'StockWeeklyCacheManager', 'stock_weekly_cache_manager',
]
