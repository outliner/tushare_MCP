"""
Tushare MCP Data Service — FastAPI 主入口

独立的数据存取服务，可单独部署到阿里云。
启动方式：
    python -m data_service.app
    或：
    uvicorn data_service.app:app --host 0.0.0.0 --port 8001
"""
import sys
import io
import logging

# Windows UTF-8 兼容
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from data_service.config import DATA_SERVICE_HOST, DATA_SERVICE_PORT

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# 创建 FastAPI 应用
app = FastAPI(
    title="Tushare Data Service",
    description="独立数据存取服务 — 为 Tushare MCP Server 提供数据持久化",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _init_all_tables():
    """初始化所有数据库表"""
    from data_service.routers.cache import _init_cache_table
    from data_service.routers.stock_daily import _init_stock_daily_table
    from data_service.routers.mapping import _init_mapping_table
    from data_service.routers.sector_strength import _init_sector_strength_table
    from data_service.routers.stock_intraday import _init_stock_intraday_table

    _init_cache_table()
    _init_stock_daily_table()
    _init_mapping_table()
    _init_sector_strength_table()
    _init_stock_intraday_table()

    logger.info("✓ 所有数据库表初始化完成")


def _register_routers():
    """注册所有路由"""
    from data_service.routers import health, cache, stock_daily, mapping, sector_strength, stock_intraday

    app.include_router(health.router)
    app.include_router(cache.router)
    app.include_router(stock_daily.router)
    app.include_router(mapping.router)
    app.include_router(sector_strength.router)
    app.include_router(stock_intraday.router)

    logger.info("✓ 所有 API 路由注册完成")


@app.on_event("startup")
async def startup():
    """应用启动事件"""
    logger.info("=" * 60)
    logger.info("🚀 Tushare Data Service 正在启动...")
    logger.info("=" * 60)
    _init_all_tables()
    _register_routers()
    logger.info(f"📍 API 文档: http://{DATA_SERVICE_HOST}:{DATA_SERVICE_PORT}/docs")
    logger.info(f"📍 健康检查: http://{DATA_SERVICE_HOST}:{DATA_SERVICE_PORT}/health")
    logger.info("=" * 60)


@app.on_event("shutdown")
async def shutdown():
    """应用关闭事件"""
    from data_service.db.connection import db_manager
    db_manager.close()
    logger.info("✓ 数据库连接已关闭")


# 根路径
@app.get("/")
def root():
    return {
        "service": "Tushare Data Service",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    # 需要在启动前手动初始化（uvicorn 的 startup 事件会自动调用）
    _init_all_tables()
    _register_routers()

    print(f"\n🚀 启动 Tushare Data Service on {DATA_SERVICE_HOST}:{DATA_SERVICE_PORT}\n", file=sys.stderr)

    uvicorn.run(
        app,
        host=DATA_SERVICE_HOST,
        port=DATA_SERVICE_PORT,
        log_level="info",
        timeout_keep_alive=300,
    )
