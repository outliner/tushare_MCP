"""Data Service 配置"""
import os
from pathlib import Path


# 数据库路径，优先从环境变量读取
DATA_SERVICE_DB_PATH = Path(
    os.getenv("DATA_SERVICE_DB_PATH", str(Path(__file__).parent.parent / ".cache" / "cache.db"))
)

# 确保父目录存在
DATA_SERVICE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# 服务端口
DATA_SERVICE_PORT = int(os.getenv("DATA_SERVICE_PORT", "8001"))

# 服务主机
DATA_SERVICE_HOST = os.getenv("DATA_SERVICE_HOST", "0.0.0.0")

# 缓存过期时间（秒）— 与 MCP Server 的 settings.py 保持一致
CACHE_TTL = {
    'stock_basic': 24 * 3600,
    'stock_search': 12 * 3600,
    'income_statement': 7 * 24 * 3600,
    'global_index': 24 * 3600,
    'fx_daily': 24 * 3600,
    'sw_industry_daily': 24 * 3600,
    'index_classify': 7 * 24 * 3600,
}
