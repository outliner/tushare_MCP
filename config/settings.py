"""项目配置常量"""
from pathlib import Path

# 环境变量文件路径
# 优先使用项目根目录的 .env 文件
LOCAL_ENV_FILE = Path(__file__).parent.parent / ".env"
USER_ENV_FILE = Path.home() / ".tushare_mcp" / ".env"

# 缓存配置
CACHE_DIR = Path(__file__).parent.parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_DB = CACHE_DIR / "cache.db"

# 缓存过期时间（秒）
CACHE_TTL = {
    'stock_basic': 24 * 3600,           # 股票基本信息：24小时
    'stock_search': 12 * 3600,          # 股票搜索：12小时
    'income_statement': 7 * 24 * 3600,  # 财务报表：7天
    'global_index': 24 * 3600,          # 国际指数：24小时
    'fx_daily': 24 * 3600,              # 外汇日线行情：24小时
    'sw_industry_daily': 24 * 3600,     # 申万行业指数日线：24小时
    'index_classify': 7 * 24 * 3600,    # 行业分类：7天
}

