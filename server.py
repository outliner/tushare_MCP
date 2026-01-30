"""
Tushare MCP服务器主入口

该文件作为MCP服务器的启动入口，负责：
1. 初始化MCP服务器实例
2. 自动发现并注册所有工具
3. 注册提示模板
4. 启动服务器
"""
import os
import sys
import io
from pathlib import Path

# Force UTF-8 for stdout/stderr to handle emojis on Windows
if sys.platform == 'win32':
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding='utf-8')
    if isinstance(sys.stderr, io.TextIOWrapper):
        sys.stderr.reconfigure(encoding='utf-8')

import tushare as ts
from mcp.server.fastmcp import FastMCP

# 导入配置
from config.settings import LOCAL_ENV_FILE
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager
from tools import discover_tools

# 创建MCP服务器实例
mcp = FastMCP("Tushare Stock Info")

# 注册提示模板
@mcp.prompt()
def configure_token() -> str:
    """配置Tushare token的提示模板"""
    return """请提供您的Tushare API token。
您可以在 https://tushare.pro/user/token 获取您的token。
如果您还没有Tushare账号，请先在 https://tushare.pro/register 注册。

请输入您的token:"""

@mcp.prompt()
def income_statement_query() -> str:
    """利润表查询提示模板"""
    return """请提供以下信息来查询利润表：

1. 股票代码（必填，如：000001.SZ）

2. 时间范围（可选）：
   - 开始日期（YYYYMMDD格式，如：20230101）
   - 结束日期（YYYYMMDD格式，如：20231231）

3. 报告类型（可选，默认为合并报表）：
   1 = 合并报表（默认）
   2 = 单季合并
   3 = 调整单季合并表
   4 = 调整合并报表
   5 = 调整前合并报表
   6 = 母公司报表
   7 = 母公司单季表
   8 = 母公司调整单季表
   9 = 母公司调整表
   10 = 母公司调整前报表
   11 = 母公司调整前合并报表
   12 = 母公司调整前报表

示例查询：
1. 查询最新报表：
   "查询平安银行(000001.SZ)的最新利润表"

2. 查询指定时间范围：
   "查询平安银行2023年的利润表"
   "查询平安银行2023年第一季度的利润表"

3. 查询特定报表类型：
   "查询平安银行的母公司报表"
   "查询平安银行2023年的单季合并报表"

请告诉我您想查询的内容："""


if __name__ == "__main__":
    # 启动时自动尝试加载 token
    token = get_tushare_token()
    if token:
        try:
            ts.set_token(token)
            env_source = "项目本地 .env" if LOCAL_ENV_FILE.exists() and os.getenv("TUSHARE_TOKEN") else "用户目录 .env"
            print(f"已从 {env_source} 加载 Tushare token", file=sys.stderr)
        except Exception as e:
            print(f"加载 token 时出错: {str(e)}", file=sys.stderr)
    else:
        print("未找到 Tushare token，请使用 setup_tushare_token 工具进行配置", file=sys.stderr)
    
    # 启动时标记过期缓存（数据永久保留，只标记状态）
    try:
        expired_count = cache_manager.cleanup_expired()
        if expired_count > 0:
            print(f"已标记 {expired_count} 条过期缓存记录（数据已保留）", file=sys.stderr)
    except Exception as e:
        print(f"标记过期缓存时出错: {str(e)}", file=sys.stderr)
    
    # 启动时清理重复数据（可选，保留最新版本）
    try:
        duplicate_count = cache_manager.cleanup_duplicates()
        if duplicate_count > 0:
            print(f"已清理 {duplicate_count} 条重复缓存记录（已保留最新版本）", file=sys.stderr)
    except Exception as e:
        print(f"清理重复缓存时出错: {str(e)}", file=sys.stderr)
    
    # 自动发现并注册所有工具
    discover_tools(mcp)
    
    # 启动MCP服务器
    mcp.run()
