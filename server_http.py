"""
Tushare MCP服务器 - Streamable HTTP 模式

该文件提供基于 Streamable HTTP 的 MCP 服务器实现。
相比 stdio 模式，HTTP 模式的优势：
1. 可以通过网络访问（本地或远程）
2. 支持多客户端并发连接
3. 更容易进行调试和监控
4. 可以与 Web 前端集成

启动方式：
    python server_http.py
    
    或使用 uvicorn:
    uvicorn server_http:app --host 127.0.0.1 --port 8000

配置方式（在 Claude Desktop 或其他 MCP 客户端）：
    {
      "mcpServers": {
        "tushare": {
          "url": "http://localhost:8000/mcp"
        }
      }
    }
"""
import os
import sys
import io
import traceback
import logging
import asyncio
import functools
from pathlib import Path
from typing import Callable
from concurrent.futures import ThreadPoolExecutor

# 修复 Windows 终端中文乱码问题
# 即使 bat 脚本设置了 chcp 65001 和 PYTHONIOENCODING，
# Python 的 stderr 仍可能使用系统默认编码（GBK）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import tushare as ts
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import uvicorn

# 导入配置
from config.settings import LOCAL_ENV_FILE
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager
from tools import discover_tools

# 导入 MCP 服务器核心
from mcp.server.fastmcp import FastMCP

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# 创建线程池用于执行同步工具调用，避免阻塞事件循环
_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="mcp_tool")


class ConnectionKeepAliveMiddleware(BaseHTTPMiddleware):
    """连接保持活跃中间件，确保SSE连接不会因为空闲而关闭"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # 对于SSE连接，添加必要的响应头
        if request.url.path.startswith("/mcp"):
            # 确保响应头包含连接保持活跃的设置
            if isinstance(response, StreamingResponse):
                # SSE连接需要这些头部
                response.headers["Cache-Control"] = "no-cache"
                response.headers["Connection"] = "keep-alive"
                response.headers["X-Accel-Buffering"] = "no"  # 禁用Nginx缓冲
            elif hasattr(response, 'headers'):
                response.headers["Connection"] = "keep-alive"
                response.headers["Keep-Alive"] = "timeout=300"
        
        return response


class ExceptionHandlerMiddleware(BaseHTTPMiddleware):
    """全局异常处理中间件，确保所有异常都被正确捕获和转换"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            logger.error(f"未处理的异常: {type(e).__name__}: {str(e)}", exc_info=True)
            
            # 如果是 MCP 端点，返回 JSON-RPC 错误响应
            if request.url.path.startswith("/mcp"):
                error_detail = traceback.format_exc()
                return JSONResponse(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}",
                            "data": error_detail
                        }
                    },
                    status_code=500
                )
            else:
                # 其他端点返回标准错误响应
                return JSONResponse(
                    {
                        "error": str(e),
                        "detail": traceback.format_exc()
                    },
                    status_code=500
                )


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """日志中间件，用于记录所有请求的头部，帮助调试 406 错误"""
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # 只记录关键端点的日志，避免刷屏
        if request.url.path in ["/mcp", "/health", "/tools"]:
            logger.info(f"收到请求: {request.method} {request.url.path}")
            logger.info(f"请求头部: {dict(request.headers)}")
        
        try:
            response = await call_next(request)
            if request.url.path in ["/mcp", "/health", "/tools"]:
                logger.info(f"响应状态: {response.status_code}")
            return response
        except Exception as e:
            logger.error(f"中间件处理出错: {str(e)}")
            raise


class ForceAcceptHeaderMiddleware(BaseHTTPMiddleware):
    """强制添加 Accept 头部中间件，解决某些客户端不发送正确头部导致的 406 错误"""
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path.startswith("/mcp"):
            # 如果是 MCP 端点且 Accept 头部不完整，补全它
            accept = request.headers.get("accept", "")
            if "text/event-stream" not in accept or "application/json" not in accept:
                # 复制并修改头部
                # 找到 accept 的索引或添加新的
                new_accept = b"application/json, text/event-stream"
                found = False
                new_headers = []
                for k, v in request.scope["headers"]:
                    if k.lower() == b"accept":
                        new_headers.append((k, new_accept))
                        found = True
                    else:
                        new_headers.append((k, v))
                if not found:
                    new_headers.append((b"accept", new_accept))
                
                request.scope["headers"] = new_headers
                logger.info("已强制重写 Accept 头部以匹配 MCP SDK 要求")
                
        return await call_next(request)


class TushareMCPServer:
    """Tushare MCP 服务器（Streamable HTTP 模式）"""
    
    def __init__(self):
        self.mcp = FastMCP("Tushare Stock Info")
        self.tools = {}
        self._initialize()
        self._wrap_sync_tools()
    
    def _initialize(self):
        """初始化服务器"""
        # 加载 Tushare Token
        token = get_tushare_token()
        if token:
            try:
                ts.set_token(token)
                env_source = "项目本地 .env" if LOCAL_ENV_FILE.exists() and os.getenv("TUSHARE_TOKEN") else "用户目录 .env"
                print(f"✓ 已从 {env_source} 加载 Tushare token", file=sys.stderr)
            except Exception as e:
                print(f"⚠️  加载 token 时出错: {str(e)}", file=sys.stderr)
        else:
            print("⚠️  未找到 Tushare token，部分功能可能无法使用", file=sys.stderr)
        
        # 清理过期缓存
        try:
            expired_count = cache_manager.cleanup_expired()
            if expired_count > 0:
                print(f"✓ 已标记 {expired_count} 条过期缓存记录", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  标记过期缓存时出错: {str(e)}", file=sys.stderr)
        
        # 清理重复数据
        try:
            duplicate_count = cache_manager.cleanup_duplicates()
            if duplicate_count > 0:
                print(f"✓ 已清理 {duplicate_count} 条重复缓存记录", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  清理重复缓存时出错: {str(e)}", file=sys.stderr)
        
        # 自动发现并注册所有工具
        print("\n" + "="*60, file=sys.stderr)
        print("正在注册 MCP 工具...", file=sys.stderr)
        print("="*60, file=sys.stderr)
        registered_modules = discover_tools(self.mcp)
        print(f"\n✓ 工具注册完成，共 {len(registered_modules)} 个模块", file=sys.stderr)
        print("="*60 + "\n", file=sys.stderr)
        
        # 提取已注册的工具
        self._extract_tools()
    
    def _extract_tools(self):
        """从 FastMCP 实例中提取已注册的工具"""
        try:
            if hasattr(self.mcp, '_tool_manager'):
                tool_manager = self.mcp._tool_manager
                if hasattr(tool_manager, '_tools'):
                    tools_dict = tool_manager._tools
                    for tool_name, tool_info in tools_dict.items():
                        # FastMCP 的工具对象属性可能是 fn 或 func
                        fn = getattr(tool_info, 'fn', getattr(tool_info, 'func', None))
                        if fn:
                            self.tools[tool_name] = fn
                        elif callable(tool_info):
                            self.tools[tool_name] = tool_info
                    print(f"✓ 已提取 {len(self.tools)} 个工具函数", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  提取工具函数时出错: {str(e)}", file=sys.stderr)
    
    def _create_async_wrapper(self, original_func, tool_name):
        """创建异步包装函数，正确捕获原始函数引用"""
        @functools.wraps(original_func)
        async def async_wrapper(*args, **kwargs):
            """异步包装器，在线程池中执行同步函数"""
            try:
                # 在线程池中执行同步函数，避免阻塞事件循环
                loop = asyncio.get_event_loop()
                future = loop.run_in_executor(
                    _executor,
                    lambda: original_func(*args, **kwargs)
                )
                # 设置 300 秒超时，防止工具调用无限阻塞线程池
                result = await asyncio.wait_for(future, timeout=300)
                return result
            except asyncio.TimeoutError:
                logger.error(f"工具 {tool_name} 执行超时（300秒）")
                raise TimeoutError(f"工具 {tool_name} 执行超时，请稍后重试")
            except Exception as e:
                logger.error(f"工具 {tool_name} 执行出错: {str(e)}", exc_info=True)
                raise
        
        return async_wrapper
    
    def _wrap_sync_tools(self):
        """将同步工具包装为异步，避免阻塞事件循环"""
        try:
            if hasattr(self.mcp, '_tool_manager'):
                tool_manager = self.mcp._tool_manager
                if hasattr(tool_manager, '_tools'):
                    tools_dict = tool_manager._tools
                    wrapped_count = 0
                    
                    for tool_name, tool_info in tools_dict.items():
                        # FastMCP 的工具对象属性可能是 fn 或 func
                        fn_attr = 'fn' if hasattr(tool_info, 'fn') else 'func' if hasattr(tool_info, 'func') else None
                        if not fn_attr:
                            continue
                            
                        original_func = getattr(tool_info, fn_attr)
                        
                        # 检查原始函数是否为同步函数
                        if not asyncio.iscoroutinefunction(original_func):
                            # 使用工厂函数创建包装器，确保正确捕获函数引用
                            async_wrapper = self._create_async_wrapper(original_func, tool_name)
                            
                            # 替换原始函数
                            setattr(tool_info, fn_attr, async_wrapper)
                            wrapped_count += 1
                            logger.debug(f"已包装同步工具: {tool_name}")
                    
                    if wrapped_count > 0:
                        print(f"✓ 已包装 {wrapped_count} 个同步工具为异步执行", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  包装工具函数时出错: {str(e)}", file=sys.stderr)
    
    
    async def health_check(self, request):
        """健康检查端点"""
        return JSONResponse({
            "status": "healthy",
            "server": "Tushare MCP Server",
            "version": "1.0.0",
            "transport": "streamable-http",
            "tools_count": len(self.tools),
            "tushare_token_configured": bool(get_tushare_token())
        })
    
    async def list_tools(self, request):
        """列出所有可用工具"""
        tools_info = []
        
        if hasattr(self.mcp, '_tool_manager') and hasattr(self.mcp._tool_manager, '_tools'):
            for tool_name, tool_info in self.mcp._tool_manager._tools.items():
                tool_data = {
                    "name": tool_name,
                }
                
                # 尝试获取工具描述
                if hasattr(tool_info, 'description'):
                    tool_data["description"] = tool_info.description
                elif hasattr(tool_info, 'func') and tool_info.func.__doc__:
                    tool_data["description"] = tool_info.func.__doc__.strip().split('\n')[0]
                
                tools_info.append(tool_data)
        
        return JSONResponse({
            "tools": tools_info,
            "count": len(tools_info)
        })


# 创建服务器实例
print("正在初始化 Tushare MCP Server (Streamable HTTP)...", file=sys.stderr)
mcp_server = TushareMCPServer()

# 获取 FastMCP 的 Streamable HTTP 应用
# streamable_http_app() 返回一个完整的 Starlette 应用，在 /mcp 端点处理 JSON-RPC 请求
app = mcp_server.mcp.streamable_http_app()

# 添加自定义路由到 FastMCP 的应用
app.add_route("/health", mcp_server.health_check, methods=["GET"])
app.add_route("/tools", mcp_server.list_tools, methods=["GET"])

# 添加 CORS 中间件（允许跨域访问）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 强制修正 Accept 头部 (解决 406 问题)
app.add_middleware(ForceAcceptHeaderMiddleware)

# 添加请求日志中间件 (用于调试)
app.add_middleware(RequestLoggingMiddleware)

# 添加连接保持活跃中间件
app.add_middleware(ConnectionKeepAliveMiddleware)

# 添加全局异常处理中间件
app.add_middleware(ExceptionHandlerMiddleware)


if __name__ == "__main__":
    print("\n" + "="*60, file=sys.stderr)
    print("🚀 启动 Tushare MCP Server (Streamable HTTP)", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"📍 MCP 端点:  http://127.0.0.1:8000/mcp", file=sys.stderr)
    print(f"📍 健康检查: http://127.0.0.1:8000/health", file=sys.stderr)
    print(f"📍 工具列表: http://127.0.0.1:8000/tools", file=sys.stderr)
    print("="*60 + "\n", file=sys.stderr)
    
    # 启动服务器
    # 配置参数：
    # - timeout_keep_alive: 保持连接的时间（秒），增加此值可以避免长时间运行的工具调用导致连接关闭
    # - timeout_graceful_shutdown: 优雅关闭的超时时间
    # - limit_concurrency: 限制并发连接数，避免资源耗尽
    # 注意：不设置 limit_max_requests 表示无限制请求数
    try:
        uvicorn.run(
            app,
            host="0.0.0.0",  # 绑定到所有接口，支持Docker和远程访问
            port=8000,
            log_level="info",
            timeout_keep_alive=600,  # 10分钟，允许长时间运行的工具调用
            timeout_graceful_shutdown=30,  # 30秒优雅关闭时间
            access_log=True,  # 启用访问日志，便于调试
            limit_concurrency=100,  # 限制并发连接数
            backlog=2048,  # 增加连接队列大小
        )
    finally:
        # 清理线程池
        _executor.shutdown(wait=True)

