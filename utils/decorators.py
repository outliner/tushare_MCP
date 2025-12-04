"""装饰器模块

本模块包含项目中使用的通用装饰器。
"""
import functools
import traceback
from typing import Callable, Any
from config.token_manager import get_tushare_token


def require_token(func: Callable) -> Callable:
    """
    Token检查装饰器
    
    用于在调用API之前检查是否已配置Tushare token。
    
    使用示例:
        @mcp.tool()
        @require_token
        def get_stock_data(...):
            ...
    
    注意：此装饰器应放在 @mcp.tool() 装饰器之后
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        return func(*args, **kwargs)
    return wrapper


def handle_exceptions(func: Callable) -> Callable:
    """
    异常处理装饰器
    
    统一处理函数执行过程中的异常，返回格式化的错误信息。
    
    使用示例:
        @mcp.tool()
        @handle_exceptions
        def get_stock_data(...):
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    return wrapper


def api_tool(func: Callable) -> Callable:
    """
    API工具组合装饰器
    
    组合了 token 检查和异常处理功能。
    
    使用示例:
        @mcp.tool()
        @api_tool
        def get_stock_data(...):
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # 检查 token
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 执行函数并处理异常
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    return wrapper


__all__ = [
    'require_token',
    'handle_exceptions',
    'api_tool'
]

