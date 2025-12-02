"""基础工具类（已废弃，保留用于向后兼容）"""
# 注意：此文件已不再使用，工具现在使用装饰器方式注册
# 保留此文件仅用于向后兼容，新工具应使用register_*_tools函数模式

from typing import Optional
from config.token_manager import get_tushare_token

def check_token() -> Optional[str]:
    """检查Token，如果未配置返回错误信息，否则返回None"""
    token = get_tushare_token()
    if not token:
        return "请先配置Tushare token"
    return None

__all__ = ['check_token']
