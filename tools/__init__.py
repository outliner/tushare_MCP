"""工具模块自动发现和注册"""
import sys
import importlib
import pkgutil
from pathlib import Path
from typing import List
from mcp.server.fastmcp import FastMCP

def discover_tools(mcp: FastMCP) -> List[str]:
    """
    自动发现并注册所有工具
    
    扫描tools目录下所有模块，自动发现register_*_tools函数并调用
    
    工作原理：
    1. 遍历tools目录下的所有Python模块
    2. 动态导入每个模块
    3. 查找所有以register_开头、以_tools结尾的函数
    4. 调用这些函数，传入mcp实例
    5. 工具通过装饰器自动注册到MCP服务器
    
    使用示例：
        from tools import discover_tools
        discover_tools(mcp)  # 自动发现并注册所有工具
    """
    registered_modules = []
    tools_dir = Path(__file__).parent
    
    print("开始自动发现MCP工具...", file=sys.stderr)
    
    # 遍历tools目录下的所有Python文件
    for module_info in pkgutil.iter_modules([str(tools_dir)]):
        module_name = module_info.name
        
        # 跳过特殊文件
        if module_name in ['__init__', 'base']:
            continue
        
        try:
            # 动态导入模块
            module = importlib.import_module(f'tools.{module_name}')
            
            # 查找所有register_*_tools函数
            for attr_name in dir(module):
                if attr_name.startswith('register_') and attr_name.endswith('_tools'):
                    attr = getattr(module, attr_name)
                    
                    # 检查是否是可调用函数
                    if callable(attr):
                        try:
                            # 调用注册函数，传入mcp实例
                            attr(mcp)
                            registered_modules.append(module_name)
                            print(f"✓ 已自动注册工具: {attr_name} (来自 {module_name})", file=sys.stderr)
                        except Exception as e:
                            print(f"✗ 注册工具 {attr_name} 时出错: {str(e)}", file=sys.stderr)
        
        except ImportError as e:
            print(f"✗ 导入模块 {module_name} 失败: {str(e)}", file=sys.stderr)
        except Exception as e:
            print(f"✗ 加载工具模块 {module_name} 时出错: {str(e)}", file=sys.stderr)
    
    print(f"工具发现完成，共注册 {len(registered_modules)} 个工具模块", file=sys.stderr)
    return registered_modules

__all__ = ['discover_tools']
