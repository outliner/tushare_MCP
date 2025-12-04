"""通用工具函数模块

本模块包含项目中多处使用的通用函数，避免代码重复。
"""
from typing import Optional, Union
import pandas as pd


def format_date(date_str: str) -> str:
    """
    格式化日期字符串（YYYYMMDD -> YYYY-MM-DD）
    
    参数:
        date_str: 日期字符串（YYYYMMDD格式）
    
    返回:
        格式化后的日期字符串（YYYY-MM-DD格式）
    
    示例:
        >>> format_date("20241201")
        "2024-12-01"
        >>> format_date("2024-12-01")
        "2024-12-01"
    """
    if not date_str:
        return "-"
    date_str = str(date_str).strip()
    if len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def format_number(value: Union[float, int, None], 
                  decimals: int = 2, 
                  suffix: str = "",
                  show_sign: bool = False) -> str:
    """
    格式化数字，支持千分位分隔符
    
    参数:
        value: 要格式化的数字
        decimals: 小数位数（默认2）
        suffix: 后缀（如 '%', '万', '亿'）
        show_sign: 是否显示正负号
    
    返回:
        格式化后的字符串
    
    示例:
        >>> format_number(1234567.89)
        "1,234,567.89"
        >>> format_number(12.5, suffix="%")
        "12.50%"
        >>> format_number(100, show_sign=True)
        "+100.00"
    """
    if value is None or pd.isna(value):
        return "-"
    
    try:
        if show_sign and value > 0:
            return f"+{value:,.{decimals}f}{suffix}"
        return f"{value:,.{decimals}f}{suffix}"
    except (ValueError, TypeError):
        return str(value)


def format_amount(value: Union[float, int, None], unit: str = "万") -> str:
    """
    格式化金额，自动转换单位
    
    参数:
        value: 金额（元或千元）
        unit: 目标单位（'万' 或 '亿'）
    
    返回:
        格式化后的金额字符串
    """
    if value is None or pd.isna(value):
        return "-"
    
    try:
        if unit == "亿":
            return f"{value / 100000000:,.2f}亿"
        elif unit == "万":
            return f"{value / 10000:,.2f}万"
        return f"{value:,.2f}"
    except (ValueError, TypeError):
        return str(value)


def format_percent(value: Union[float, int, None], decimals: int = 2) -> str:
    """
    格式化百分比
    
    参数:
        value: 百分比数值（已经是百分比形式，如 5.5 表示 5.5%）
        decimals: 小数位数
    
    返回:
        格式化后的百分比字符串
    """
    if value is None or pd.isna(value):
        return "-"
    
    try:
        if value > 0:
            return f"+{value:.{decimals}f}%"
        return f"{value:.{decimals}f}%"
    except (ValueError, TypeError):
        return str(value)


def safe_get(row: pd.Series, column: str, default: str = "-") -> str:
    """
    安全获取DataFrame行中的值
    
    参数:
        row: DataFrame的一行
        column: 列名
        default: 默认值
    
    返回:
        列值或默认值
    """
    if column not in row.index:
        return default
    value = row[column]
    if pd.isna(value):
        return default
    return str(value)


def build_query_info(params: dict) -> str:
    """
    构建查询条件信息字符串
    
    参数:
        params: 查询参数字典
    
    返回:
        格式化的查询条件字符串
    """
    info_parts = []
    for key, value in params.items():
        if value:
            info_parts.append(f"{key}: {value}")
    return ", ".join(info_parts) if info_parts else "无筛选条件"


__all__ = [
    'format_date',
    'format_number',
    'format_amount', 
    'format_percent',
    'safe_get',
    'build_query_info'
]

