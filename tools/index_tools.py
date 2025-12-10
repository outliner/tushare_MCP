"""国际指数相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING, Optional
from config.token_manager import get_tushare_token
from cache.index_cache_manager import index_cache_manager
from utils.common import format_date

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

# 指数代码与名称的映射
INDEX_NAME_MAP = {
    'XIN9': '富时中国A50指数',
    'HSI': '恒生指数',
    'HKTECH': '恒生科技指数',
    'HKAH': '恒生AH股H指数',
    'DJI': '道琼斯工业指数',
    'SPX': '标普500指数',
    'IXIC': '纳斯达克指数',
    'FTSE': '富时100指数',
    'FCHI': '法国CAC40指数',
    'GDAXI': '德国DAX指数',
    'N225': '日经225指数',
    'KS11': '韩国综合指数',
    'AS51': '澳大利亚标普200指数',
    'SENSEX': '印度孟买SENSEX指数',
    'IBOVESPA': '巴西IBOVESPA指数',
    'RTS': '俄罗斯RTS指数',
    'TWII': '台湾加权指数',
    'CKLSE': '马来西亚指数',
    'SPTSX': '加拿大S&P/TSX指数',
    'CSX5P': 'STOXX欧洲50指数',
    'RUT': '罗素2000指数'
}

# 反向映射：名称到代码
NAME_INDEX_MAP = {v: k for k, v in INDEX_NAME_MAP.items()}

def get_index_code(index_input: str) -> str:
    """
    根据输入获取指数代码
    
    参数:
        index_input: 指数代码或名称（如：XIN9、富时中国A50指数）
    
    返回:
        指数代码
    """
    index_input = index_input.strip()
    
    # 如果是代码，直接返回
    if index_input.upper() in INDEX_NAME_MAP:
        return index_input.upper()
    
    # 如果是名称，查找对应的代码
    if index_input in NAME_INDEX_MAP:
        return NAME_INDEX_MAP[index_input]
    
    # 模糊匹配名称
    for name, code in NAME_INDEX_MAP.items():
        if index_input in name or name in index_input:
            return code
    
    # 如果找不到，返回原输入（让API处理）
    return index_input.upper()

def format_index_data(df: pd.DataFrame, index_code: str) -> str:
    """
    格式化指数数据输出
    
    参数:
        df: 指数数据DataFrame
        index_code: 指数代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的指数数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    index_name = INDEX_NAME_MAP.get(index_code, index_code)
    
    result = []
    result.append(f"📊 {index_name} ({index_code})")
    result.append("=" * 60)
    result.append("")
    
    # 显示最近几条数据
    display_count = min(10, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'收盘':<12} {'涨跌':<12} {'涨跌幅':<10} {'振幅':<10}")
    result.append("-" * 60)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = row['trade_date']
        close = row['close']
        change = row.get('change', 0)
        pct_chg = row.get('pct_chg', 0)
        swing = row.get('swing', 0)
        
        # 格式化日期（YYYYMMDD -> YYYY-MM-DD）
        if len(trade_date) == 8:
            formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        else:
            formatted_date = trade_date
        
        # 格式化数值
        close_str = f"{close:.2f}" if pd.notna(close) else "-"
        change_str = f"{change:+.2f}" if pd.notna(change) else "-"
        pct_chg_str = f"{pct_chg:+.2f}%" if pd.notna(pct_chg) else "-"
        swing_str = f"{swing:.2f}%" if pd.notna(swing) else "-"
        
        result.append(f"{formatted_date:<12} {close_str:<12} {change_str:<12} {pct_chg_str:<10} {swing_str:<10}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📈 最新数据摘要：")
        result.append("-" * 60)
        
        if len(latest['trade_date']) == 8:
            formatted_date = f"{latest['trade_date'][:4]}-{latest['trade_date'][4:6]}-{latest['trade_date'][6:8]}"
        else:
            formatted_date = latest['trade_date']
        
        result.append(f"交易日期: {formatted_date}")
        result.append(f"收盘点位: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘点位: -")
        result.append(f"开盘点位: {latest['open']:.2f}" if pd.notna(latest.get('open')) else "开盘点位: -")
        result.append(f"最高点位: {latest['high']:.2f}" if pd.notna(latest.get('high')) else "最高点位: -")
        result.append(f"最低点位: {latest['low']:.2f}" if pd.notna(latest.get('low')) else "最低点位: -")
        
        if pd.notna(latest.get('change')):
            result.append(f"涨跌点位: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('swing')):
            result.append(f"振幅: {latest['swing']:.2f}%")
    
    return "\n".join(result)

def is_cache_data_complete(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> bool:
    """
    检查缓存中的数据是否完整覆盖了请求的日期范围
    
    参数:
        df: 从缓存获取的DataFrame
        start_date: 请求的开始日期（YYYYMMDD格式）
        end_date: 请求的结束日期（YYYYMMDD格式）
    
    返回:
        如果数据完整返回True，否则返回False
    """
    if df is None or df.empty:
        return False
    
    # 获取缓存数据中的最小和最大日期
    cache_min_date = str(df['trade_date'].min())
    cache_max_date = str(df['trade_date'].max())
    
    # 如果同时提供了start_date和end_date
    if start_date and end_date:
        # 检查缓存数据是否完全覆盖请求范围
        # 缓存的最小日期应该 <= 请求的开始日期，最大日期应该 >= 请求的结束日期
        if cache_min_date <= start_date and cache_max_date >= end_date:
            return True
        else:
            # 缓存数据没有完全覆盖请求范围，需要从API获取
            return False
    # 如果只提供了start_date
    elif start_date:
        # 检查缓存中是否有从start_date开始的数据
        if cache_min_date <= start_date:
            return True
        else:
            return False
    # 如果只提供了end_date
    elif end_date:
        # 检查缓存中是否有到end_date结束的数据
        if cache_max_date >= end_date:
            return True
        else:
            return False
    
    # 如果没有提供日期范围参数，认为数据完整（这种情况不应该调用此函数）
    return True

def register_index_tools(mcp: "FastMCP"):
    """注册国际指数相关工具"""
    
    @mcp.tool()
    def get_global_index(
        index_code: str = "",
        index_name: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取国际主要指数行情数据
        
        参数:
            index_code: 指数代码（如：XIN9、HSI、DJI、SPX、IXIC等）
            index_name: 指数名称（如：富时中国A50指数、恒生指数、道琼斯工业指数等）
            trade_date: 交易日期（YYYYMMDD格式，如：20241201，查询指定日期的数据，可选）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，可选，与trade_date二选一）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，可选，需与start_date配合使用）
        
        注意：
        - 如果提供了trade_date，将查询该特定日期的数据
        - 如果提供了start_date和end_date，将查询该日期范围内的数据
        - trade_date优先级高于start_date/end_date
        
        支持的指数包括：
        - XIN9: 富时中国A50指数
        - HSI: 恒生指数
        - HKTECH: 恒生科技指数
        - DJI: 道琼斯工业指数
        - SPX: 标普500指数
        - IXIC: 纳斯达克指数
        - FTSE: 富时100指数
        - FCHI: 法国CAC40指数
        - GDAXI: 德国DAX指数
        - N225: 日经225指数
        等20多个国际主要指数
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 确定指数代码
            if index_name:
                index_code = get_index_code(index_name)
            elif index_code:
                index_code = get_index_code(index_code)
            else:
                return "请提供指数代码或指数名称"
            
            # 参数验证：trade_date 和 start_date/end_date 的处理
            # 将空字符串转换为 None，便于后续处理
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                df = index_cache_manager.get_index_data(
                    ts_code=index_code,
                    trade_date=trade_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                df = index_cache_manager.get_index_data(
                    ts_code=index_code,
                    start_date=start_date,
                    end_date=end_date
                )
                # 检查缓存数据是否完整覆盖请求的日期范围
                if df is None or df.empty:
                    need_fetch_from_api = True
                elif not is_cache_data_complete(df, start_date, end_date):
                    # 缓存数据不完整，需要从API获取完整数据
                    need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新10条）
                df = index_cache_manager.get_index_data(
                    ts_code=index_code,
                    limit=10,
                    order_by='DESC'
                )
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {
                    'ts_code': index_code
                }
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.index_global(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = index_cache_manager.save_index_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        df = index_cache_manager.get_index_data(
                            ts_code=index_code,
                            trade_date=trade_date
                        )
                    elif start_date or end_date:
                        df = index_cache_manager.get_index_data(
                            ts_code=index_code,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:
                        # 查询最近数据
                        df = index_cache_manager.get_index_data(
                            ts_code=index_code,
                            limit=10,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                index_name = INDEX_NAME_MAP.get(index_code, index_code)
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {index_name} ({index_code}) 在 {date_info} 的指数数据，请检查参数是否正确"
            
            # 格式化输出
            return format_index_data(df, index_code)
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def search_global_indexes(keyword: str = "") -> str:
        """
        搜索可用的国际指数
        
        参数:
            keyword: 搜索关键词（可选，留空则显示所有可用指数）
        
        返回所有支持的国际指数列表，或根据关键词筛选
        """
        try:
            if keyword:
                # 根据关键词筛选
                keyword = keyword.strip().lower()
                results = []
                for code, name in INDEX_NAME_MAP.items():
                    if keyword in code.lower() or keyword in name.lower():
                        results.append(f"{code} - {name}")
                
                if not results:
                    return f"未找到包含 '{keyword}' 的指数"
                
                return "\n".join(results)
            else:
                # 返回所有指数
                results = []
                results.append("📊 支持的国际主要指数：")
                results.append("=" * 60)
                for code, name in sorted(INDEX_NAME_MAP.items()):
                    results.append(f"{code:<10} - {name}")
                return "\n".join(results)
                
        except Exception as e:
            return f"搜索失败：{str(e)}"
    
    @mcp.tool()
    def get_sw_industry_daily(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        level: str = "L1"
    ) -> str:
        """
        获取申万行业指数日线行情数据
        
        参数:
            ts_code: 指数代码（如：801210.SI社会服务，可选，如果提供则只查询该指数）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据，可选）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，可选，与trade_date二选一）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，可选，需与start_date配合使用）
            level: 行业分级（L1：一级行业，L2：二级行业，L3：三级行业），默认为L1，当提供ts_code时此参数可选
        
        注意：
            - 如果提供了ts_code，将只查询该指数的数据
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - L1：一级行业（如：采掘、化工、钢铁等）
            - L2：二级行业（如：煤炭开采、石油开采等）
            - L3：三级行业（如：动力煤、焦煤等）
            - 数据说明：交易日每天15点～16点之间入库，本接口是未复权行情
        """
        from cache.cache_manager import cache_manager
        
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and level not in ['L1', 'L2', 'L3']:
            return "当未提供ts_code时，level参数必须是 L1、L2 或 L3 之一"
        
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供指数代码(ts_code)或交易日期(trade_date)或日期范围(start_date/end_date)之一"
        
        try:
            # 参数处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'level': level,
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('sw_industry_daily', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('sw_industry_daily', **cache_params):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                params = {}
                
                # 如果提供了ts_code，使用ts_code查询
                if ts_code:
                    params['ts_code'] = ts_code
                else:
                    # 否则使用level查询
                    params['level'] = level
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.sw_daily(**params)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('sw_industry_daily', df, **cache_params)
            
            if df.empty:
                if ts_code:
                    index_info = f"指数 {ts_code}"
                else:
                    level_name = {'L1': '一级行业', 'L2': '二级行业', 'L3': '三级行业'}.get(level, level)
                    index_info = f"申万{level_name}"
                date_info = f"日期 {trade_date}" if trade_date else f"日期范围 {start_date} 至 {end_date}" if start_date else "最近数据"
                return f"未找到 {index_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 如果提供了ts_code，只显示该指数的数据
            if ts_code:
                df = df[df['ts_code'] == ts_code]
                if df.empty:
                    return f"未找到指数 {ts_code} 的数据"
                return format_single_sw_industry_daily(df, ts_code)
            
            # 格式化输出
            if ts_code:
                return format_single_sw_industry_daily(df, ts_code)
            else:
                return format_sw_industry_daily_data(df, level)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_industry_index_codes(level: str = "L1", src: str = "SW2021") -> str:
        """
        获取申万行业分类指数代码
        
        参数:
            level: 行业分级（L1：一级行业，L2：二级行业，L3：三级行业），默认为L1
            src: 指数来源（SW2014：申万2014年版本，SW2021：申万2021年版本），默认为SW2021
        
        返回申万行业分类的指数代码列表，包括指数代码、行业名称、行业代码等信息
        
        注意：
        - L1：一级行业（如：采掘、化工、钢铁等）
        - L2：二级行业（如：煤炭开采、石油开采等）
        - L3：三级行业（如：动力煤、焦煤等）
        - SW2021是较新的版本，推荐使用
        """
        from cache.cache_manager import cache_manager
        
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if level not in ['L1', 'L2', 'L3']:
            return "level参数必须是 L1、L2 或 L3 之一"
        
        if src not in ['SW2014', 'SW2021']:
            return "src参数必须是 SW2014 或 SW2021 之一"
        
        try:
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {'level': level, 'src': src}
            df = cache_manager.get_dataframe('index_classify', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('index_classify', **cache_params):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                df = pro.index_classify(level=level, src=src)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('index_classify', df, **cache_params)
            
            if df.empty:
                return f"未找到 {level} 级别的行业分类数据"
            
            # 格式化输出
            return format_industry_index_codes(df, level, src)
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_sw_industry_members(
        l1_code: str = "",
        l2_code: str = "",
        l3_code: str = "",
        ts_code: str = ""
    ) -> str:
        """
        获取申万行业成分构成(分级)
        
        参数:
            l1_code: 一级行业代码（如：801050.SI 有色金属，可选）
            l2_code: 二级行业代码（如：801053.SI 贵金属，可选）
            l3_code: 三级行业代码（如：850531.SI 黄金，可选）
            ts_code: 股票代码（如：000001.SZ，可选，用于查询该股票所属的行业分类）
        
        返回:
            申万行业成分列表，包含一级/二级/三级行业代码和名称、股票代码和名称、纳入日期等信息
        
        说明:
            - 可以按行业代码查询该行业下的所有成分股
            - 可以按股票代码查询该股票所属的行业分类
            - 支持一级、二级、三级行业代码查询
            - 单次最大返回2000行数据
            - 需要2000积分权限
        
        示例:
            - 获取黄金行业成分股：l3_code="850531.SI"
            - 获取某股票所属行业：ts_code="000001.SZ"
            - 获取贵金属二级行业成分股：l2_code="801053.SI"
        """
        from cache.cache_manager import cache_manager
        
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not l1_code and not l2_code and not l3_code and not ts_code:
            return "请至少提供一个查询参数：l1_code（一级行业代码）、l2_code（二级行业代码）、l3_code（三级行业代码）或 ts_code（股票代码）"
        
        try:
            # 参数处理
            l1_code = l1_code.strip() if l1_code else None
            l2_code = l2_code.strip() if l2_code else None
            l3_code = l3_code.strip() if l3_code else None
            ts_code = ts_code.strip() if ts_code else None
            
            # 构建缓存参数
            cache_params = {
                'l1_code': l1_code or '',
                'l2_code': l2_code or '',
                'l3_code': l3_code or '',
                'ts_code': ts_code or ''
            }
            
            # 尝试从缓存获取
            df = cache_manager.get_dataframe('index_member_all', **cache_params)
            
            # 检查是否需要更新
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('index_member_all', **cache_params):
                need_update = True
            
            if need_update:
                pro = ts.pro_api()
                params = {}
                
                if l1_code:
                    params['l1_code'] = l1_code
                if l2_code:
                    params['l2_code'] = l2_code
                if l3_code:
                    params['l3_code'] = l3_code
                if ts_code:
                    params['ts_code'] = ts_code
                
                df = pro.index_member_all(**params)
                
                # 保存到缓存
                if df is not None and not df.empty:
                    cache_manager.set('index_member_all', df, **cache_params)
            
            if df is None or df.empty:
                query_info = []
                if l1_code:
                    query_info.append(f"一级行业 {l1_code}")
                if l2_code:
                    query_info.append(f"二级行业 {l2_code}")
                if l3_code:
                    query_info.append(f"三级行业 {l3_code}")
                if ts_code:
                    query_info.append(f"股票 {ts_code}")
                return f"未找到 {' / '.join(query_info)} 的申万行业成分数据"
            
            # 格式化输出
            return format_sw_industry_members(df, l1_code, l2_code, l3_code, ts_code)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"


def format_sw_industry_members(
    df: pd.DataFrame,
    l1_code: Optional[str] = None,
    l2_code: Optional[str] = None,
    l3_code: Optional[str] = None,
    ts_code: Optional[str] = None
) -> str:
    """
    格式化申万行业成分数据输出
    
    参数:
        df: 申万行业成分数据DataFrame
        l1_code: 一级行业代码
        l2_code: 二级行业代码
        l3_code: 三级行业代码
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的申万行业成分数据"
    
    result = []
    
    # 根据查询类型显示不同的标题
    if ts_code:
        result.append(f"📊 股票 {ts_code} 所属申万行业分类")
    elif l3_code:
        # 获取三级行业名称
        l3_name = df['l3_name'].iloc[0] if 'l3_name' in df.columns and not df.empty else l3_code
        result.append(f"📊 申万三级行业【{l3_name}】({l3_code}) 成分股")
    elif l2_code:
        l2_name = df['l2_name'].iloc[0] if 'l2_name' in df.columns and not df.empty else l2_code
        result.append(f"📊 申万二级行业【{l2_name}】({l2_code}) 成分股")
    elif l1_code:
        l1_name = df['l1_name'].iloc[0] if 'l1_name' in df.columns and not df.empty else l1_code
        result.append(f"📊 申万一级行业【{l1_name}】({l1_code}) 成分股")
    else:
        result.append("📊 申万行业成分")
    
    result.append("=" * 100)
    result.append("")
    
    # 如果是查询股票所属行业，显示行业分类信息
    if ts_code:
        result.append(f"{'股票代码':<12} {'股票名称':<12} {'一级行业':<15} {'二级行业':<15} {'三级行业':<15} {'纳入日期':<12}")
        result.append("-" * 100)
        
        for _, row in df.iterrows():
            stock_code = str(row.get('ts_code', '-'))
            stock_name = str(row.get('name', '-'))
            l1_name = str(row.get('l1_name', '-'))
            l2_name = str(row.get('l2_name', '-'))
            l3_name = str(row.get('l3_name', '-'))
            in_date_raw = str(row.get('in_date', '-'))
            in_date = format_date(in_date_raw) if in_date_raw != '-' else '-'
            
            result.append(f"{stock_code:<12} {stock_name:<12} {l1_name:<15} {l2_name:<15} {l3_name:<15} {in_date:<12}")
    else:
        # 显示成分股列表
        result.append(f"{'股票代码':<12} {'股票名称':<15} {'一级行业':<15} {'二级行业':<15} {'三级行业':<15} {'纳入日期':<12}")
        result.append("-" * 100)
        
        # 按股票代码排序
        df_sorted = df.sort_values('ts_code') if 'ts_code' in df.columns else df
        
        for _, row in df_sorted.iterrows():
            stock_code = str(row.get('ts_code', '-'))
            stock_name = str(row.get('name', '-'))
            l1_name = str(row.get('l1_name', '-'))
            l2_name = str(row.get('l2_name', '-'))
            l3_name = str(row.get('l3_name', '-'))
            in_date_raw = str(row.get('in_date', '-'))
            in_date = format_date(in_date_raw) if in_date_raw != '-' else '-'
            
            result.append(f"{stock_code:<12} {stock_name:<15} {l1_name:<15} {l2_name:<15} {l3_name:<15} {in_date:<12}")
    
    result.append("")
    result.append(f"共 {len(df)} 条记录")
    
    # 显示行业层级信息
    if not ts_code and not df.empty:
        result.append("")
        result.append("📋 行业层级信息：")
        result.append("-" * 100)
        
        # 获取唯一的行业层级
        if 'l1_code' in df.columns and 'l1_name' in df.columns:
            l1_info = df[['l1_code', 'l1_name']].drop_duplicates()
            for _, row in l1_info.iterrows():
                result.append(f"一级行业: {row['l1_name']} ({row['l1_code']})")
        
        if 'l2_code' in df.columns and 'l2_name' in df.columns:
            l2_info = df[['l2_code', 'l2_name']].drop_duplicates()
            for _, row in l2_info.iterrows():
                result.append(f"二级行业: {row['l2_name']} ({row['l2_code']})")
        
        if 'l3_code' in df.columns and 'l3_name' in df.columns:
            l3_info = df[['l3_code', 'l3_name']].drop_duplicates()
            for _, row in l3_info.iterrows():
                result.append(f"三级行业: {row['l3_name']} ({row['l3_code']})")
    
    return "\n".join(result)


def format_sw_industry_daily_data(df: pd.DataFrame, level: str) -> str:
    """
    格式化申万行业指数日线行情数据输出
    
    参数:
        df: 申万行业指数日线行情数据DataFrame
        level: 行业分级
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的申万行业指数日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 根据级别显示不同的标题
    level_names = {
        'L1': '一级行业',
        'L2': '二级行业',
        'L3': '三级行业'
    }
    level_name = level_names.get(level, level)
    
    result.append(f"📊 申万{level_name}指数日线行情")
    result.append("=" * 100)
    result.append("")
    
    # 按日期分组显示
    dates = df['trade_date'].unique()
    for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
        date_df = df[df['trade_date'] == date]
        if not date_df.empty:
            result.append(f"📅 交易日期: {format_date(date)}")
            result.append("=" * 100)
            result.append(f"{'指数代码':<15} {'行业名称':<20} {'收盘点位':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
            result.append("-" * 100)
            
            # 按涨跌幅排序（降序），NaN值放在最后
            # 使用fillna处理NaN值，确保排序正常
            date_df_sorted = date_df.copy()
            # 检查是否存在pct_chg列
            if 'pct_chg' in date_df_sorted.columns:
                # 将NaN值填充为一个很小的值，这样排序时NaN会排在最后
                date_df_sorted['_sort_pct_chg'] = date_df_sorted['pct_chg'].fillna(-999999)
                date_df_sorted = date_df_sorted.sort_values('_sort_pct_chg', ascending=False)
                date_df_sorted = date_df_sorted.drop('_sort_pct_chg', axis=1)
            elif 'change' in date_df_sorted.columns:
                # 如果没有pct_chg，使用change列排序
                date_df_sorted['_sort_change'] = date_df_sorted['change'].fillna(-999999)
                date_df_sorted = date_df_sorted.sort_values('_sort_change', ascending=False)
                date_df_sorted = date_df_sorted.drop('_sort_change', axis=1)
            else:
                # 如果都没有，按指数代码排序
                date_df_sorted = date_df_sorted.sort_values('ts_code' if 'ts_code' in date_df_sorted.columns else 'index_code')
            
            for _, row in date_df_sorted.iterrows():
                index_code = str(row.get('index_code', row.get('ts_code', '-')))
                industry_name = str(row.get('industry_name', '-'))
                close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
                change = f"{row.get('change', 0):+.2f}" if pd.notna(row.get('change')) else "-"
                pct_chg_val = row.get('pct_chg')
                pct_chg = f"{pct_chg_val:+.2f}%" if pd.notna(pct_chg_val) else "-"
                vol = f"{row.get('vol', 0):.0f}" if pd.notna(row.get('vol')) else "-"
                amount = f"{row.get('amount', 0):.0f}" if pd.notna(row.get('amount')) else "-"
                
                result.append(f"{index_code:<15} {industry_name:<20} {close:<12} {change:<12} {pct_chg:<10} {vol:<15} {amount:<15}")
            result.append("")
    
    # 如果有更多日期，显示提示
    if len(dates) > 10:
        result.append(f"（共 {len(dates)} 个交易日，仅显示最近 10 个交易日）")
    
    return "\n".join(result)

def format_single_sw_industry_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个申万行业指数的日线行情数据输出
    
    参数:
        df: 单个申万行业指数的日线行情数据DataFrame
        ts_code: 指数代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 申万行业指数日线行情")
    result.append("=" * 100)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(str(row.get('trade_date', '-')))
        open_price = f"{row.get('open', 0):.2f}" if pd.notna(row.get('open')) else "-"
        high = f"{row.get('high', 0):.2f}" if pd.notna(row.get('high')) else "-"
        low = f"{row.get('low', 0):.2f}" if pd.notna(row.get('low')) else "-"
        close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
        change = f"{row.get('change', 0):+.2f}" if pd.notna(row.get('change')) else "-"
        pct_chg_val = row.get('pct_chg')
        pct_chg = f"{pct_chg_val:+.2f}%" if pd.notna(pct_chg_val) else "-"
        vol = f"{row.get('vol', 0):.0f}" if pd.notna(row.get('vol')) else "-"
        amount = f"{row.get('amount', 0):.0f}" if pd.notna(row.get('amount')) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<12} {high:<12} {low:<12} {close:<12} {change:<12} {pct_chg:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 100)
        trade_date_str = str(latest.get('trade_date', '-'))
        result.append(f"交易日期: {format_date(trade_date_str)}")
        result.append(f"开盘点位: {latest['open']:.2f}" if pd.notna(latest.get('open')) else "开盘点位: -")
        result.append(f"最高点位: {latest['high']:.2f}" if pd.notna(latest.get('high')) else "最高点位: -")
        result.append(f"最低点位: {latest['low']:.2f}" if pd.notna(latest.get('low')) else "最低点位: -")
        result.append(f"收盘点位: {latest['close']:.2f}" if pd.notna(latest.get('close')) else "收盘点位: -")
        result.append(f"昨收点位: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收点位: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌点位: {latest.get('change', 0):+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest.get('pct_chg', 0):+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest.get('vol', 0):.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest.get('amount', 0):.0f} 千元")
    
    return "\n".join(result)

def format_industry_index_codes(df: pd.DataFrame, level: str, src: str) -> str:
    """
    格式化行业分类指数代码输出
    
    参数:
        df: 行业分类数据DataFrame
        level: 行业分级
        src: 指数来源
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的行业分类数据"
    
    result = []
    
    # 根据级别显示不同的标题
    level_names = {
        'L1': '一级行业',
        'L2': '二级行业',
        'L3': '三级行业'
    }
    level_name = level_names.get(level, level)
    
    result.append(f"📊 申万{src} {level_name}分类指数代码")
    result.append("=" * 80)
    result.append("")
    
    # 按行业代码排序
    df_sorted = df.sort_values('index_code')
    
    # 显示表头
    result.append(f"{'指数代码':<15} {'行业名称':<20} {'行业代码':<15} {'父级代码':<15} {'是否发布指数':<12}")
    result.append("-" * 80)
    
    for _, row in df_sorted.iterrows():
        index_code = str(row.get('index_code', '-'))
        industry_name = str(row.get('industry_name', '-'))
        industry_code = str(row.get('industry_code', '-'))
        parent_code = str(row.get('parent_code', '-'))
        is_pub = str(row.get('is_pub', '-'))
        
        result.append(f"{index_code:<15} {industry_name:<20} {industry_code:<15} {parent_code:<15} {is_pub:<12}")
    
    result.append("")
    result.append(f"共 {len(df)} 个{level_name}")
    
    return "\n".join(result)

