"""股票相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING, Optional
from config.token_manager import get_tushare_token

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from cache.cache_manager import cache_manager
from cache.stock_daily_cache_manager import stock_daily_cache_manager
from cache.stock_weekly_cache_manager import stock_weekly_cache_manager
from cache.index_daily_cache_manager import index_daily_cache_manager

def register_stock_tools(mcp: "FastMCP"):
    """注册股票相关工具"""
    
    @mcp.tool()
    def get_stock_basic_info(ts_code: str = "", name: str = "") -> str:
        """
        获取股票基本信息
        
        参数:
            ts_code: 股票代码（如：000001.SZ）
            name: 股票名称（如：平安银行）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {'ts_code': ts_code, 'name': name}
            df = cache_manager.get_dataframe('stock_basic', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('stock_basic', **cache_params):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                filters = {}
                if ts_code:
                    filters['ts_code'] = ts_code
                if name:
                    filters['name'] = name
                    
                df = pro.stock_basic(**filters)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('stock_basic', df, **cache_params)
            
            if df.empty:
                return "未找到符合条件的股票"
                
            # 格式化输出
            result = []
            for _, row in df.iterrows():
                # 获取所有可用的列
                available_fields = row.index.tolist()
                
                # 构建基本信息
                info_parts = []
                
                # 必要字段
                if 'ts_code' in available_fields:
                    info_parts.append(f"股票代码: {row['ts_code']}")
                if 'name' in available_fields:
                    info_parts.append(f"股票名称: {row['name']}")
                    
                # 可选字段
                optional_fields = {
                    'area': '所属地区',
                    'industry': '所属行业',
                    'list_date': '上市日期',
                    'market': '市场类型',
                    'exchange': '交易所',
                    'curr_type': '币种',
                    'list_status': '上市状态',
                    'delist_date': '退市日期'
                }
                
                for field, label in optional_fields.items():
                    if field in available_fields and not pd.isna(row[field]):
                        info_parts.append(f"{label}: {row[field]}")
                
                info = "\n".join(info_parts)
                info += "\n------------------------"
                result.append(info)
                
            return "\n".join(result)
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def search_stocks(keyword: str) -> str:
        """
        搜索股票
        
        参数:
            keyword: 关键词（可以是股票代码的一部分或股票名称的一部分）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 尝试从缓存获取完整的股票列表（即使过期也返回）
            df = cache_manager.get_dataframe('stock_search', keyword='all')
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('stock_search', keyword='all'):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                df = pro.stock_basic()
                # 保存完整列表到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('stock_search', df, keyword='all')
            
            # 在代码和名称中搜索关键词
            mask = (df['ts_code'].str.contains(keyword, case=False)) | \
                   (df['name'].str.contains(keyword, case=False))
            results = df[mask]
            
            if results.empty:
                return "未找到符合条件的股票"
                
            # 格式化输出
            output = []
            for _, row in results.iterrows():
                output.append(f"{row['ts_code']} - {row['name']}")
                
            return "\n".join(output)
            
        except Exception as e:
            return f"搜索失败：{str(e)}"
    
    @mcp.tool()
    def get_stock_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取A股日线行情数据
        
        参数:
            ts_code: 股票代码（如：000001.SZ，支持多个股票同时提取，逗号分隔，如：000001.SZ,600000.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与start_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：交易日每天15点～16点之间入库，本接口是未复权行情，停牌期间不提供数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供股票代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
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
                if ts_code:
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有股票在特定日期的数据
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not stock_daily_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有股票在日期范围内的数据
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        ts_code=ts_code,
                        limit=20,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供股票代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.daily(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = stock_daily_cache_manager.save_stock_daily_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        if ts_code:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                ts_code=ts_code,
                                limit=20,
                                order_by='DESC'
                            )
            
            if df is None or df.empty:
                if ts_code:
                    stock_info = f"股票 {ts_code}"
                else:
                    stock_info = "股票"
                
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
                return f"未找到 {stock_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_stock_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_stock_weekly(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取A股周线行情数据
        
        参数:
            ts_code: 股票代码（如：000001.SZ，支持多个股票同时提取，逗号分隔，如：000001.SZ,600000.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定周的数据，trade_date为该周的最后交易日）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与start_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定周的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的周线数据
            - trade_date优先级高于start_date/end_date
            - trade_date为该周的最后交易日（通常是周五）
            - 数据说明：周线数据每周更新一次，本接口是未复权行情
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code:
            return "请提供股票代码(ts_code)"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
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
                # 查询特定周
                df = stock_weekly_cache_manager.get_stock_weekly_data(
                    ts_code=ts_code,
                    trade_date=trade_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                df = stock_weekly_cache_manager.get_stock_weekly_data(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                # 检查缓存数据是否完整覆盖请求的日期范围
                if df is None or df.empty:
                    need_fetch_from_api = True
                elif not stock_weekly_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                    # 缓存数据不完整，需要从API获取完整数据
                    need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                df = stock_weekly_cache_manager.get_stock_weekly_data(
                    ts_code=ts_code,
                    limit=20,
                    order_by='DESC'
                )
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.weekly(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = stock_weekly_cache_manager.save_stock_weekly_data(df)
                    # 如果查询的是特定周或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        df = stock_weekly_cache_manager.get_stock_weekly_data(
                            ts_code=ts_code,
                            trade_date=trade_date
                        )
                    elif start_date or end_date:
                        df = stock_weekly_cache_manager.get_stock_weekly_data(
                            ts_code=ts_code,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:
                        # 查询最近数据
                        df = stock_weekly_cache_manager.get_stock_weekly_data(
                            ts_code=ts_code,
                            limit=20,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                stock_info = f"股票 {ts_code}"
                
                if trade_date:
                    date_info = f"周 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {stock_info} 在 {date_info} 的周线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_stock_weekly_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_etf_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取ETF日线行情数据
        
        参数:
            ts_code: ETF基金代码（如：510330.SH沪深300ETF华夏，支持多个ETF同时提取，逗号分隔，如：510330.SH,510300.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与end_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取ETF行情每日收盘后成交数据，历史超过10年
            - 限量：单次最大2000行记录，可以根据ETF代码和日期循环获取历史
        
        常用ETF代码示例：
            - 510330.SH: 沪深300ETF华夏
            - 510300.SH: 沪深300ETF
            - 159919.SZ: 沪深300ETF
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供ETF代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
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
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有ETF在特定日期的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not index_daily_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有ETF在日期范围内的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        limit=20,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供ETF代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                # 使用fund_daily接口获取ETF日线行情数据
                df = pro.fund_daily(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = index_daily_cache_manager.save_index_daily_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                limit=20,
                                order_by='DESC'
                            )
            
            if df is None or df.empty:
                if ts_code:
                    etf_info = f"ETF {ts_code}"
                else:
                    etf_info = "ETF"
                
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
                return f"未找到 {etf_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_etf_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_index_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取A股指数日线行情数据
        
        参数:
            ts_code: 指数代码（如：000300.SH沪深300、000001.SH上证指数、399001.SZ深证成指等，支持多个指数同时提取，逗号分隔，如：000300.SH,000001.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与end_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：交易日每天15点～16点之间入库，本接口是未复权行情
        
        常用指数代码：
            - 000300.SH: 沪深300指数
            - 000001.SH: 上证指数
            - 399001.SZ: 深证成指
            - 399006.SZ: 创业板指
            - 000016.SH: 上证50指数
            - 399005.SZ: 中小板指
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供指数代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
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
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有指数在特定日期的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not index_daily_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有指数在日期范围内的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        limit=20,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供指数代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                # 使用index_daily接口获取A股指数日线行情数据
                df = pro.index_daily(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = index_daily_cache_manager.save_index_daily_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                limit=20,
                                order_by='DESC'
                            )
            
            if df is None or df.empty:
                if ts_code:
                    index_info = f"指数 {ts_code}"
                else:
                    index_info = "指数"
                
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
                return f"未找到 {index_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_index_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_stock_holder_trade(
        ts_code: str = "",
        ann_date: str = "",
        start_date: str = "",
        end_date: str = "",
        trade_type: str = "",
        holder_type: str = ""
    ) -> str:
        """
        获取上市公司股东增减持数据
        
        参数:
            ts_code: 股票代码（如：300766.SZ，留空则查询所有股票）
            ann_date: 公告日期（YYYYMMDD格式，如：20240426，查询指定日期的增减持数据）
            start_date: 公告开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 公告结束日期（YYYYMMDD格式，需与start_date配合使用）
            trade_type: 交易类型（IN增持，DE减持，留空则查询所有类型）
            holder_type: 股东类型（C公司，P个人，G高管，留空则查询所有类型）
        
        返回:
            包含股东增减持数据的格式化字符串
        
        说明:
            - 数据来源于上市公司公告
            - 支持按股票代码、公告日期、交易类型、股东类型筛选
            - 显示增减持数量、占流通比例、平均价格等信息
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not ann_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、公告日期(ann_date)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if ann_date:
                params['ann_date'] = ann_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            if trade_type:
                params['trade_type'] = trade_type
            if holder_type:
                params['holder_type'] = holder_type
            
            # 获取增减持数据
            df = pro.stk_holdertrade(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if trade_type:
                    param_info.append(f"交易类型: {trade_type}")
                if holder_type:
                    param_info.append(f"股东类型: {holder_type}")
                
                return f"未找到符合条件的增减持数据\n查询条件: {', '.join(param_info)}"
            
            # 按公告日期排序（最新的在前）
            if 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            
            # 格式化输出
            result = []
            result.append("📊 上市公司股东增减持数据")
            result.append("=" * 120)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"股票代码: {ts_code}")
            if ann_date:
                query_info.append(f"公告日期: {ann_date}")
            if start_date or end_date:
                date_range = f"{start_date or '开始'} 至 {end_date or '结束'}"
                query_info.append(f"日期范围: {date_range}")
            if trade_type:
                trade_type_name = "增持" if trade_type == "IN" else "减持" if trade_type == "DE" else trade_type
                query_info.append(f"交易类型: {trade_type_name}")
            if holder_type:
                holder_type_name = {"C": "公司", "P": "个人", "G": "高管"}.get(holder_type, holder_type)
                query_info.append(f"股东类型: {holder_type_name}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 显示数据统计
            result.append(f"📈 共找到 {len(df)} 条增减持记录")
            result.append("")
            
            # 按股票代码分组显示（如果查询了多个股票）
            if not ts_code:
                # 如果未指定股票代码，按股票代码分组
                codes = df['ts_code'].unique()
                for code in codes[:10]:  # 最多显示10个股票
                    code_df = df[df['ts_code'] == code].copy()
                    result.append(format_holder_trade_data(code_df, code))
                    result.append("")
                
                if len(codes) > 10:
                    result.append(f"（共 {len(codes)} 个股票，仅显示前 10 个）")
            else:
                # 单个股票，直接显示
                result.append(format_holder_trade_data(df, ts_code))
            
            result.append("")
            result.append("📝 说明：")
            result.append("  - 数据来源于上市公司公告")
            result.append("  - IN: 增持，DE: 减持")
            result.append("  - 股东类型：C公司，P个人，G高管")
            result.append("  - change_ratio: 占流通比例（%）")
            result.append("  - avg_price: 平均交易价格")
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_stock_holder_number(
        ts_code: str = "",
        ann_date: str = "",
        enddate: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取上市公司股东户数数据
        
        参数:
            ts_code: 股票代码（如：300766.SZ，留空则查询所有股票）
            ann_date: 公告日期（YYYYMMDD格式，如：20240426，查询指定公告日期的数据）
            enddate: 截止日期（YYYYMMDD格式，如：20240930，查询指定截止日期的数据）
            start_date: 公告开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 公告结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含股东户数数据的格式化字符串
        
        说明:
            - 数据来源于上市公司定期报告，不定期公布
            - 支持按股票代码、公告日期、截止日期、日期范围筛选
            - 股东户数变化可以反映股票的集中度变化趋势
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not ann_date and not enddate and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、公告日期(ann_date)、截止日期(enddate)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if ann_date:
                params['ann_date'] = ann_date
            if enddate:
                params['enddate'] = enddate
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 获取股东户数数据
            df = pro.stk_holdernumber(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if enddate:
                    param_info.append(f"截止日期: {enddate}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的股东户数数据\n查询条件: {', '.join(param_info)}"
            
            # 按公告日期排序（最新的在前）
            if 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            elif 'end_date' in df.columns:
                df = df.sort_values('end_date', ascending=False)
            
            # 格式化输出
            result = []
            result.append("📊 上市公司股东户数数据")
            result.append("=" * 100)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"股票代码: {ts_code}")
            if ann_date:
                query_info.append(f"公告日期: {ann_date}")
            if enddate:
                query_info.append(f"截止日期: {enddate}")
            if start_date or end_date:
                date_range = f"{start_date or '开始'} 至 {end_date or '结束'}"
                query_info.append(f"日期范围: {date_range}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 显示数据统计
            result.append(f"📈 共找到 {len(df)} 条股东户数记录")
            result.append("")
            
            # 按股票代码分组显示（如果查询了多个股票）
            if not ts_code:
                # 如果未指定股票代码，按股票代码分组
                codes = df['ts_code'].unique()
                for code in codes[:10]:  # 最多显示10个股票
                    code_df = df[df['ts_code'] == code].copy()
                    result.append(format_holder_number_data(code_df, code))
                    result.append("")
                
                if len(codes) > 10:
                    result.append(f"（共 {len(codes)} 个股票，仅显示前 10 个）")
            else:
                # 单个股票，直接显示
                result.append(format_holder_number_data(df, ts_code))
            
            result.append("")
            result.append("📝 说明：")
            result.append("  - 数据来源于上市公司定期报告，不定期公布")
            result.append("  - 股东户数增加通常表示持股分散，减少表示持股集中")
            result.append("  - 建议结合股价走势分析股东户数变化趋势")
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"

def format_holder_number_data(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化股东户数数据
    
    参数:
        df: 股东户数数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的股东户数数据"
    
    result = []
    result.append(f"📊 {ts_code} 股东户数变化")
    result.append("-" * 100)
    result.append("")
    
    # 表头
    result.append(f"{'公告日期':<12} {'截止日期':<12} {'股东户数':<12} {'变化':<12} {'变化率':<12}")
    result.append("-" * 100)
    
    # 按公告日期排序（最新的在前）
    if 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    elif 'end_date' in df.columns:
        df = df.sort_values('end_date', ascending=False)
    
    # 计算变化
    df = df.copy()
    if 'holder_num' in df.columns:
        df['holder_num'] = pd.to_numeric(df['holder_num'], errors='coerce')
        # 计算变化（与上一条记录比较）
        df['change'] = df['holder_num'].diff().fillna(0)
        df['change_pct'] = (df['change'] / df['holder_num'].shift(1) * 100).fillna(0)
    
    for _, row in df.iterrows():
        # 公告日期
        ann_date = format_date(str(row['ann_date'])) if 'ann_date' in row and pd.notna(row['ann_date']) else "-"
        
        # 截止日期
        end_date = format_date(str(row['end_date'])) if 'end_date' in row and pd.notna(row['end_date']) else "-"
        
        # 股东户数
        holder_num = f"{int(row['holder_num']):,}" if 'holder_num' in row and pd.notna(row['holder_num']) else "-"
        
        # 变化
        change = "-"
        if 'change' in row and pd.notna(row['change']):
            change_val = row['change']
            if change_val > 0:
                change = f"+{int(change_val):,}"
            elif change_val < 0:
                change = f"{int(change_val):,}"
            else:
                change = "0"
        
        # 变化率
        change_pct = "-"
        if 'change_pct' in row and pd.notna(row['change_pct']):
            change_pct_val = row['change_pct']
            if change_pct_val > 0:
                change_pct = f"+{change_pct_val:.2f}%"
            elif change_pct_val < 0:
                change_pct = f"{change_pct_val:.2f}%"
            else:
                change_pct = "0.00%"
        
        result.append(f"{ann_date:<12} {end_date:<12} {holder_num:<12} {change:<12} {change_pct:<12}")
    
    # 统计信息
    result.append("")
    result.append("📊 统计信息：")
    
    if 'holder_num' in df.columns and len(df) > 1:
        # 最新股东户数
        latest_num = df['holder_num'].iloc[0]
        oldest_num = df['holder_num'].iloc[-1]
        result.append(f"  - 最新股东户数: {int(latest_num):,} 户")
        result.append(f"  - 最早股东户数: {int(oldest_num):,} 户")
        
        # 总变化
        total_change = latest_num - oldest_num
        if total_change > 0:
            result.append(f"  - 总变化: +{int(total_change):,} 户（持股分散）")
        elif total_change < 0:
            result.append(f"  - 总变化: {int(total_change):,} 户（持股集中）")
        else:
            result.append(f"  - 总变化: 0 户")
        
        # 变化率
        if oldest_num > 0:
            total_change_pct = (total_change / oldest_num) * 100
            result.append(f"  - 变化率: {total_change_pct:+.2f}%")
    
    # 数据点数量
    result.append(f"  - 数据点数量: {len(df)} 个")
    
    return "\n".join(result)

def format_holder_trade_data(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化股东增减持数据
    
    参数:
        df: 增减持数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的增减持数据"
    
    result = []
    result.append(f"📊 {ts_code} 股东增减持记录")
    result.append("-" * 120)
    result.append("")
    
    # 表头
    result.append(f"{'公告日期':<12} {'股东名称':<25} {'类型':<8} {'变动数量(股)':<18} {'占流通比例(%)':<15} {'平均价格':<12} {'变动后持股(股)':<18}")
    result.append("-" * 120)
    
    # 按公告日期排序（最新的在前）
    if 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    
    for _, row in df.iterrows():
        # 公告日期
        ann_date = format_date(str(row['ann_date'])) if 'ann_date' in row and pd.notna(row['ann_date']) else "-"
        
        # 股东名称
        holder_name = str(row['holder_name'])[:23] if 'holder_name' in row and pd.notna(row['holder_name']) else "-"
        
        # 交易类型
        in_de = row.get('in_de', '-')
        if in_de == 'IN':
            trade_type = "增持"
        elif in_de == 'DE':
            trade_type = "减持"
        else:
            trade_type = str(in_de)
        
        # 变动数量
        change_vol = f"{int(row['change_vol']):,}" if 'change_vol' in row and pd.notna(row['change_vol']) else "-"
        
        # 占流通比例
        change_ratio = f"{row['change_ratio']:.2f}%" if 'change_ratio' in row and pd.notna(row['change_ratio']) else "-"
        
        # 平均价格
        avg_price = f"{row['avg_price']:.2f}" if 'avg_price' in row and pd.notna(row['avg_price']) else "-"
        
        # 变动后持股
        after_share = f"{int(row['after_share']):,}" if 'after_share' in row and pd.notna(row['after_share']) else "-"
        
        result.append(f"{ann_date:<12} {holder_name:<25} {trade_type:<8} {change_vol:<18} {change_ratio:<15} {avg_price:<12} {after_share:<18}")
    
    # 统计信息
    result.append("")
    result.append("📊 统计信息：")
    
    # 增持/减持统计
    if 'in_de' in df.columns:
        increase_count = len(df[df['in_de'] == 'IN'])
        decrease_count = len(df[df['in_de'] == 'DE'])
        result.append(f"  - 增持记录: {increase_count} 条")
        result.append(f"  - 减持记录: {decrease_count} 条")
    
    # 股东类型统计
    if 'holder_type' in df.columns:
        holder_type_map = {"C": "公司", "P": "个人", "G": "高管"}
        for htype, count in df['holder_type'].value_counts().items():
            type_name = holder_type_map.get(htype, htype)
            result.append(f"  - {type_name}股东: {count} 条")
    
    # 总变动数量
    if 'change_vol' in df.columns:
        total_change = df['change_vol'].sum()
        if total_change > 0:
            result.append(f"  - 净增持: {int(total_change):,} 股")
        elif total_change < 0:
            result.append(f"  - 净减持: {int(abs(total_change)):,} 股")
        else:
            result.append(f"  - 净变动: 0 股")
    
    return "\n".join(result)

def format_stock_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化股票日线行情数据输出
    
    参数:
        df: 日线行情数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_daily(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 80)
                result.append(f"{'股票代码':<15} {'收盘价':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 80)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_stock_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的日线行情数据
    
    参数:
        df: 单个股票的日线行情数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 日线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 80)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<10} {high:<10} {low:<10} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 80)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"开盘价: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘价: -")
        result.append(f"最高价: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高价: -")
        result.append(f"最低价: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低价: -")
        result.append(f"收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘价: -")
        result.append(f"昨收价: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收价: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌额: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_stock_weekly_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化股票周线行情数据输出
    
    参数:
        df: 周线行情数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的周线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_weekly(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10周
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易周（最后交易日）: {format_date(date)}")
                result.append("=" * 100)
                result.append(f"{'股票代码':<15} {'收盘价':<10} {'涨跌额':<10} {'涨跌幅':<10} {'波动范围':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 100)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    # 计算波动范围（最高价 - 最低价）
                    if pd.notna(row.get('high')) and pd.notna(row.get('low')):
                        swing_range = f"{row['high'] - row['low']:.2f}"
                    else:
                        swing_range = "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<10} {change:<10} {pct_chg:<10} {swing_range:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_stock_weekly(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的周线行情数据
    
    参数:
        df: 单个股票的周线行情数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的周线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 周线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20周）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 周数据：")
    result.append("")
    result.append(f"{'交易周':<12} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'涨跌额':<10} {'涨跌幅':<10} {'波动范围':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        # 计算波动范围（最高价 - 最低价）
        if pd.notna(row['high']) and pd.notna(row['low']):
            swing_range = f"{row['high'] - row['low']:.2f}"
        else:
            swing_range = "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<10} {high:<10} {low:<10} {close:<10} {change:<10} {pct_chg:<10} {swing_range:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新周数据摘要：")
        result.append("-" * 80)
        result.append(f"交易周（最后交易日）: {format_date(latest['trade_date'])}")
        result.append(f"周开盘价: {latest['open']:.2f}" if pd.notna(latest['open']) else "周开盘价: -")
        result.append(f"周最高价: {latest['high']:.2f}" if pd.notna(latest['high']) else "周最高价: -")
        result.append(f"周最低价: {latest['low']:.2f}" if pd.notna(latest['low']) else "周最低价: -")
        result.append(f"周收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "周收盘价: -")
        result.append(f"上周收盘价: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "上周收盘价: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌额: {latest['change']:+.2f} (收盘价 - 上周收盘价)")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        # 添加波动范围
        if pd.notna(latest.get('high')) and pd.notna(latest.get('low')):
            swing_range = latest['high'] - latest['low']
            result.append(f"波动范围: {swing_range:.2f} (最高价 - 最低价)")
        if pd.notna(latest.get('vol')):
            result.append(f"周成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"周成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_index_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化A股指数日线行情数据输出
    
    参数:
        df: 指数日线行情数据DataFrame
        ts_code: 指数代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的指数日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个指数或多个指数
    if ts_code:
        # 按指数代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            index_df = df[df['ts_code'] == code]
            if not index_df.empty:
                result.append(format_single_index_daily(index_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有指数
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 80)
                result.append(f"{'指数代码':<15} {'收盘点位':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 80)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<12} {change:<12} {pct_chg:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_index_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个指数的日线行情数据
    
    参数:
        df: 单个指数的日线行情数据DataFrame
        ts_code: 指数代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 日线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
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
        result.append("-" * 80)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"开盘点位: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘点位: -")
        result.append(f"最高点位: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高点位: -")
        result.append(f"最低点位: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低点位: -")
        result.append(f"收盘点位: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘点位: -")
        result.append(f"昨收点位: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收点位: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌点位: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_etf_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化ETF日线行情数据输出
    
    参数:
        df: ETF日线行情数据DataFrame
        ts_code: ETF代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的ETF日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个ETF或多个ETF
    if ts_code:
        # 按ETF代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            etf_df = df[df['ts_code'] == code]
            if not etf_df.empty:
                result.append(format_single_etf_daily(etf_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有ETF
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 80)
                result.append(f"{'ETF代码':<15} {'收盘价':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 80)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_etf_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个ETF的日线行情数据
    
    参数:
        df: 单个ETF的日线行情数据DataFrame
        ts_code: ETF代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} ETF日线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<10} {high:<10} {low:<10} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 80)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"开盘价: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘价: -")
        result.append(f"最高价: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高价: -")
        result.append(f"最低价: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低价: -")
        result.append(f"收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘价: -")
        result.append(f"昨收价: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收价: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌额: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_date(date_str: str) -> str:
    """
    格式化日期字符串（YYYYMMDD -> YYYY-MM-DD）
    
    参数:
        date_str: 日期字符串（YYYYMMDD格式）
    
    返回:
        格式化后的日期字符串（YYYY-MM-DD格式）
    """
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str