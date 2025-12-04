"""融资融券相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING, Optional
from config.token_manager import get_tushare_token

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from cache.margin_cache_manager import margin_cache_manager
from cache.margin_detail_cache_manager import margin_detail_cache_manager
from utils.common import format_date


def register_margin_tools(mcp: "FastMCP"):
    """注册融资融券相关工具"""
    
    @mcp.tool()
    def get_margin(
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        exchange_id: str = ""
    ) -> str:
        """
        获取融资融券每日交易汇总数据
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20180802，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20180101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20181231，需与start_date配合使用）
            exchange_id: 交易所代码（SSE上交所SZSE深交所BSE北交所，留空则查询所有交易所）
        
        注意:
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：融资融券数据从证券交易所网站直接获取，提供了有记录以来的全部汇总和明细数据
            - 权限要求：2000积分可获得本接口权限，积分越高权限越大
            - 限量：单次请求最大返回4000行数据，可根据日期循环获取
        
        返回:
            包含融资融券交易汇总数据的格式化字符串
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            exchange_id = exchange_id.strip() if exchange_id else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                if exchange_id:
                    df = margin_cache_manager.get_margin_data(
                        trade_date=trade_date,
                        exchange_id=exchange_id
                    )
                else:
                    # 查询所有交易所在特定日期的数据
                    df = margin_cache_manager.get_margin_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if exchange_id:
                    df = margin_cache_manager.get_margin_data(
                        exchange_id=exchange_id,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not margin_cache_manager.is_cache_data_complete(exchange_id, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有交易所在日期范围内的数据
                    df = margin_cache_manager.get_margin_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                df = margin_cache_manager.get_margin_data(
                    exchange_id=exchange_id,
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
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                if exchange_id:
                    params['exchange_id'] = exchange_id
                
                df = pro.margin(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = margin_cache_manager.save_margin_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if exchange_id:
                            df = margin_cache_manager.get_margin_data(
                                trade_date=trade_date,
                                exchange_id=exchange_id
                            )
                        else:
                            df = margin_cache_manager.get_margin_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if exchange_id:
                            df = margin_cache_manager.get_margin_data(
                                exchange_id=exchange_id,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = margin_cache_manager.get_margin_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        df = margin_cache_manager.get_margin_data(
                            exchange_id=exchange_id,
                            limit=20,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                exchange_info = f"交易所 {exchange_id}" if exchange_id else "所有交易所"
                
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
                return f"未找到 {exchange_info} 在 {date_info} 的融资融券交易汇总数据，请检查参数是否正确"
            
            # 格式化输出
            return format_margin_data(df, exchange_id or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_margin_detail(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取融资融券交易明细数据（按股票代码）
        
        参数:
            ts_code: 股票代码（如：000001.SZ，支持多个股票同时提取，逗号分隔，如：000001.SZ,600000.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20180802，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20180101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20181231，需与start_date配合使用）
        
        注意:
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：本报表基于证券公司报送的融资融券余额数据汇总生成
            - 权限要求：2000积分可获得本接口权限，积分越高权限越大
            - 限量：单次请求最大返回4000行数据，可根据日期循环获取
        
        返回:
            包含融资融券交易明细数据的格式化字符串
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
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
                    df = margin_detail_cache_manager.get_margin_detail_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有股票在特定日期的数据
                    df = margin_detail_cache_manager.get_margin_detail_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = margin_detail_cache_manager.get_margin_detail_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not margin_detail_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有股票在日期范围内的数据
                    df = margin_detail_cache_manager.get_margin_detail_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = margin_detail_cache_manager.get_margin_detail_data(
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
                
                df = pro.margin_detail(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = margin_detail_cache_manager.save_margin_detail_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = margin_detail_cache_manager.get_margin_detail_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = margin_detail_cache_manager.get_margin_detail_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = margin_detail_cache_manager.get_margin_detail_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = margin_detail_cache_manager.get_margin_detail_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        df = margin_detail_cache_manager.get_margin_detail_data(
                            ts_code=ts_code,
                            limit=20,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                stock_info = f"股票 {ts_code}" if ts_code else "股票"
                
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
                return f"未找到 {stock_info} 在 {date_info} 的融资融券交易明细数据，请检查参数是否正确"
            
            # 格式化输出
            return format_margin_detail_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"


def format_margin_data(df: pd.DataFrame, exchange_id: str = "") -> str:
    """
    格式化融资融券交易汇总数据输出
    
    参数:
        df: 融资融券交易汇总数据DataFrame
        exchange_id: 交易所代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的融资融券交易汇总数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append("📊 融资融券每日交易汇总数据")
    result.append("=" * 120)
    result.append("")
    
    # 如果查询的是单个交易所或多个交易所
    if exchange_id:
        # 按交易所分组显示
        exchanges = [ex.strip() for ex in exchange_id.split(',')]
        for ex in exchanges:
            ex_df = df[df['exchange_id'] == ex]
            if not ex_df.empty:
                result.append(format_single_exchange_margin(ex_df, ex))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有交易所
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:20]:  # 最多显示最近20个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 120)
                result.append(f"{'交易所':<10} {'融资余额(元)':<20} {'融资买入额(元)':<20} {'融资偿还额(元)':<20} {'融券余额(元)':<20} {'融资融券余额(元)':<20}")
                result.append("-" * 120)
                for _, row in date_df.iterrows():
                    exchange_name = get_exchange_name(row['exchange_id'])
                    rzye = format_large_number(row['rzye']) if pd.notna(row['rzye']) else "-"
                    rzmre = format_large_number(row['rzmre']) if pd.notna(row['rzmre']) else "-"
                    rzche = format_large_number(row['rzche']) if pd.notna(row['rzche']) else "-"
                    rqye = format_large_number(row['rqye']) if pd.notna(row['rqye']) else "-"
                    rzrqye = format_large_number(row['rzrqye']) if pd.notna(row['rzrqye']) else "-"
                    result.append(f"{exchange_name:<10} {rzye:<20} {rzmre:<20} {rzche:<20} {rqye:<20} {rzrqye:<20}")
                result.append("")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源于证券交易所网站，由券商申报的数据汇总")
    result.append("  - 本日融资余额 = 前日融资余额 + 本日融资买入 - 本日融资偿还额")
    result.append("  - 本日融券余量 = 前日融券余量 + 本日融券卖出量 - 本日融券买入量 - 本日现券偿还量")
    result.append("  - 本日融券余额 = 本日融券余量 × 本日收盘价")
    result.append("  - 本日融资融券余额 = 本日融资余额 + 本日融券余额")
    result.append("  - 2014年9月22日起，融资融券交易总量数据包含调出标的证券名单的证券的融资融券余额")
    
    return "\n".join(result)


def format_single_exchange_margin(df: pd.DataFrame, exchange_id: str) -> str:
    """
    格式化单个交易所的融资融券数据
    
    参数:
        df: 单个交易所的融资融券数据DataFrame
        exchange_id: 交易所代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {exchange_id} 的融资融券数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    exchange_name = get_exchange_name(exchange_id)
    result = []
    result.append(f"📈 {exchange_name} ({exchange_id}) 融资融券数据")
    result.append("=" * 120)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'融资余额(元)':<20} {'融资买入额(元)':<20} {'融资偿还额(元)':<20} {'融券余额(元)':<20} {'融券卖出量':<18} {'融资融券余额(元)':<20}")
    result.append("-" * 140)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        rzye = format_large_number(row['rzye']) if pd.notna(row['rzye']) else "-"
        rzmre = format_large_number(row['rzmre']) if pd.notna(row['rzmre']) else "-"
        rzche = format_large_number(row['rzche']) if pd.notna(row['rzche']) else "-"
        rqye = format_large_number(row['rqye']) if pd.notna(row['rqye']) else "-"
        rqmcl = format_large_number(row['rqmcl']) if pd.notna(row['rqmcl']) else "-"
        rzrqye = format_large_number(row['rzrqye']) if pd.notna(row['rzrqye']) else "-"
        
        result.append(f"{trade_date:<12} {rzye:<20} {rzmre:<20} {rzche:<20} {rqye:<20} {rqmcl:<18} {rzrqye:<20}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 120)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"融资余额: {format_large_number(latest['rzye'])} 元" if pd.notna(latest['rzye']) else "融资余额: -")
        result.append(f"融资买入额: {format_large_number(latest['rzmre'])} 元" if pd.notna(latest['rzmre']) else "融资买入额: -")
        result.append(f"融资偿还额: {format_large_number(latest['rzche'])} 元" if pd.notna(latest['rzche']) else "融资偿还额: -")
        result.append(f"融券余额: {format_large_number(latest['rqye'])} 元" if pd.notna(latest['rqye']) else "融券余额: -")
        result.append(f"融券卖出量: {format_large_number(latest['rqmcl'])} 股/份/手" if pd.notna(latest['rqmcl']) else "融券卖出量: -")
        result.append(f"融资融券余额: {format_large_number(latest['rzrqye'])} 元" if pd.notna(latest['rzrqye']) else "融资融券余额: -")
        result.append(f"融券余量: {format_large_number(latest['rqyl'])} 股/份/手" if pd.notna(latest.get('rqyl')) and pd.notna(latest['rqyl']) else "融券余量: -")
        
        # 计算融资净买入（融资买入额 - 融资偿还额）
        if pd.notna(latest.get('rzmre')) and pd.notna(latest.get('rzche')):
            net_buy = latest['rzmre'] - latest['rzche']
            result.append(f"融资净买入: {format_large_number(net_buy)} 元")
    
    return "\n".join(result)


def format_large_number(num: float) -> str:
    """
    格式化大数字（添加千分位分隔符）
    
    参数:
        num: 数字
    
    返回:
        格式化后的字符串
    """
    if pd.isna(num):
        return "-"
    
    # 转换为整数（去掉小数部分）
    num_int = int(num)
    
    # 添加千分位分隔符
    return f"{num_int:,}"


def format_margin_detail_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化融资融券交易明细数据输出
    
    参数:
        df: 融资融券交易明细数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的融资融券交易明细数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append("📊 融资融券交易明细数据")
    result.append("=" * 140)
    result.append("")
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_margin_detail(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 140)
                # 按融资余额排序，显示前20只股票
                date_df_sorted = date_df.sort_values('rzye', ascending=False).head(20)
                result.append(f"{'股票代码':<15} {'融资余额(元)':<20} {'融资买入额(元)':<20} {'融资偿还额(元)':<20} {'融券余额(元)':<20} {'融资融券余额(元)':<20}")
                result.append("-" * 140)
                for _, row in date_df_sorted.iterrows():
                    code = row['ts_code']
                    rzye = format_large_number(row['rzye']) if pd.notna(row['rzye']) else "-"
                    rzmre = format_large_number(row['rzmre']) if pd.notna(row['rzmre']) else "-"
                    rzche = format_large_number(row['rzche']) if pd.notna(row['rzche']) else "-"
                    rqye = format_large_number(row['rqye']) if pd.notna(row['rqye']) else "-"
                    rzrqye = format_large_number(row['rzrqye']) if pd.notna(row['rzrqye']) else "-"
                    result.append(f"{code:<15} {rzye:<20} {rzmre:<20} {rzche:<20} {rqye:<20} {rzrqye:<20}")
                
                if len(date_df) > 20:
                    result.append(f"（共 {len(date_df)} 只股票，仅显示融资余额前 20 只）")
                result.append("")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源于证券公司报送的融资融券余额数据汇总生成")
    result.append("  - 本日融资余额 = 前日融资余额 + 本日融资买入 - 本日融资偿还额")
    result.append("  - 本日融券余量 = 前日融券余量 + 本日融券卖出量 - 本日融券买入量 - 本日现券偿还量")
    result.append("  - 本日融券余额 = 本日融券余量 × 本日收盘价")
    result.append("  - 本日融资融券余额 = 本日融资余额 + 本日融券余额")
    result.append("  - 单位说明：股（标的证券为股票）、份（标的证券为基金）、手（标的证券为债券）")
    result.append("  - 2014年9月22日起，融资融券交易总量数据包含调出标的证券名单的证券的融资融券余额")
    
    return "\n".join(result)


def format_single_stock_margin_detail(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的融资融券明细数据
    
    参数:
        df: 单个股票的融资融券明细数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的融资融券明细数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 融资融券明细数据")
    result.append("=" * 140)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'融资余额(元)':<20} {'融资买入额(元)':<20} {'融资偿还额(元)':<20} {'融券余额(元)':<20} {'融券卖出量':<18} {'融资融券余额(元)':<20}")
    result.append("-" * 140)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        rzye = format_large_number(row['rzye']) if pd.notna(row['rzye']) else "-"
        rzmre = format_large_number(row['rzmre']) if pd.notna(row['rzmre']) else "-"
        rzche = format_large_number(row['rzche']) if pd.notna(row['rzche']) else "-"
        rqye = format_large_number(row['rqye']) if pd.notna(row['rqye']) else "-"
        rqmcl = format_large_number(row['rqmcl']) if pd.notna(row['rqmcl']) else "-"
        rzrqye = format_large_number(row['rzrqye']) if pd.notna(row['rzrqye']) else "-"
        
        result.append(f"{trade_date:<12} {rzye:<20} {rzmre:<20} {rzche:<20} {rqye:<20} {rqmcl:<18} {rzrqye:<20}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 140)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"融资余额: {format_large_number(latest['rzye'])} 元" if pd.notna(latest['rzye']) else "融资余额: -")
        result.append(f"融资买入额: {format_large_number(latest['rzmre'])} 元" if pd.notna(latest['rzmre']) else "融资买入额: -")
        result.append(f"融资偿还额: {format_large_number(latest['rzche'])} 元" if pd.notna(latest['rzche']) else "融资偿还额: -")
        result.append(f"融券余额: {format_large_number(latest['rqye'])} 元" if pd.notna(latest['rqye']) else "融券余额: -")
        result.append(f"融券卖出量: {format_large_number(latest['rqmcl'])} 股/份/手" if pd.notna(latest['rqmcl']) else "融券卖出量: -")
        result.append(f"融资融券余额: {format_large_number(latest['rzrqye'])} 元" if pd.notna(latest['rzrqye']) else "融资融券余额: -")
        
        # 计算融资净买入（融资买入额 - 融资偿还额）
        if pd.notna(latest.get('rzmre')) and pd.notna(latest.get('rzche')):
            net_buy = latest['rzmre'] - latest['rzche']
            result.append(f"融资净买入: {format_large_number(net_buy)} 元")
        
        # 计算融资余额变化趋势（如果有历史数据）
        if len(df) > 1:
            prev = df.iloc[1]
            if pd.notna(latest.get('rzye')) and pd.notna(prev.get('rzye')):
                rzye_change = latest['rzye'] - prev['rzye']
                rzye_change_pct = (rzye_change / prev['rzye']) * 100 if prev['rzye'] > 0 else 0
                if rzye_change > 0:
                    result.append(f"融资余额变化: +{format_large_number(rzye_change)} 元 (+{rzye_change_pct:.2f}%)")
                elif rzye_change < 0:
                    result.append(f"融资余额变化: {format_large_number(rzye_change)} 元 ({rzye_change_pct:.2f}%)")
                else:
                    result.append(f"融资余额变化: 0 元")
    
    return "\n".join(result)


def get_exchange_name(exchange_id: str) -> str:
    """
    获取交易所中文名称
    
    参数:
        exchange_id: 交易所代码
    
    返回:
        交易所中文名称
    """
    exchange_map = {
        'SSE': '上交所',
        'SZSE': '深交所',
        'BSE': '北交所'
    }
    return exchange_map.get(exchange_id, exchange_id)

