"""概念板块相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING, Optional, List
from datetime import datetime

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from config.token_manager import get_tushare_token
from tools.alpha_strategy_analyzer import (
    analyze_sector_alpha,
    rank_sectors_alpha,
    format_alpha_analysis,
    calculate_alpha_rank_velocity
)

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

def format_concept_data(df: pd.DataFrame, include_header: bool = False) -> str:
    """
    格式化概念板块数据输出
    
    参数:
        df: 概念板块数据DataFrame
        include_header: 是否包含标题（默认False）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的概念板块数据"
    
    # 按涨跌幅排序（降序）
    if 'pct_change' in df.columns:
        df = df.sort_values('pct_change', ascending=False)
    
    result = []
    if include_header:
        result.append("📊 东方财富概念板块数据")
        result.append("=" * 120)
        result.append("")
    
    # 显示数据统计
    result.append(f"📈 共找到 {len(df)} 个概念板块")
    result.append("")
    
    # 表头
    result.append(f"{'概念代码':<15} {'概念名称':<20} {'涨跌幅':<10} {'领涨股票':<15} {'领涨涨跌幅':<12} {'总市值(万元)':<15} {'换手率':<10} {'上涨/下跌':<12}")
    result.append("-" * 120)
    
    for _, row in df.iterrows():
        # 概念代码
        ts_code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
        
        # 概念名称
        name = str(row['name'])[:18] if 'name' in row and pd.notna(row['name']) else "-"
        
        # 涨跌幅
        pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
        
        # 领涨股票
        leading = str(row['leading'])[:13] if 'leading' in row and pd.notna(row['leading']) else "-"
        
        # 领涨股票涨跌幅
        leading_pct = f"{row['leading_pct']:+.2f}%" if 'leading_pct' in row and pd.notna(row['leading_pct']) else "-"
        
        # 总市值
        total_mv = f"{row['total_mv']:,.0f}" if 'total_mv' in row and pd.notna(row['total_mv']) else "-"
        
        # 换手率
        turnover_rate = f"{row['turnover_rate']:.2f}%" if 'turnover_rate' in row and pd.notna(row['turnover_rate']) else "-"
        
        # 上涨/下跌家数
        up_num = int(row['up_num']) if 'up_num' in row and pd.notna(row['up_num']) else 0
        down_num = int(row['down_num']) if 'down_num' in row and pd.notna(row['down_num']) else 0
        up_down = f"{up_num}/{down_num}"
        
        result.append(f"{ts_code:<15} {name:<20} {pct_change:<10} {leading:<15} {leading_pct:<12} {total_mv:<15} {turnover_rate:<10} {up_down:<12}")
    
    # 统计信息
    result.append("")
    result.append("📊 统计信息：")
    
    if 'pct_change' in df.columns:
        # 涨跌幅统计
        positive_count = len(df[df['pct_change'] > 0])
        negative_count = len(df[df['pct_change'] < 0])
        flat_count = len(df[df['pct_change'] == 0])
        result.append(f"  - 上涨板块: {positive_count} 个")
        result.append(f"  - 下跌板块: {negative_count} 个")
        result.append(f"  - 平盘板块: {flat_count} 个")
        
        # 涨跌幅范围
        if not df['pct_change'].isna().all():
            max_pct = df['pct_change'].max()
            min_pct = df['pct_change'].min()
            result.append(f"  - 最大涨跌幅: {max_pct:+.2f}%")
            result.append(f"  - 最小涨跌幅: {min_pct:+.2f}%")
    
    if 'turnover_rate' in df.columns:
        # 换手率统计
        if not df['turnover_rate'].isna().all():
            avg_turnover = df['turnover_rate'].mean()
            result.append(f"  - 平均换手率: {avg_turnover:.2f}%")
    
    if 'total_mv' in df.columns:
        # 总市值统计
        if not df['total_mv'].isna().all():
            total_market_value = df['total_mv'].sum()
            result.append(f"  - 总市值合计: {total_market_value:,.0f} 万元")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：东方财富概念板块")
    result.append("  - 总市值单位：万元")
    result.append("  - 换手率：反映板块活跃度")
    result.append("  - 上涨/下跌：上涨家数/下跌家数")
    
    return "\n".join(result)

def format_concept_member_data(df: pd.DataFrame, show_date: bool = True, show_concept: bool = True) -> str:
    """
    格式化概念板块成分数据输出
    
    参数:
        df: 概念板块成分数据DataFrame
        show_date: 是否显示交易日期列（默认True）
        show_concept: 是否显示概念代码列（默认True）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的概念板块成分数据"
    
    result = []
    result.append(f"📈 共找到 {len(df)} 只成分股")
    result.append("")
    
    # 根据参数决定表头
    if show_date and show_concept:
        # 表头：显示所有列
        result.append(f"{'交易日期':<12} {'概念代码':<15} {'成分代码':<15} {'成分股名称':<20}")
        result.append("-" * 80)
        
        for _, row in df.iterrows():
            trade_date = format_date(str(row['trade_date'])) if 'trade_date' in row and pd.notna(row['trade_date']) else "-"
            ts_code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
            con_code = str(row['con_code']) if 'con_code' in row and pd.notna(row['con_code']) else "-"
            name = str(row['name'])[:18] if 'name' in row and pd.notna(row['name']) else "-"
            result.append(f"{trade_date:<12} {ts_code:<15} {con_code:<15} {name:<20}")
    elif show_concept:
        # 表头：不显示日期
        result.append(f"{'概念代码':<15} {'成分代码':<15} {'成分股名称':<20}")
        result.append("-" * 60)
        
        for _, row in df.iterrows():
            ts_code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
            con_code = str(row['con_code']) if 'con_code' in row and pd.notna(row['con_code']) else "-"
            name = str(row['name'])[:18] if 'name' in row and pd.notna(row['name']) else "-"
            result.append(f"{ts_code:<15} {con_code:<15} {name:<20}")
    else:
        # 表头：只显示成分代码和名称
        result.append(f"{'成分代码':<15} {'成分股名称':<20}")
        result.append("-" * 40)
        
        for _, row in df.iterrows():
            con_code = str(row['con_code']) if 'con_code' in row and pd.notna(row['con_code']) else "-"
            name = str(row['name'])[:18] if 'name' in row and pd.notna(row['name']) else "-"
            result.append(f"{con_code:<15} {name:<20}")
    
    # 统计信息
    result.append("")
    result.append("📊 统计信息：")
    
    # 按概念代码分组统计
    if 'ts_code' in df.columns:
        concept_count = df['ts_code'].nunique()
        if concept_count > 1:
            result.append(f"  - 涉及概念板块: {concept_count} 个")
    
    # 按交易日期分组统计
    if 'trade_date' in df.columns:
        date_count = df['trade_date'].nunique()
        if date_count > 1:
            result.append(f"  - 涉及交易日期: {date_count} 个")
    
    # 成分股统计
    if 'con_code' in df.columns:
        stock_count = df['con_code'].nunique()
        result.append(f"  - 成分股数量: {stock_count} 只")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：东方财富板块成分")
    result.append("  - 可以根据概念板块代码和交易日期，获取历史成分")
    result.append("  - 限量：单次最大获取5000条数据，可以通过日期和代码循环获取")
    
    return "\n".join(result)

def get_concept_codes(trade_date: str = None) -> List[str]:
    """
    获取所有东财概念板块代码列表
    
    参数:
        trade_date: 交易日期（YYYYMMDD格式，默认今天）
    
    返回:
        概念板块代码列表
    """
    token = get_tushare_token()
    if not token:
        return []
    
    if trade_date is None:
        trade_date = datetime.now().strftime('%Y%m%d')
    
    try:
        pro = ts.pro_api()
        # 获取指定日期的所有概念板块
        df = pro.dc_index(trade_date=trade_date)
        
        if df.empty:
            return []
        
        # 提取唯一的板块代码
        if 'ts_code' in df.columns:
            codes = df['ts_code'].unique().tolist()
            return sorted(codes)
        
        return []
    except Exception as e:
        print(f"获取概念板块代码失败: {str(e)}", file=__import__('sys').stderr)
        return []

def register_concept_tools(mcp: "FastMCP"):
    """注册概念板块相关工具"""
    
    @mcp.tool()
    def get_eastmoney_concept_board(
        ts_code: str = "",
        name: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取东方财富概念板块实时行情数据
        
        参数:
            ts_code: 指数代码（支持多个代码同时输入，用逗号分隔，如：BK1186.DC,BK1185.DC）
            name: 板块名称（例如：人形机器人）
            trade_date: 交易日期（YYYYMMDD格式，如：20250103，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20250101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20250131，需与start_date配合使用）
        
        注意:
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取东方财富每个交易日的概念板块数据，支持按日期查询
            - 限量：单次最大可获取5000条数据，历史数据可根据日期循环获取
            - 权限：用户积累6000积分可调取
        
        返回:
            包含概念板块数据的格式化字符串，包括：
            - 概念代码、概念名称
            - 涨跌幅、领涨股票及涨跌幅
            - 总市值、换手率
            - 上涨家数、下跌家数
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not name and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：概念代码(ts_code)、板块名称(name)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if name:
                params['name'] = name
            if trade_date:
                params['trade_date'] = trade_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 获取概念板块数据
            df = pro.dc_index(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"概念代码: {ts_code}")
                if name:
                    param_info.append(f"板块名称: {name}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的概念板块数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            result = []
            result.append("📊 东方财富概念板块数据")
            result.append("=" * 120)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"概念代码: {ts_code}")
            if name:
                query_info.append(f"板块名称: {name}")
            if trade_date:
                query_info.append(f"交易日期: {format_date(trade_date)}")
            if start_date or end_date:
                date_range = f"{format_date(start_date) if start_date else '开始'} 至 {format_date(end_date) if end_date else '结束'}"
                query_info.append(f"日期范围: {date_range}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 如果有多个交易日期，按日期分组显示
            if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
                dates = sorted(df['trade_date'].unique(), reverse=True)
                for date in dates[:10]:  # 最多显示最近10个交易日
                    date_df = df[df['trade_date'] == date]
                    if not date_df.empty:
                        result.append(f"📅 交易日期: {format_date(date)}")
                        result.append("=" * 120)
                        result.append(format_concept_data(date_df, include_header=False))
                        result.append("")
                
                if len(dates) > 10:
                    result.append(f"（共 {len(dates)} 个交易日，仅显示最近 10 个）")
            else:
                # 单个日期或没有日期字段，直接显示
                result.append(format_concept_data(df, include_header=False))
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_eastmoney_concept_member(
        ts_code: str = "",
        con_code: str = "",
        trade_date: str = ""
    ) -> str:
        """
        获取东方财富板块每日成分数据
        
        参数:
            ts_code: 板块指数代码（如：BK1184.DC人形机器人，可选）
            con_code: 成分股票代码（如：002117.SZ，可选）
            trade_date: 交易日期（YYYYMMDD格式，如：20250102，可选）
        
        注意:
            - 可以根据概念板块代码和交易日期，获取历史成分
            - 限量：单次最大获取5000条数据，可以通过日期和代码循环获取
            - 权限：用户积累6000积分可调取
            - 本接口只限个人学习和研究使用，如需商业用途，请自行联系东方财富解决数据采购问题
        
        返回:
            包含概念板块成分数据的格式化字符串，包括：
            - 交易日期
            - 概念代码
            - 成分代码（股票代码）
            - 成分股名称
        
        示例:
            - 获取2025年1月2日的人形机器人概念板块成分列表：
              ts_code='BK1184.DC', trade_date='20250102'
            - 查询某只股票属于哪些概念板块：
              con_code='002117.SZ'
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not con_code and not trade_date:
            return "请至少提供以下参数之一：板块指数代码(ts_code)、成分股票代码(con_code)或交易日期(trade_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if con_code:
                params['con_code'] = con_code
            if trade_date:
                params['trade_date'] = trade_date
            
            # 获取概念板块成分数据
            df = pro.dc_member(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"板块指数代码: {ts_code}")
                if con_code:
                    param_info.append(f"成分股票代码: {con_code}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                
                return f"未找到符合条件的概念板块成分数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期和概念代码排序
            sort_columns = []
            if 'trade_date' in df.columns:
                sort_columns.append('trade_date')
            if 'ts_code' in df.columns:
                sort_columns.append('ts_code')
            if sort_columns:
                df = df.sort_values(sort_columns, ascending=[False] * len(sort_columns))
            
            # 格式化输出
            result = []
            result.append("📊 东方财富板块成分数据")
            result.append("=" * 80)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"板块指数代码: {ts_code}")
            if con_code:
                query_info.append(f"成分股票代码: {con_code}")
            if trade_date:
                query_info.append(f"交易日期: {format_date(trade_date)}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 如果有多个交易日期或多个概念板块，按日期和概念分组显示
            if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
                dates = sorted(df['trade_date'].unique(), reverse=True)
                for date in dates[:10]:  # 最多显示最近10个交易日
                    date_df = df[df['trade_date'] == date]
                    if not date_df.empty:
                        result.append(f"📅 交易日期: {format_date(date)}")
                        result.append("=" * 80)
                        
                        # 如果该日期有多个概念板块，按概念板块分组
                        if 'ts_code' in date_df.columns and len(date_df['ts_code'].unique()) > 1:
                            concepts = date_df['ts_code'].unique()
                            for concept in concepts:
                                concept_df = date_df[date_df['ts_code'] == concept]
                                if not concept_df.empty:
                                    result.append(f"📌 概念板块: {concept} ({len(concept_df)} 只成分股)")
                                    result.append("-" * 80)
                                    # 不显示日期和概念代码（已在标题中显示）
                                    result.append(format_concept_member_data(concept_df, show_date=False, show_concept=False))
                                    result.append("")
                        else:
                            # 单个概念板块，不显示日期和概念代码
                            result.append(format_concept_member_data(date_df, show_date=False, show_concept=False))
                            result.append("")
                
                if len(dates) > 10:
                    result.append(f"（共 {len(dates)} 个交易日，仅显示最近 10 个）")
            elif 'ts_code' in df.columns and len(df['ts_code'].unique()) > 1:
                # 多个概念板块，按概念板块分组
                concepts = df['ts_code'].unique()
                for concept in concepts:
                    concept_df = df[df['ts_code'] == concept]
                    if not concept_df.empty:
                        result.append(f"📌 概念板块: {concept} ({len(concept_df)} 只成分股)")
                        result.append("-" * 80)
                        # 不显示概念代码（已在标题中显示）
                        result.append(format_concept_member_data(concept_df, show_date=True, show_concept=False))
                        result.append("")
            else:
                # 单个日期或单个概念板块，根据查询条件决定显示哪些列
                show_date_col = not trade_date or len(df['trade_date'].unique()) > 1 if 'trade_date' in df.columns else False
                show_concept_col = not ts_code or len(df['ts_code'].unique()) > 1 if 'ts_code' in df.columns else False
                result.append(format_concept_member_data(df, show_date=show_date_col, show_concept=show_concept_col))
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_eastmoney_concept_daily(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        idx_type: str = ""
    ) -> str:
        """
        获取东财概念板块、行业指数板块、地域板块行情数据
        
        参数:
            ts_code: 板块代码（格式：xxxxx.DC，如：BK1184.DC，可选）
            trade_date: 交易日期（YYYYMMDD格式，如：20250513，查询指定日期的数据，可选）
            start_date: 开始日期（YYYYMMDD格式，如：20250101，需与end_date配合使用，可选）
            end_date: 结束日期（YYYYMMDD格式，如：20250531，需与start_date配合使用，可选）
            idx_type: 板块类型（可选值：概念板块、行业板块、地域板块，可选）
        
        注意:
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取东财概念板块、行业指数板块、地域板块行情数据，历史数据开始于2020年
            - 限量：单次最大2000条数据，可根据日期参数循环获取
            - 权限：用户积累6000积分可调取
            - 本接口只限个人学习和研究使用，如需商业用途，请自行联系东方财富解决数据采购问题
        
        返回:
            包含板块行情数据的格式化字符串，包括：
            - 板块代码、交易日期
            - 开盘、最高、最低、收盘点位
            - 涨跌点位、涨跌幅
            - 成交量、成交额
            - 振幅、换手率
        
        示例:
            - 获取2025年5月13日所有概念板块行情：
              trade_date='20250513'
            - 获取某个板块的历史行情：
              ts_code='BK1184.DC', start_date='20250101', end_date='20250531'
            - 获取行业板块行情：
              idx_type='行业板块', trade_date='20250513'
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：板块代码(ts_code)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if trade_date:
                params['trade_date'] = trade_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            if idx_type:
                params['idx_type'] = idx_type
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 获取板块行情数据
            df = pro.dc_daily(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"板块代码: {ts_code}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if idx_type:
                    param_info.append(f"板块类型: {idx_type}")
                
                return f"未找到符合条件的板块行情数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期和涨跌幅排序（最新的在前，涨跌幅降序）
            sort_columns = []
            if 'trade_date' in df.columns:
                sort_columns.append('trade_date')
            if 'pct_change' in df.columns:
                sort_columns.append('pct_change')
            if sort_columns:
                df = df.sort_values(sort_columns, ascending=[False, False])
            
            # 格式化输出
            result = []
            result.append("📊 东财概念/行业/地域板块行情数据")
            result.append("=" * 120)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"板块代码: {ts_code}")
            if trade_date:
                query_info.append(f"交易日期: {format_date(trade_date)}")
            if start_date or end_date:
                date_range = f"{format_date(start_date) if start_date else '开始'} 至 {format_date(end_date) if end_date else '结束'}"
                query_info.append(f"日期范围: {date_range}")
            if idx_type:
                query_info.append(f"板块类型: {idx_type}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 调用格式化函数
            result.append(format_concept_daily_data(df, ts_code or ""))
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def analyze_concept_alpha_strategy(
        concept_code: str = "",
        benchmark_code: str = "000300.SH",
        end_date: str = ""
    ) -> str:
        """
        分析单个东财概念板块的相对强度Alpha
        
        参数:
            concept_code: 概念板块代码（如：BK1184.DC人形机器人、BK1186.DC首发经济等）
            benchmark_code: 基准指数代码（默认：000300.SH沪深300）
            end_date: 结束日期（YYYYMMDD格式，如：20241124，默认今天）
        
        返回:
            包含Alpha分析结果的格式化字符串
        
        说明:
            - 计算2天和5天的区间收益率
            - 计算超额收益Alpha = 板块收益 - 基准收益
            - 综合得分 = Alpha_2 × 60% + Alpha_5 × 40%
        """
        if not concept_code:
            return "请提供概念板块代码(如：BK1184.DC、BK1186.DC等)"
        
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        result = analyze_sector_alpha(concept_code, benchmark_code, end_date)
        
        if "error" in result:
            return result["error"]
        
        # 格式化输出
        output = []
        output.append(f"📊 {concept_code} 相对强度Alpha分析")
        output.append("=" * 80)
        output.append("")
        output.append(f"基准指数: {result['benchmark_code']}")
        output.append(f"分析日期: {result['end_date']}")
        output.append("")
        output.append("📈 收益率分析：")
        output.append("-" * 80)
        
        if pd.notna(result['r_sector_2']):
            output.append(f"板块2日收益率: {result['r_sector_2']*100:.2f}%")
        else:
            output.append("板块2日收益率: 数据不足")
        
        if pd.notna(result['r_sector_5']):
            output.append(f"板块5日收益率: {result['r_sector_5']*100:.2f}%")
        else:
            output.append("板块5日收益率: 数据不足")
        
        if pd.notna(result['r_benchmark_2']):
            output.append(f"基准2日收益率: {result['r_benchmark_2']*100:.2f}%")
        else:
            output.append("基准2日收益率: 数据不足")
        
        if pd.notna(result['r_benchmark_5']):
            output.append(f"基准5日收益率: {result['r_benchmark_5']*100:.2f}%")
        else:
            output.append("基准5日收益率: 数据不足")
        
        output.append("")
        output.append("🎯 Alpha分析：")
        output.append("-" * 80)
        
        if pd.notna(result['alpha_2']):
            alpha_2_pct = result['alpha_2'] * 100
            status_2 = "✅ 跑赢大盘" if alpha_2_pct > 0 else "❌ 跑输大盘"
            output.append(f"2日Alpha: {alpha_2_pct:+.2f}% {status_2}")
        else:
            output.append("2日Alpha: 数据不足")
        
        if pd.notna(result['alpha_5']):
            alpha_5_pct = result['alpha_5'] * 100
            status_5 = "✅ 跑赢大盘" if alpha_5_pct > 0 else "❌ 跑输大盘"
            output.append(f"5日Alpha: {alpha_5_pct:+.2f}% {status_5}")
        else:
            output.append("5日Alpha: 数据不足")
        
        output.append("")
        output.append("🏆 综合评分：")
        output.append("-" * 80)
        
        if pd.notna(result['score']):
            score_pct = result['score'] * 100
            if score_pct > 5:
                rating = "⭐⭐⭐ 非常强势"
            elif score_pct > 2:
                rating = "⭐⭐ 强势"
            elif score_pct > 0:
                rating = "⭐ 略强"
            elif score_pct > -2:
                rating = "➖ 中性"
            elif score_pct > -5:
                rating = "⚠️ 弱势"
            else:
                rating = "❌ 非常弱势"
            
            output.append(f"综合得分: {score_pct:+.2f}% {rating}")
            output.append("")
            output.append("计算公式: 得分 = Alpha_2 × 60% + Alpha_5 × 40%")
        else:
            output.append("综合得分: 数据不足")
        
        return "\n".join(output)
    
    @mcp.tool()
    def rank_concepts_by_alpha(
        benchmark_code: str = "000300.SH",
        end_date: str = "",
        top_n: int = 20
    ) -> str:
        """
        对所有东财概念板块进行Alpha排名
        
        参数:
            benchmark_code: 基准指数代码（默认：000300.SH沪深300）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            top_n: 显示前N名（默认20）
        
        返回:
            包含排名结果的格式化字符串
        
        说明:
            - 自动获取指定日期的所有概念板块
            - 按综合得分降序排列
            - 显示前N名强势板块
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        try:
            # 获取概念板块代码列表
            concept_codes = get_concept_codes(end_date or datetime.now().strftime('%Y%m%d'))
            
            if not concept_codes:
                return "无法获取概念板块列表，请检查网络连接和token配置。\n提示：可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 进行Alpha排名
            df = rank_sectors_alpha(concept_codes, benchmark_code, end_date)
            
            if df.empty:
                return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 显示所有排名（如果top_n大于等于总数，显示全部）
            if top_n >= len(df):
                df_display = df
            else:
                df_display = df.head(top_n)
            
            result = format_alpha_analysis(df_display)
            
            # 如果只显示了部分，添加提示
            if top_n < len(df):
                result += f"\n\n（共 {len(df)} 个概念板块，仅显示前 {top_n} 名）"
            else:
                result += f"\n\n（共 {len(df)} 个概念板块）"
            
            return result
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def rank_concepts_alpha_velocity(
        benchmark_code: str = "000300.SH",
        end_date: str = ""
    ) -> str:
        """
        分析东财概念板块Alpha排名上升速度
        
        参数:
            benchmark_code: 基准指数代码（默认：000300.SH沪深300）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
        
        返回:
            包含排名上升速度的格式化字符串，包括：
            - 板块当天alpha值
            - 相较昨日上升位数
            - 相较前天上升位数
            - 一天内上升位数排行
            - 两天内上升位数排行
        
        说明:
            - 自动获取指定日期的所有概念板块
            - 计算排名上升速度（当天对比前一天和前两天的排名变化）
            - 正数表示排名上升，负数表示排名下降
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 如果end_date为空，使用None让calculate_alpha_rank_velocity使用默认值
            if end_date == "":
                end_date = None
            
            # 获取概念板块代码列表
            concept_codes = get_concept_codes(end_date or datetime.now().strftime('%Y%m%d'))
            
            if not concept_codes:
                return "无法获取概念板块列表，请检查网络连接和token配置。\n提示：可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 计算排名上升速度
            df = calculate_alpha_rank_velocity(concept_codes, benchmark_code, end_date)
            
            if df.empty:
                # 如果无法获取排名上升速度数据，尝试获取当前排名作为降级方案
                today = datetime.now().strftime('%Y%m%d')
                df_current = rank_sectors_alpha(concept_codes, benchmark_code, today)
                if not df_current.empty:
                    # 返回当前排名，但提示无法获取历史排名
                    return f"⚠️ 无法获取历史排名数据，仅显示当前排名：\n\n" + format_alpha_analysis(df_current) + "\n\n提示：可能是API限流或历史数据缺失，请稍后重试获取排名上升速度分析。"
                else:
                    return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 获取实际使用的日期信息
            current_date = df.attrs.get('current_date', '未知')
            yesterday_date = df.attrs.get('yesterday_date', None)
            day_before_yesterday_date = df.attrs.get('day_before_yesterday_date', None)
            
            # 格式化日期显示
            def format_date_display(date_str):
                """格式化日期显示（YYYYMMDD -> YYYY-MM-DD）"""
                if date_str and len(str(date_str)) == 8:
                    date_str = str(date_str)
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return str(date_str) if date_str else "无数据"
            
            current_date_display = format_date_display(current_date)
            yesterday_date_display = format_date_display(yesterday_date)
            day_before_yesterday_date_display = format_date_display(day_before_yesterday_date)
            
            # 格式化输出
            output = []
            output.append("📊 东财概念板块Alpha排名上升速度分析")
            output.append("=" * 120)
            output.append("")
            output.append(f"📅 分析日期：")
            output.append(f"  - 当前日期：{current_date_display} ({current_date})")
            output.append(f"  - 对比日期1（较昨日）：{yesterday_date_display} ({yesterday_date if yesterday_date else '无数据'})")
            output.append(f"  - 对比日期2（较前天）：{day_before_yesterday_date_display} ({day_before_yesterday_date if day_before_yesterday_date else '无数据'})")
            output.append("")
            
            # 显示所有板块的基本信息
            output.append("📈 所有板块Alpha值及排名变化：")
            output.append("-" * 120)
            change_1d_label = f"较{yesterday_date_display}变化" if yesterday_date else "较昨日上升"
            change_2d_label = f"较{day_before_yesterday_date_display}变化" if day_before_yesterday_date else "较前天上升"
            output.append(f"{'排名':<6} {'板块代码':<15} {'Alpha值':<12} {change_1d_label:<20} {change_2d_label:<20}")
            output.append("-" * 120)
            
            # 按当前排名排序
            df_sorted = df.sort_values('current_rank', ascending=True)
            
            for _, row in df_sorted.iterrows():
                rank = f"{int(row['current_rank'])}"
                concept_code = row['sector_code']
                alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                
                # 较昨日上升位数
                if pd.notna(row['rank_change_1d']):
                    change_1d = f"{int(row['rank_change_1d']):+d}"
                    if row['rank_change_1d'] > 0:
                        change_1d += " ⬆️"
                    elif row['rank_change_1d'] < 0:
                        change_1d += " ⬇️"
                    else:
                        change_1d += " ➖"
                else:
                    change_1d = "-"
                
                # 较前天上升位数
                if pd.notna(row['rank_change_2d']):
                    change_2d = f"{int(row['rank_change_2d']):+d}"
                    if row['rank_change_2d'] > 0:
                        change_2d += " ⬆️"
                    elif row['rank_change_2d'] < 0:
                        change_2d += " ⬇️"
                    else:
                        change_2d += " ➖"
                else:
                    change_2d = "-"
                
                output.append(f"{rank:<6} {concept_code:<15} {alpha:<12} {change_1d:<12} {change_2d:<12}")
            
            output.append("")
            
            # 一天内上升位数排行（只显示有数据的）
            df_1d = df[df['rank_change_1d'].notna()].copy()
            if not df_1d.empty:
                df_1d = df_1d.sort_values('rank_change_1d', ascending=False)
                output.append(f"🚀 较{yesterday_date_display}排名变化排行（前10名）：")
                output.append("-" * 120)
                output.append(f"{'排名':<6} {'板块代码':<15} {f'{current_date_display}排名':<15} {'排名变化':<12} {'Alpha值':<12}")
                output.append("-" * 120)
                
                for idx, (_, row) in enumerate(df_1d.head(10).iterrows(), 1):
                    rank = f"{int(row['current_rank'])}"
                    concept_code = row['sector_code']
                    change_1d = f"{int(row['rank_change_1d']):+d}"
                    alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                    output.append(f"{idx:<6} {concept_code:<15} {rank:<15} {change_1d:<12} {alpha:<12}")
                
                output.append("")
            
            # 两天内上升位数排行（只显示有数据的）
            df_2d = df[df['rank_change_2d'].notna()].copy()
            if not df_2d.empty:
                df_2d = df_2d.sort_values('rank_change_2d', ascending=False)
                output.append(f"🚀 较{day_before_yesterday_date_display}排名变化排行（前10名）：")
                output.append("-" * 120)
                output.append(f"{'排名':<6} {'板块代码':<15} {f'{current_date_display}排名':<15} {'排名变化':<12} {'Alpha值':<12}")
                output.append("-" * 120)
                
                for idx, (_, row) in enumerate(df_2d.head(10).iterrows(), 1):
                    rank = f"{int(row['current_rank'])}"
                    concept_code = row['sector_code']
                    change_2d = f"{int(row['rank_change_2d']):+d}"
                    alpha = f"{row['current_alpha']*100:.2f}%" if pd.notna(row['current_alpha']) else "-"
                    output.append(f"{idx:<6} {concept_code:<15} {rank:<15} {change_2d:<12} {alpha:<12}")
                
                output.append("")
            
            output.append("📝 说明：")
            output.append("  - Alpha = 板块收益率 - 基准收益率（沪深300）")
            output.append("  - 排名变化 = 对比日期排名 - 当前排名（正数表示排名上升）")
            output.append(f"  - 当前日期：{current_date_display} ({current_date})")
            if yesterday_date:
                output.append(f"  - 对比日期1：{yesterday_date_display} ({yesterday_date})")
            if day_before_yesterday_date:
                output.append(f"  - 对比日期2：{day_before_yesterday_date_display} ({day_before_yesterday_date})")
            output.append("  - 建议关注排名变化较大的板块，可能具有较强动能")
            output.append("")
            output.append(f"📊 统计：共分析 {len(df)} 个概念板块")
            
            return "\n".join(output)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"

def format_concept_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化概念板块行情数据输出
    
    参数:
        df: 概念板块行情数据DataFrame
        ts_code: 板块代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的概念板块行情数据"
    
    result = []
    
    # 如果查询的是单个板块或多个板块
    if ts_code:
        # 按板块代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                result.append(format_single_concept_daily(code_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 如果有多个交易日期，按日期分组显示
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
            dates = sorted(df['trade_date'].unique(), reverse=True)
            for date in dates[:10]:  # 最多显示最近10个交易日
                date_df = df[df['trade_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 交易日期: {format_date(date)}")
                    result.append("=" * 120)
                    result.append(f"{'板块代码':<15} {'收盘点位':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'振幅':<10} {'换手率':<10} {'成交量':<15} {'成交额':<15}")
                    result.append("-" * 120)
                    
                    # 按涨跌幅排序（降序）
                    if 'pct_change' in date_df.columns:
                        date_df = date_df.sort_values('pct_change', ascending=False)
                    
                    for _, row in date_df.iterrows():
                        code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
                        close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
                        change = f"{row['change']:+.2f}" if 'change' in row and pd.notna(row['change']) else "-"
                        pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
                        swing = f"{row['swing']:.2f}%" if 'swing' in row and pd.notna(row['swing']) else "-"
                        turnover_rate = f"{row['turnover_rate']:.2f}%" if 'turnover_rate' in row and pd.notna(row['turnover_rate']) else "-"
                        vol = f"{row['vol']:.0f}" if 'vol' in row and pd.notna(row['vol']) else "-"
                        amount = f"{row['amount']:.0f}" if 'amount' in row and pd.notna(row['amount']) else "-"
                        
                        result.append(f"{code:<15} {close:<12} {change:<12} {pct_change:<10} {swing:<10} {turnover_rate:<10} {vol:<15} {amount:<15}")
                    
                    result.append("")
            
            if len(dates) > 10:
                result.append(f"（共 {len(dates)} 个交易日，仅显示最近 10 个）")
        else:
            # 单个日期或没有日期字段，直接显示所有板块
            result.append(f"📈 共找到 {len(df)} 个板块")
            result.append("")
            result.append(f"{'板块代码':<15} {'收盘点位':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'振幅':<10} {'换手率':<10} {'成交量':<15} {'成交额':<15}")
            result.append("-" * 120)
            
            # 按涨跌幅排序（降序）
            if 'pct_change' in df.columns:
                df = df.sort_values('pct_change', ascending=False)
            
            for _, row in df.iterrows():
                code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
                close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
                change = f"{row['change']:+.2f}" if 'change' in row and pd.notna(row['change']) else "-"
                pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
                swing = f"{row['swing']:.2f}%" if 'swing' in row and pd.notna(row['swing']) else "-"
                turnover_rate = f"{row['turnover_rate']:.2f}%" if 'turnover_rate' in row and pd.notna(row['turnover_rate']) else "-"
                vol = f"{row['vol']:.0f}" if 'vol' in row and pd.notna(row['vol']) else "-"
                amount = f"{row['amount']:.0f}" if 'amount' in row and pd.notna(row['amount']) else "-"
                
                result.append(f"{code:<15} {close:<12} {change:<12} {pct_change:<10} {swing:<10} {turnover_rate:<10} {vol:<15} {amount:<15}")
            
            # 统计信息
            result.append("")
            result.append("📊 统计信息：")
            
            if 'pct_change' in df.columns:
                positive_count = len(df[df['pct_change'] > 0])
                negative_count = len(df[df['pct_change'] < 0])
                flat_count = len(df[df['pct_change'] == 0])
                result.append(f"  - 上涨板块: {positive_count} 个")
                result.append(f"  - 下跌板块: {negative_count} 个")
                result.append(f"  - 平盘板块: {flat_count} 个")
                
                if not df['pct_change'].isna().all():
                    max_pct = df['pct_change'].max()
                    min_pct = df['pct_change'].min()
                    result.append(f"  - 最大涨跌幅: {max_pct:+.2f}%")
                    result.append(f"  - 最小涨跌幅: {min_pct:+.2f}%")
            
            if 'turnover_rate' in df.columns:
                if not df['turnover_rate'].isna().all():
                    avg_turnover = df['turnover_rate'].mean()
                    result.append(f"  - 平均换手率: {avg_turnover:.2f}%")
            
            result.append("")
            result.append("📝 说明：")
            result.append("  - 数据来源：东方财富概念/行业/地域板块")
            result.append("  - 历史数据开始于2020年")
            result.append("  - 限量：单次最大2000条数据")
    
    return "\n".join(result)

def format_single_concept_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个板块的日线行情数据
    
    参数:
        df: 单个板块的日线行情数据DataFrame
        ts_code: 板块代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 日线行情")
    result.append("=" * 120)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'振幅':<10} {'换手率':<10} {'成交量':<15} {'成交额':<15}")
    result.append("-" * 140)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_change = f"{row['pct_change']:+.2f}%" if pd.notna(row['pct_change']) else "-"
        swing = f"{row['swing']:.2f}%" if pd.notna(row['swing']) else "-"
        turnover_rate = f"{row['turnover_rate']:.2f}%" if pd.notna(row['turnover_rate']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<12} {high:<12} {low:<12} {close:<12} {change:<12} {pct_change:<10} {swing:<10} {turnover_rate:<10} {vol:<15} {amount:<15}")
    
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
        result.append(f"开盘点位: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘点位: -")
        result.append(f"最高点位: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高点位: -")
        result.append(f"最低点位: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低点位: -")
        result.append(f"收盘点位: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘点位: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌点位: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_change')):
            result.append(f"涨跌幅: {latest['pct_change']:+.2f}%")
        if pd.notna(latest.get('swing')):
            result.append(f"振幅: {latest['swing']:.2f}%")
        if pd.notna(latest.get('turnover_rate')):
            result.append(f"换手率: {latest['turnover_rate']:.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f}")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f}")
    
    return "\n".join(result)
