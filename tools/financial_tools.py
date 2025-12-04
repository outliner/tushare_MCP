"""财务报表相关MCP工具"""
import tushare as ts
import pandas as pd
from typing import TYPE_CHECKING
from config.token_manager import get_tushare_token
from formatters.financial_formatter import format_income_statement_analysis
from cache.cache_manager import cache_manager
from cache.fina_indicator_cache_manager import fina_indicator_cache_manager
from utils.common import format_date

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

def register_financial_tools(mcp: "FastMCP"):
    """注册财务报表相关工具"""
    
    @mcp.tool()
    def get_income_statement(
        ts_code: str,
        start_date: str = "",
        end_date: str = "",
        report_type: str = "1"
    ) -> str:
        """
        获取利润表数据
        
        参数:
            ts_code: 股票代码（如：000001.SZ）
            start_date: 开始日期（YYYYMMDD格式，如：20230101）
            end_date: 结束日期（YYYYMMDD格式，如：20231231）
            report_type: 报告类型（1合并报表；2单季合并；3调整单季合并表；4调整合并报表；5调整前合并报表；6母公司报表；7母公司单季表；8母公司调整单季表；9母公司调整表；10母公司调整前报表；11母公司调整前合并报表；12母公司调整前报表）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 尝试从缓存获取股票名称
            stock_name = ts_code
            stock_info_df = cache_manager.get_dataframe('stock_basic', ts_code=ts_code)
            if stock_info_df is not None and not stock_info_df.empty:
                stock_name = stock_info_df.iloc[0]['name']
            else:
                # 如果缓存中没有，从API获取并缓存
                pro = ts.pro_api()
                stock_info = pro.stock_basic(ts_code=ts_code)
                if not stock_info.empty:
                    stock_name = stock_info.iloc[0]['name']
                    cache_manager.set('stock_basic', stock_info, ts_code=ts_code)
            
            # 尝试从缓存获取财务报表数据（即使过期也返回）
            cache_params = {
                'ts_code': ts_code,
                'start_date': start_date or '',
                'end_date': end_date or '',
                'report_type': report_type
            }
            df = cache_manager.get_dataframe('income_statement', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('income_statement', **cache_params):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                params = {
                    'ts_code': ts_code,
                    'fields': 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag'
                }
                
                if start_date:
                    params['start_date'] = start_date
                if end_date:
                    params['end_date'] = end_date
                
                df = pro.income(**params)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('income_statement', df, **cache_params)
            
            if df.empty:
                return "未找到符合条件的利润表数据"
                
            # 获取报表类型描述
            report_types = {
                "1": "合并报表",
                "2": "单季合并",
                "3": "调整单季合并表",
                "4": "调整合并报表",
                "5": "调整前合并报表",
                "6": "母公司报表",
                "7": "母公司单季表",
                "8": "母公司调整单季表",
                "9": "母公司调整表",
                "10": "母公司调整前报表",
                "11": "母公司调整前合并报表",
                "12": "母公司调整前报表"
            }
            report_type_desc = report_types.get(report_type, "未知类型")
            
            # 构建输出标题
            title = f"我查询到了 {stock_name}（{ts_code}）的{report_type_desc}利润数据，如下呈现：\n\n"
            
            # 格式化数据并生成分析
            result = format_income_statement_analysis(df)
            
            return title + result
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_fina_indicator(
        ts_code: str = "",
        ann_date: str = "",
        start_date: str = "",
        end_date: str = "",
        period: str = ""
    ) -> str:
        """
        获取财务指标数据
        
        参数:
            ts_code: 股票代码（如：600000.SH，留空则查询所有股票）
            ann_date: 公告日期（YYYYMMDD格式）
            start_date: 报告期开始日期（YYYYMMDD格式，如：20170101，需与end_date配合使用）
            end_date: 报告期结束日期（YYYYMMDD格式，如：20180801，需与start_date配合使用）
            period: 报告期（每年发布4次）
        
        返回:
            包含财务指标数据的格式化字符串
        
        说明:
            - 数据来源于上市公司定期报告，每年发布4次（一季报、半年报、三季报、年报）
            - 包含盈利能力、成长能力、运营能力、偿债能力等各类财务指标
            - 支持按股票代码、公告日期、报告期、日期范围筛选
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not ann_date and not start_date and not end_date and not period:
            return "请至少提供以下参数之一：股票代码(ts_code)、公告日期(ann_date)、报告期(period)或日期范围(start_date/end_date)"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            ann_date = ann_date.strip() if ann_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            period = period.strip() if period else None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if ts_code:
                # 查询特定股票
                df = fina_indicator_cache_manager.get_fina_indicator_data(
                    ts_code=ts_code,
                    ann_date=ann_date,
                    start_date=start_date,
                    end_date=end_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif ann_date:
                # 查询特定公告日期
                df = fina_indicator_cache_manager.get_fina_indicator_data(
                    ann_date=ann_date,
                    start_date=start_date,
                    end_date=end_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围
                df = fina_indicator_cache_manager.get_fina_indicator_data(
                    start_date=start_date,
                    end_date=end_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            else:
                return "请至少提供股票代码(ts_code)、公告日期(ann_date)或日期范围(start_date/end_date)"
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                if ann_date:
                    params['ann_date'] = ann_date
                if start_date:
                    params['start_date'] = start_date
                if end_date:
                    params['end_date'] = end_date
                if period:
                    params['period'] = period
                
                df = pro.fina_indicator(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = fina_indicator_cache_manager.save_fina_indicator_data(df)
                    # 重新从缓存读取以确保数据一致性
                    df = fina_indicator_cache_manager.get_fina_indicator_data(
                        ts_code=ts_code,
                        ann_date=ann_date,
                        start_date=start_date,
                        end_date=end_date
                    )
            
            if df is None or df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if period:
                    param_info.append(f"报告期: {period}")
                
                return f"未找到符合条件的财务指标数据\n查询条件: {', '.join(param_info)}"
            
            # 格式化输出
            return format_fina_indicator_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"


def format_fina_indicator_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化财务指标数据输出
    
    参数:
        df: 财务指标数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的财务指标数据"
    
    # 按报告期排序（最新的在前）
    if 'end_date' in df.columns:
        df = df.sort_values('end_date', ascending=False)
    
    result = []
    result.append("📊 财务指标数据")
    result.append("=" * 160)
    result.append("")
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_fina_indicator(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按报告期查询，显示所有股票
        # 按报告期分组
        if 'end_date' in df.columns:
            dates = df['end_date'].unique()
            for date in sorted(dates, reverse=True)[:5]:  # 最多显示最近5个报告期
                date_df = df[df['end_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 报告期: {format_date(date)}")
                    result.append("=" * 160)
                    result.append(f"{'股票代码':<15} {'每股收益':<12} {'ROE(%)':<10} {'ROA(%)':<10} {'销售毛利率(%)':<14} {'销售净利率(%)':<14} {'资产负债率(%)':<14} {'总资产周转率':<14}")
                    result.append("-" * 160)
                    
                    for _, row in date_df.head(20).iterrows():  # 最多显示20只股票
                        code = row['ts_code']
                        eps = f"{row['eps']:.4f}" if pd.notna(row.get('eps')) else "-"
                        roe = f"{row['roe']:.2f}" if pd.notna(row.get('roe')) else "-"
                        roa = f"{row['roa']:.2f}" if pd.notna(row.get('roa')) else "-"
                        gross_margin = f"{row['grossprofit_margin']:.2f}" if pd.notna(row.get('grossprofit_margin')) else "-"
                        net_margin = f"{row['netprofit_margin']:.2f}" if pd.notna(row.get('netprofit_margin')) else "-"
                        debt_ratio = f"{row['debt_to_assets']:.2f}" if pd.notna(row.get('debt_to_assets')) else "-"
                        assets_turn = f"{row['assets_turn']:.4f}" if pd.notna(row.get('assets_turn')) else "-"
                        
                        result.append(f"{code:<15} {eps:<12} {roe:<10} {roa:<10} {gross_margin:<14} {net_margin:<14} {debt_ratio:<14} {assets_turn:<14}")
                    
                    if len(date_df) > 20:
                        result.append(f"（共 {len(date_df)} 只股票，仅显示前 20 只）")
                    result.append("")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源于上市公司定期报告，每年发布4次（一季报、半年报、三季报、年报）")
    result.append("  - ROE：净资产收益率，反映股东权益的收益水平")
    result.append("  - ROA：总资产报酬率，反映企业资产综合利用效果")
    result.append("  - 销售毛利率：反映产品的盈利能力")
    result.append("  - 销售净利率：反映企业的盈利能力")
    result.append("  - 资产负债率：反映企业的偿债能力")
    result.append("  - 总资产周转率：反映企业资产运营效率")
    
    return "\n".join(result)


def format_single_stock_fina_indicator(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的财务指标数据
    
    参数:
        df: 单个股票的财务指标数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的财务指标数据"
    
    # 按报告期排序（最新的在前）
    if 'end_date' in df.columns:
        df = df.sort_values('end_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 财务指标数据")
    result.append("=" * 160)
    result.append("")
    
    # 显示最近的数据（最多10条）
    display_count = min(10, len(df))
    result.append(f"最近 {display_count} 个报告期数据：")
    result.append("")
    
    # 主要指标表格
    result.append(f"{'报告期':<12} {'公告日期':<12} {'每股收益':<12} {'ROE(%)':<10} {'ROA(%)':<10} {'销售毛利率(%)':<14} {'销售净利率(%)':<14} {'资产负债率(%)':<14}")
    result.append("-" * 160)
    
    for _, row in df.head(display_count).iterrows():
        end_date = format_date(str(row['end_date'])) if pd.notna(row.get('end_date')) else "-"
        ann_date = format_date(str(row['ann_date'])) if pd.notna(row.get('ann_date')) else "-"
        eps = f"{row['eps']:.4f}" if pd.notna(row.get('eps')) else "-"
        roe = f"{row['roe']:.2f}" if pd.notna(row.get('roe')) else "-"
        roa = f"{row['roa']:.2f}" if pd.notna(row.get('roa')) else "-"
        gross_margin = f"{row['grossprofit_margin']:.2f}" if pd.notna(row.get('grossprofit_margin')) else "-"
        net_margin = f"{row['netprofit_margin']:.2f}" if pd.notna(row.get('netprofit_margin')) else "-"
        debt_ratio = f"{row['debt_to_assets']:.2f}" if pd.notna(row.get('debt_to_assets')) else "-"
        
        result.append(f"{end_date:<12} {ann_date:<12} {eps:<12} {roe:<10} {roa:<10} {gross_margin:<14} {net_margin:<14} {debt_ratio:<14}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 160)
        
        if pd.notna(latest.get('end_date')):
            result.append(f"报告期: {format_date(str(latest['end_date']))}")
        if pd.notna(latest.get('ann_date')):
            result.append(f"公告日期: {format_date(str(latest['ann_date']))}")
        
        result.append("")
        result.append("盈利能力指标：")
        result.append(f"  每股收益(EPS): {latest['eps']:.4f}" if pd.notna(latest.get('eps')) else "  每股收益(EPS): -")
        result.append(f"  每股收益(扣非): {latest['dt_eps']:.4f}" if pd.notna(latest.get('dt_eps')) else "  每股收益(扣非): -")
        result.append(f"  净资产收益率(ROE): {latest['roe']:.2f}%" if pd.notna(latest.get('roe')) else "  净资产收益率(ROE): -")
        result.append(f"  总资产报酬率(ROA): {latest['roa']:.2f}%" if pd.notna(latest.get('roa')) else "  总资产报酬率(ROA): -")
        result.append(f"  销售毛利率: {latest['grossprofit_margin']:.2f}%" if pd.notna(latest.get('grossprofit_margin')) else "  销售毛利率: -")
        result.append(f"  销售净利率: {latest['netprofit_margin']:.2f}%" if pd.notna(latest.get('netprofit_margin')) else "  销售净利率: -")
        
        result.append("")
        result.append("运营能力指标：")
        result.append(f"  总资产周转率: {latest['assets_turn']:.4f}" if pd.notna(latest.get('assets_turn')) else "  总资产周转率: -")
        result.append(f"  流动资产周转率: {latest['ca_turn']:.4f}" if pd.notna(latest.get('ca_turn')) else "  流动资产周转率: -")
        result.append(f"  固定资产周转率: {latest['fa_turn']:.4f}" if pd.notna(latest.get('fa_turn')) else "  固定资产周转率: -")
        result.append(f"  存货周转率: {latest['inv_turn']:.4f}" if pd.notna(latest.get('inv_turn')) else "  存货周转率: -")
        result.append(f"  应收账款周转率: {latest['ar_turn']:.4f}" if pd.notna(latest.get('ar_turn')) else "  应收账款周转率: -")
        
        result.append("")
        result.append("偿债能力指标：")
        result.append(f"  资产负债率: {latest['debt_to_assets']:.2f}%" if pd.notna(latest.get('debt_to_assets')) else "  资产负债率: -")
        result.append(f"  流动比率: {latest['current_ratio']:.4f}" if pd.notna(latest.get('current_ratio')) else "  流动比率: -")
        result.append(f"  速动比率: {latest['quick_ratio']:.4f}" if pd.notna(latest.get('quick_ratio')) else "  速动比率: -")
        result.append(f"  现金比率: {latest['cash_ratio']:.4f}" if pd.notna(latest.get('cash_ratio')) else "  现金比率: -")
        
        result.append("")
        result.append("成长能力指标：")
        result.append(f"  营业收入同比增长率: {latest['or_yoy']:.2f}%" if pd.notna(latest.get('or_yoy')) else "  营业收入同比增长率: -")
        result.append(f"  净利润同比增长率: {latest['q_profit_yoy']:.2f}%" if pd.notna(latest.get('q_profit_yoy')) else "  净利润同比增长率: -")
        result.append(f"  净资产同比增长率: {latest['equity_yoy']:.2f}%" if pd.notna(latest.get('equity_yoy')) else "  净资产同比增长率: -")
        
        result.append("")
        result.append("每股指标：")
        result.append(f"  每股净资产(BPS): {latest['bps']:.4f}" if pd.notna(latest.get('bps')) else "  每股净资产(BPS): -")
        result.append(f"  每股经营活动产生的现金流量净额: {latest['ocfps']:.4f}" if pd.notna(latest.get('ocfps')) else "  每股经营活动产生的现金流量净额: -")
        result.append(f"  每股现金流量净额: {latest['cfps']:.4f}" if pd.notna(latest.get('cfps')) else "  每股现金流量净额: -")
    
    return "\n".join(result)


