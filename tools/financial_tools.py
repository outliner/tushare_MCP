"""财务报表相关MCP工具"""
import tushare as ts
from typing import TYPE_CHECKING
from config.token_manager import get_tushare_token
from formatters.financial_formatter import format_income_statement_analysis
from cache.cache_manager import cache_manager

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
