"""股票筛选相关MCP工具"""
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from config.token_manager import get_tushare_token
from newfunction.strategy_context import StrategyContext
from newfunction.trading_strategy import run_funnel_strategy
from newfunction.batch_data_collector import BatchDataCollector
from cache.concept_cache_manager import concept_cache_manager
import sqlite3
import tushare as ts
from config.settings import CACHE_DB


def register_stock_filter_tools(mcp: "FastMCP"):
    """注册股票筛选相关工具"""
    
    @mcp.tool()
    def collect_stock_data(
        trade_date: str = "",
        auto_detect: bool = True
    ) -> str:
        """
        收集股票数据到 t1_data 表
        
        参数:
            trade_date: 目标交易日（T日，格式：YYYYMMDD，如：20251219）
                       如果为空且 auto_detect=True，则自动使用最近交易日
            auto_detect: 是否自动检测最近交易日（默认True）
        
        说明:
            - 收集的是 T-1 日的数据作为基准数据
            - 数据包括：每日指标、日线行情、筹码数据、融资融券、龙虎榜等
            - 数据会保存到 t1_data 表中，供后续筛选使用
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            collector = BatchDataCollector()
            pro = ts.pro_api(token)
            
            # 确定目标日期
            if not trade_date and auto_detect:
                # 获取最近交易日
                df_cal = pro.trade_cal(
                    exchange='', 
                    is_open='1',
                    start_date=time.strftime('%Y%m%d', time.localtime(time.time() - 30 * 86400)),
                    end_date=time.strftime('%Y%m%d')
                )
                if not df_cal.empty:
                    trade_date = df_cal['cal_date'].iloc[-1]
                else:
                    return "无法自动检测最近交易日，请手动指定 trade_date"
            
            if not trade_date:
                return "请指定 trade_date 参数或设置 auto_detect=True"
            
            # 验证日期格式
            if len(trade_date) != 8 or not trade_date.isdigit():
                return f"日期格式错误，应为 YYYYMMDD 格式，如：20251219"
            
            # 计算 T-1 日期（需要基准数据的前一个交易日）
            df_cal = pro.trade_cal(
                exchange='',
                is_open='1',
                start_date=time.strftime('%Y%m%d', time.localtime(time.mktime(time.strptime(trade_date, '%Y%m%d')) - 60 * 86400)),
                end_date=trade_date
            )
            if df_cal.empty:
                return f"无法获取 {trade_date} 的交易日历"
            
            # 获取 T-1 日期（前一个交易日）
            prev_dates = df_cal[df_cal['cal_date'] < trade_date]['cal_date'].tolist()
            if not prev_dates:
                return f"无法找到 {trade_date} 的前一交易日"
            
            t1_date = prev_dates[-1]
            
            # 检查数据是否已存在
            conn = sqlite3.connect(CACHE_DB)
            try:
                query = f"SELECT COUNT(*) FROM t1_data WHERE trade_date = '{t1_date}'"
                count = pd.read_sql(query, conn).iloc[0, 0]
                if count > 0:
                    conn.close()
                    return f"T-1日({t1_date})数据已存在，共 {count} 条记录。如需重新采集，请先删除旧数据。"
            finally:
                conn.close()
            
            # 开始采集
            result_parts = [f"开始采集 T-1日({t1_date}) 数据（对应 T日={trade_date}）..."]
            collector.collect_for_date(t1_date)
            
            # 获取采集结果统计
            conn = sqlite3.connect(CACHE_DB)
            try:
                query = f"SELECT COUNT(*) FROM t1_data WHERE trade_date = '{t1_date}'"
                final_count = pd.read_sql(query, conn).iloc[0, 0]
                result_parts.append(f"\n采集完成！共收集 {final_count} 条记录")
                
                # 统计数据完整性
                query_stats = f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN total_mv > 0 THEN 1 ELSE 0 END) as has_basic,
                        SUM(CASE WHEN winner_rate > 0 THEN 1 ELSE 0 END) as has_chip,
                        SUM(CASE WHEN sum_inst_net != 0 THEN 1 ELSE 0 END) as has_inst,
                        SUM(CASE WHEN margin_cap_ratio > 0 THEN 1 ELSE 0 END) as has_margin
                    FROM t1_data 
                    WHERE trade_date = '{t1_date}'
                """
                stats = pd.read_sql(query_stats, conn).iloc[0]
                result_parts.append(f"\n数据完整性统计：")
                result_parts.append(f"  基础数据（市值等）: {stats['has_basic']}/{stats['total']} ({stats['has_basic']/stats['total']*100:.1f}%)")
                result_parts.append(f"  筹码数据: {stats['has_chip']}/{stats['total']} ({stats['has_chip']/stats['total']*100:.1f}%)")
                result_parts.append(f"  机构数据: {stats['has_inst']}/{stats['total']} ({stats['has_inst']/stats['total']*100:.1f}%)")
                result_parts.append(f"  融资融券数据: {stats['has_margin']}/{stats['total']} ({stats['has_margin']/stats['total']*100:.1f}%)")
            finally:
                conn.close()
            
            collector.print_final_report()
            return "\n".join(result_parts)
            
        except Exception as e:
            import traceback
            return f"数据采集失败：{str(e)}\n{traceback.format_exc()}"
    
    @mcp.tool()
    def filter_stocks(
        trade_date: str = "",
        regime: int = 1,
        top_n: int = 9,
        auto_detect: bool = True,
        sector_code: str = ""
    ) -> str:
        """
        股票筛选工具 - 基于策略上下文的多模式筛选
        
        参数:
            trade_date: 目标交易日（T日，格式：YYYYMMDD，如：20251219）
                       如果为空且 auto_detect=True，则自动使用最近交易日
            regime: 市场风格模式（默认1）
                    - 0: 熊市模式（极度保守，涨幅0%~2.5%）
                    - 1: 震荡市模式（防御潜伏，涨幅0.5%~4.2%，默认）
                    - 2: 牛市模式（进攻型，涨幅3%~8.5%）
            top_n: 输出前N只股票（默认9）
            auto_detect: 是否自动检测最近交易日（默认True）
            sector_code: 板块代码（可选，支持多个板块代码，逗号分隔，如：BK0933.DC,BK0934.DC）
                        如果指定，则只筛选属于该板块的股票
                        留空则筛选全市场股票
        
        说明:
            - 需要先使用 collect_stock_data 收集 T-1 日数据
            - 如果指定了板块代码，需要先使用 collect_industry_sectors 采集板块数据
            - 筛选逻辑包括：涨幅区间、VWAP偏离度、换手率、筹码分级等
            - 根据市场风格自动调整筛选参数
            - 返回筛选结果和关键指标
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 验证 regime 参数
            if regime not in [0, 1, 2]:
                return f"regime 参数错误，应为 0（熊市）、1（震荡市）或 2（牛市）"
            
            # 确定目标日期
            pro = ts.pro_api(token)
            if not trade_date and auto_detect:
                df_cal = pro.trade_cal(
                    exchange='',
                    is_open='1',
                    start_date=time.strftime('%Y%m%d', time.localtime(time.time() - 30 * 86400)),
                    end_date=time.strftime('%Y%m%d')
                )
                if not df_cal.empty:
                    trade_date = df_cal['cal_date'].iloc[-1]
                else:
                    return "无法自动检测最近交易日，请手动指定 trade_date"
            
            if not trade_date:
                return "请指定 trade_date 参数或设置 auto_detect=True"
            
            # 验证日期格式
            if len(trade_date) != 8 or not trade_date.isdigit():
                return f"日期格式错误，应为 YYYYMMDD 格式，如：20251219"
            
            # 计算 T-1 日期
            df_cal = pro.trade_cal(
                exchange='',
                is_open='1',
                start_date=time.strftime('%Y%m%d', time.localtime(time.mktime(time.strptime(trade_date, '%Y%m%d')) - 60 * 86400)),
                end_date=trade_date
            )
            if df_cal.empty:
                return f"无法获取 {trade_date} 的交易日历"
            
            prev_dates = df_cal[df_cal['cal_date'] < trade_date]['cal_date'].tolist()
            if not prev_dates:
                return f"无法找到 {trade_date} 的前一交易日"
            
            t1_date = prev_dates[-1]
            
            # 检查 T-1 数据是否存在
            conn = sqlite3.connect(CACHE_DB)
            try:
                query = f"SELECT COUNT(*) FROM t1_data WHERE trade_date = '{t1_date}'"
                count = pd.read_sql(query, conn).iloc[0, 0]
                if count == 0:
                    conn.close()
                    return f"T-1日({t1_date})数据不存在，请先使用 collect_stock_data 工具收集数据"
            finally:
                conn.close()
            
            # 创建策略上下文
            ctx = StrategyContext(regime=regime)
            
            # 运行筛选
            result_parts = [
                f"{'='*60}",
                f"股票筛选报告 | T日: {trade_date} | T-1日: {t1_date}",
                f"市场风格: {ctx.regime_name}",
                f"{'='*60}",
                ctx.describe(),
                ""
            ]
            
            # 调用筛选函数（返回 DataFrame）
            df_result = run_funnel_strategy(
                target_date=trade_date,
                t1_date=t1_date,
                context=ctx,
                return_df=True
            )
            
            if df_result is None or df_result.empty:
                result_parts.append("筛选结果：无股票通过筛选条件")
                return "\n".join(result_parts)
            
            # 如果指定了板块代码，进行板块过滤
            if sector_code:
                sector_codes = [code.strip() for code in sector_code.split(',') if code.strip()]
                if sector_codes:
                    # 获取所有指定板块的成分股
                    all_stock_codes = set()
                    sector_names = []
                    
                    for sc in sector_codes:
                        # 查询该板块的成分股
                        df_members = concept_cache_manager.get_concept_member_data(
                            ts_code=sc,
                            trade_date=t1_date  # 使用 T-1 日期查询板块成分
                        )
                        
                        if df_members is not None and not df_members.empty:
                            stock_codes = set(df_members['con_code'].unique())
                            all_stock_codes.update(stock_codes)
                            
                            # 获取板块名称
                            df_sector_info = concept_cache_manager.get_concept_index_data(
                                ts_code=sc,
                                trade_date=t1_date,
                                idx_type='行业板块'
                            )
                            if df_sector_info is not None and not df_sector_info.empty:
                                sector_name = df_sector_info.iloc[0].get('name', sc)
                            else:
                                # 尝试从 daily_data 获取
                                df_daily = concept_cache_manager.get_concept_daily_data(
                                    ts_code=sc,
                                    trade_date=t1_date,
                                    idx_type='行业板块'
                                )
                                if df_daily is not None and not df_daily.empty:
                                    sector_name = sc  # 如果找不到名称，使用代码
                                else:
                                    sector_name = sc
                            sector_names.append(f"{sc}({sector_name})")
                        else:
                            # 如果查询不到，尝试使用 T 日数据
                            df_members = concept_cache_manager.get_concept_member_data(
                                ts_code=sc,
                                trade_date=trade_date
                            )
                            if df_members is not None and not df_members.empty:
                                stock_codes = set(df_members['con_code'].unique())
                                all_stock_codes.update(stock_codes)
                                
                                # 获取板块名称（尝试 T 日数据）
                                df_sector_info = concept_cache_manager.get_concept_index_data(
                                    ts_code=sc,
                                    trade_date=trade_date,
                                    idx_type='行业板块'
                                )
                                if df_sector_info is not None and not df_sector_info.empty:
                                    sector_name = df_sector_info.iloc[0].get('name', sc)
                                else:
                                    sector_name = sc
                                sector_names.append(f"{sc}({sector_name})")
                    
                    if all_stock_codes:
                        # 过滤出属于指定板块的股票
                        before_count = len(df_result)
                        df_result = df_result[df_result['ts_code'].isin(all_stock_codes)].copy()
                        after_count = len(df_result)
                        
                        result_parts.append(f"\n板块过滤：")
                        result_parts.append(f"  指定板块: {', '.join(sector_names)}")
                        result_parts.append(f"  板块成分股数: {len(all_stock_codes)}")
                        result_parts.append(f"  筛选前: {before_count} 只，筛选后: {after_count} 只")
                        
                        if df_result.empty:
                            result_parts.append("\n筛选结果：指定板块中无股票通过筛选条件")
                            return "\n".join(result_parts)
                    else:
                        result_parts.append(f"\n警告：未找到板块 {sector_code} 的成分股数据，请先使用 collect_industry_sectors 采集板块数据")
                        result_parts.append("将返回全市场筛选结果")
            
            # 限制输出数量
            df_output = df_result.head(top_n)
            
            # 格式化输出
            result_parts.append(f"筛选结果：共 {len(df_result)} 只候选股，显示前 {len(df_output)} 只")
            result_parts.append("")
            result_parts.append(f"{'股票代码':<12} | {'涨幅%':<8} | {'换手率%':<10} | {'VWAP偏离':<10} | {'大单异动':<10} | {'胜率%':<8} | {'筹码Tier':<10} | {'综合得分':<10}")
            result_parts.append("-" * 100)
            
            for _, row in df_output.iterrows():
                ts_code = row.get('ts_code', 'N/A')
                pct_chg = row.get('pct_chg', 0)
                turnover = row.get('turnover_rate', row.get('turnover', 0))
                bias = row.get('bias', 0)
                ats_ratio = row.get('ats_ratio', 0)
                winner_rate = row.get('winner_rate', 0)
                chip_tier = row.get('Chip_Tier', 0)
                final_score = row.get('Final_Score', 0)
                
                result_parts.append(
                    f"{ts_code:<12} | {pct_chg:>7.2f} | {turnover:>9.2f} | {bias:>9.3f} | {ats_ratio:>9.2f} | {winner_rate:>7.1f} | {chip_tier:>10} | {final_score:>9.3f}"
                )
            
            result_parts.append("")
            result_parts.append(f"{'='*60}")
            result_parts.append("筛选说明：")
            
            if sector_code:
                result_parts.append(f"- 板块过滤: 仅筛选属于指定板块的股票")
            
            if regime == 1:  # 震荡市
                result_parts.append("- 拒绝追高: 涨幅限制在 0.5%~4.2%，吃鱼身不吃鱼尾")
                result_parts.append("- 成本控制: VWAP偏离度 -0.2%~1.5%，允许微弱负溢价（主力打压吸筹）")
                result_parts.append("- 拒绝过热: 换手率上限 8%，高换手=高分歧=高风险")
                result_parts.append("- 主力在场: 大单异动 > 1.3，确保有主力资金流入")
            elif regime == 2:  # 牛市
                result_parts.append("- 追涨模式: 涨幅 3%~8.5%，抓住动量趋势")
                result_parts.append("- 大单异动 > 1.2，跟随主力资金")
            else:  # 熊市
                result_parts.append("- 极度保守: 涨幅 0%~2.5%，仅潜伏超跌反弹")
                result_parts.append("- 大单异动 > 1.5，要求更高")
            
            return "\n".join(result_parts)
            
        except Exception as e:
            import traceback
            return f"股票筛选失败：{str(e)}\n{traceback.format_exc()}"

