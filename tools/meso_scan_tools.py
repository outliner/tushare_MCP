"""中观全周期扫描 MCP 工具 (Meso 2.5 Analysis)

该模块提供概念板块中观分析工具，综合分析主线定位、凤凰策略和资金验伪。

分析维度:
- 模块A：主线定位 - Alpha排名 + 涨停梯队 + 周期定位
- 模块B：凤凰策略 - 超跌反弹机会
- 模块C：资金验伪 - 主力资金流向验证
"""
import tushare as ts
import pandas as pd
import json
import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional, Dict, Any, List, Tuple
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP


def register_meso_scan_tools(mcp: "FastMCP"):
    """注册中观全周期扫描工具"""
    
    @mcp.tool()
    def meso_scan(
        trade_date: str = "",
        top_n: int = 20,
        price_change_5d_max: float = -0.05,
        vol_ratio_threshold: float = 1.3,
        outflow_warning: float = 1.0,
        limit_up_threshold: int = 5
    ) -> str:
        """
        中观全周期扫描 (Meso 2.5 Analysis) - 概念板块综合分析
        
        参数:
            trade_date: 分析日期（YYYYMMDD格式，默认使用最新交易日）
            top_n: Alpha排名取前N个概念（默认20）
            price_change_5d_max: 凤凰策略：近5日跌幅阈值（默认-0.05即-5%）
            vol_ratio_threshold: 凤凰策略：今日放量阈值（默认1.3）
            outflow_warning: 资金背离：主力净流出警戒线（亿元，默认1.0）
            limit_up_threshold: 涨停家数分界阈值（默认5家）
        
        返回:
            包含三个分析模块的中观全周期扫描报告
        
        分析模块:
            A. 主线定位 - Alpha排名、排名变化、涨停梯队、周期状态
            B. 凤凰策略 - 超跌反弹机会（近5日下跌+今日放量）
            C. 资金验伪 - 主力资金流向验证，剔除背离板块
        
        说明:
            - 语义归类（如Sora/Kimi→AI应用）请在AI对话层完成
            - 本工具返回原始数据列表，便于AI进行智能归类
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 确定分析日期
            if not trade_date:
                trade_date = _get_latest_trade_date()
            
            # 执行三个分析模块
            mainline_result = _analyze_mainline_lifecycle(trade_date, top_n, limit_up_threshold)
            phoenix_result = _analyze_phoenix_rebound(trade_date, price_change_5d_max, vol_ratio_threshold)
            money_result = _analyze_money_validation(trade_date, mainline_result, phoenix_result, outflow_warning)
            
            # 生成综合报告
            report = _format_meso_scan_report(
                trade_date,
                mainline_result,
                phoenix_result,
                money_result,
                top_n
            )
            
            return report
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"中观全周期扫描失败：{str(e)}\n详细信息：{error_detail}"


def _get_latest_trade_date() -> str:
    """获取最新交易日"""
    today = datetime.now()
    # 如果是周末，回退到周五
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    return today.strftime("%Y%m%d")


def _clean_concept_name(name: str) -> str:
    """
    清洗概念板块名称，用于跨数据源匹配
    
    处理:
    1. 去掉"概念"、"板块"、"指数"后缀
    2. 去除括号及括号内容
    3. 去除空格
    """
    if not name:
        return ""
    # 去掉常见后缀
    for suffix in ["概念", "板块", "指数"]:
        name = name.replace(suffix, "")
    # 去掉括号内容
    name = re.sub(r'[（\(].*?[）\)]', '', name)
    return name.strip()


def _analyze_mainline_lifecycle(trade_date: str, top_n: int, limit_up_threshold: int) -> Dict[str, Any]:
    """
    模块A: 主线定位 (Mainline & Lifecycle)
    
    数据获取:
    1. rank_concepts_by_alpha → Top N Alpha 概念
    2. rank_concepts_alpha_velocity → 排名变化
    3. get_limit_cpt_list → 涨停梯队
    
    使用板块名称作为 Join Key 解决数据源异构问题
    """
    result = {
        "success": False,
        "concepts": [],
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        
        # 1. 获取 Top N Alpha 概念板块（东财体系）
        from tools.concept_tools import get_hot_concept_codes, rank_sectors_alpha
        
        concept_codes = get_hot_concept_codes(trade_date, limit=80)
        if not concept_codes:
            result["error"] = "无法获取概念板块列表"
            return result
        
        alpha_df = rank_sectors_alpha(concept_codes, "000300.SH", trade_date)
        if alpha_df.empty:
            result["error"] = "无法获取Alpha排名数据"
            return result
        
        # 取前 top_n 名
        alpha_df = alpha_df.head(top_n)
        
        # 获取板块名称
        try:
            concept_codes_str = ','.join(alpha_df['sector_code'].tolist())
            name_df = pro.dc_index(ts_code=concept_codes_str, trade_date=trade_date)
            name_map = {}
            if not name_df.empty:
                for _, row in name_df.iterrows():
                    name_map[row['ts_code']] = row.get('name', row['ts_code'])
            alpha_df['name'] = alpha_df['sector_code'].map(name_map).fillna(alpha_df['sector_code'])
        except Exception:
            alpha_df['name'] = alpha_df['sector_code']
        
        # 2. 获取排名变化（尝试获取历史排名对比）
        from tools.concept_tools import calculate_alpha_rank_velocity
        try:
            velocity_df = calculate_alpha_rank_velocity(concept_codes, "000300.SH", trade_date)
            if not velocity_df.empty:
                velocity_map = dict(zip(velocity_df['sector_code'], velocity_df['rank_change_1d']))
            else:
                velocity_map = {}
        except Exception:
            velocity_map = {}
        
        alpha_df['rank_change'] = alpha_df['sector_code'].map(velocity_map).fillna(0)
        
        # 3. 获取涨停梯队（Tushare体系）
        try:
            limit_df = pro.limit_cpt_list(trade_date=trade_date)
            if limit_df is not None and not limit_df.empty:
                # 清洗名称用于匹配
                limit_df['clean_name'] = limit_df['name'].apply(_clean_concept_name)
                limit_name_map = dict(zip(limit_df['clean_name'], limit_df['limit_up_num']))
            else:
                limit_name_map = {}
        except Exception:
            limit_name_map = {}
        
        # 4. 匹配涨停家数（使用清洗后的名称）
        alpha_df['clean_name'] = alpha_df['name'].apply(_clean_concept_name)
        alpha_df['limit_up_count'] = alpha_df['clean_name'].map(limit_name_map).fillna(0).astype(int)
        
        # 5. 周期定位
        concepts = []
        for _, row in alpha_df.iterrows():
            rank_change = row.get('rank_change', 0)
            limit_up_count = row.get('limit_up_count', 0)
            
            # 周期定位矩阵
            rank_up = rank_change > 0 if pd.notna(rank_change) else False
            limit_high = limit_up_count >= limit_up_threshold
            
            if rank_up and limit_high:
                status = "🔥 [高潮期]"
            elif rank_up and not limit_high:
                status = "🚀 [启动期]"
            elif not rank_up and limit_high:
                status = "⚡ [分歧滞涨]"
            else:
                status = "❄️ [退潮期]"
            
            concepts.append({
                "code": row['sector_code'],
                "name": row['name'],
                "alpha": row['score'] * 100 if pd.notna(row.get('score')) else 0,
                "rank_change": int(rank_change) if pd.notna(rank_change) else 0,
                "limit_up_count": int(limit_up_count),
                "status": status
            })
        
        result["success"] = True
        result["concepts"] = concepts
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


def _analyze_phoenix_rebound(trade_date: str, price_change_5d_max: float, vol_ratio_threshold: float) -> Dict[str, Any]:
    """
    模块B: 凤凰策略 (Phoenix Rebound)
    
    筛选"跌无可跌且资金回流"的板块
    """
    result = {
        "success": False,
        "rebounds": [],
        "error": None
    }
    
    try:
        from tools.concept_tools import scan_concept_volume_anomaly
        
        # 扫描超跌反弹机会
        scan_result = scan_concept_volume_anomaly(
            end_date=trade_date,
            vol_ratio_threshold=vol_ratio_threshold,
            price_change_5d_min=-0.30,  # 排除暴跌超30%的
            price_change_5d_max=price_change_5d_max,  # 近5日下跌
            hot_limit=100
        )
        
        if scan_result and scan_result.get('matched_count', 0) > 0:
            for match in scan_result.get('matches', []):
                result["rebounds"].append({
                    "code": match.get('concept_code', ''),
                    "name": match.get('concept_name', ''),
                    "price_change_5d": match.get('price_change_5d', 0) * 100,
                    "vol_ratio": match.get('vol_ratio', 0),
                    "turnover_rate": match.get('turnover_rate', 0)
                })
            result["success"] = True
        else:
            # 没有匹配结果，检查是否有接近的
            closest = scan_result.get('closest_results', [])
            if closest:
                for item in closest[:5]:
                    result["rebounds"].append({
                        "code": item.get('concept_code', ''),
                        "name": item.get('concept_name', ''),
                        "price_change_5d": item.get('price_change_5d', 0) * 100,
                        "vol_ratio": item.get('vol_ratio', 0),
                        "turnover_rate": item.get('turnover_rate', 0),
                        "note": "接近阈值"
                    })
            result["success"] = True
            result["note"] = "无完全符合条件的板块，展示最接近的候选"
            
    except Exception as e:
        result["error"] = str(e)
        result["success"] = True  # 即使失败也不影响其他模块
    
    return result


def _analyze_money_validation(
    trade_date: str,
    mainline_result: Dict[str, Any],
    phoenix_result: Dict[str, Any],
    outflow_warning: float
) -> Dict[str, Any]:
    """
    模块C: 资金验伪 (Money Validation)
    
    批量获取全市场资金流向，内存过滤匹配
    """
    result = {
        "success": False,
        "warnings": [],
        "golden_list": [],
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        
        # 批量获取当日全市场资金流向（不传 ts_code）
        cache_params = {'trade_date': trade_date, 'content_type': '概念'}
        moneyflow_df = cache_manager.get_dataframe('moneyflow_ind_dc', **cache_params)
        
        if moneyflow_df is None or cache_manager.is_expired('moneyflow_ind_dc', **cache_params):
            try:
                moneyflow_df = pro.moneyflow_ind_dc(trade_date=trade_date, content_type='概念')
                if moneyflow_df is not None and not moneyflow_df.empty:
                    cache_manager.set('moneyflow_ind_dc', moneyflow_df, **cache_params)
            except Exception:
                moneyflow_df = None
        
        if moneyflow_df is None or moneyflow_df.empty:
            result["success"] = True
            result["note"] = "无法获取资金流向数据"
            # 直接返回所有主线作为黄金列表
            if mainline_result.get("success"):
                result["golden_list"] = mainline_result.get("concepts", [])[:10]
            return result
        
        # 清洗资金流向数据的名称用于匹配
        moneyflow_df['clean_name'] = moneyflow_df['name'].apply(_clean_concept_name)
        
        # 创建资金流向映射 (主力净流入，单位：万元 -> 亿元)
        moneyflow_map = {}
        for _, row in moneyflow_df.iterrows():
            clean_name = row['clean_name']
            # net_mf_amount 是主力净流入金额（万元）
            net_inflow = row.get('net_mf_amount', 0)
            if pd.notna(net_inflow):
                moneyflow_map[clean_name] = net_inflow / 10000  # 转换为亿元
        
        # 验证主线板块
        warnings = []
        golden_list = []
        
        if mainline_result.get("success"):
            for concept in mainline_result.get("concepts", []):
                clean_name = _clean_concept_name(concept.get("name", ""))
                net_inflow = moneyflow_map.get(clean_name, 0)
                concept["net_inflow"] = net_inflow
                
                status = concept.get("status", "")
                # 背离检测：启动期或高潮期，但主力净流出超过阈值
                is_positive_phase = "[高潮期]" in status or "[启动期]" in status
                is_outflow = net_inflow < -outflow_warning
                
                if is_positive_phase and is_outflow:
                    concept["warning"] = f"主力净流出 {abs(net_inflow):.1f} 亿"
                    warnings.append(concept)
                else:
                    golden_list.append(concept)
        
        # 验证凤凰策略板块
        if phoenix_result.get("success"):
            for rebound in phoenix_result.get("rebounds", []):
                clean_name = _clean_concept_name(rebound.get("name", ""))
                net_inflow = moneyflow_map.get(clean_name, 0)
                rebound["net_inflow"] = net_inflow
                rebound["status"] = "⚡ [超跌反弹]"
                
                # 超跌反弹需要有资金流入
                if net_inflow > 0:
                    golden_list.append(rebound)
        
        result["success"] = True
        result["warnings"] = warnings
        result["golden_list"] = golden_list[:15]  # 最多展示15个
        
    except Exception as e:
        result["error"] = str(e)
        result["success"] = True
    
    return result


def _format_meso_scan_report(
    trade_date: str,
    mainline_result: Dict[str, Any],
    phoenix_result: Dict[str, Any],
    money_result: Dict[str, Any],
    top_n: int
) -> str:
    """格式化中观全周期扫描报告"""
    
    # 格式化日期
    formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}" if len(trade_date) == 8 else trade_date
    
    lines = []
    lines.append("📊 中观全周期扫描报告 (Meso 2.5 Analysis)")
    lines.append("=" * 60)
    lines.append(f"📅 分析日期: {formatted_date}")
    lines.append("")
    
    # 模块A: 主线定位
    lines.append("【模块A：主线定位】")
    lines.append("━" * 60)
    
    if mainline_result.get("success") and mainline_result.get("concepts"):
        lines.append(f"📋 Top {top_n} Alpha 概念板块（请 AI 进行语义归类）:")
        lines.append("")
        lines.append("| 排名 | 概念名称         | Alpha    | 排名变化 | 涨停数 | 状态          |")
        lines.append("|-----|-----------------|----------|---------|-------|---------------|")
        
        for i, concept in enumerate(mainline_result["concepts"], 1):
            name = concept.get("name", "")[:12]
            alpha = f"{concept.get('alpha', 0):+.2f}%"
            rank_change = concept.get("rank_change", 0)
            rank_str = f"{rank_change:+d}" if rank_change != 0 else "0"
            if rank_change > 0:
                rank_str += " ⬆️"
            elif rank_change < 0:
                rank_str += " ⬇️"
            limit_up = concept.get("limit_up_count", 0)
            status = concept.get("status", "")
            
            lines.append(f"| {i:<3} | {name:<15} | {alpha:<8} | {rank_str:<7} | {limit_up:<5} | {status} |")
        
        lines.append("")
        lines.append("💡 提示：建议将语义相近的概念合并分析（如 Sora+Kimi → AI应用）")
    else:
        lines.append(f"⚠️ 数据获取失败: {mainline_result.get('error', '未知错误')}")
    lines.append("")
    
    # 模块B: 凤凰策略
    lines.append("【模块B：凤凰策略】")
    lines.append("━" * 60)
    
    if phoenix_result.get("success") and phoenix_result.get("rebounds"):
        if phoenix_result.get("note"):
            lines.append(f"📌 {phoenix_result['note']}")
        else:
            lines.append("⚡【超跌反弹关注】(适合低吸)")
        lines.append("")
        lines.append("| 板块名称         | 5日跌幅  | 今日量比 | 换手率  |")
        lines.append("|-----------------|---------|---------|--------|")
        
        for rebound in phoenix_result["rebounds"][:10]:
            name = rebound.get("name", "")[:12]
            price_chg = f"{rebound.get('price_change_5d', 0):.1f}%"
            vol_ratio = f"{rebound.get('vol_ratio', 0):.2f}"
            turnover = f"{rebound.get('turnover_rate', 0):.1f}%"
            
            lines.append(f"| {name:<15} | {price_chg:<7} | {vol_ratio:<7} | {turnover:<6} |")
    else:
        lines.append("📌 当前无符合凤凰策略条件的板块")
    lines.append("")
    
    # 模块C: 资金验伪
    lines.append("【模块C：资金验伪】")
    lines.append("━" * 60)
    
    if money_result.get("success"):
        # 资金背离预警
        if money_result.get("warnings"):
            lines.append(f"⚠️ 资金背离预警 (主力净流出 > 1亿，谨慎追高):")
            lines.append("| 板块名称         | 状态          | 主力净流出  |")
            lines.append("|-----------------|--------------|------------|")
            
            for warn in money_result["warnings"][:5]:
                name = warn.get("name", "")[:12]
                status = warn.get("status", "")
                outflow = f"{abs(warn.get('net_inflow', 0)):.1f} 亿"
                lines.append(f"| {name:<15} | {status:<12} | {outflow:<10} |")
            lines.append("")
        
        # 黄金板块列表
        if money_result.get("golden_list"):
            lines.append("✅ 黄金板块列表 (资金验证通过):")
            lines.append("| 板块名称         | 状态          | Alpha    | 主力净流入  |")
            lines.append("|-----------------|--------------|----------|------------|")
            
            for golden in money_result["golden_list"][:10]:
                name = golden.get("name", "")[:12]
                status = golden.get("status", "")
                alpha = f"{golden.get('alpha', 0):+.2f}%" if golden.get('alpha') else "-"
                inflow = golden.get("net_inflow", 0)
                inflow_str = f"{inflow:+.1f} 亿" if inflow != 0 else "-"
                lines.append(f"| {name:<15} | {status:<12} | {alpha:<8} | {inflow_str:<10} |")
        else:
            lines.append("📌 无资金验证通过的板块")
    else:
        lines.append(f"⚠️ 资金验证失败: {money_result.get('error', '未知错误')}")
    
    lines.append("")
    lines.append("=" * 60)
    lines.append("📝 使用说明：")
    lines.append("  • [高潮期]: 排名上升 + 涨停多，考虑兑现")
    lines.append("  • [启动期]: 排名上升 + 涨停少，可关注")
    lines.append("  • [分歧滞涨]: 排名下降 + 涨停多，龙头涨后排跌")
    lines.append("  • [退潮期]: 排名下降 + 涨停少，回避")
    lines.append("  • 语义归类请让 AI 根据板块名称智能合并")
    
    return "\n".join(lines)
