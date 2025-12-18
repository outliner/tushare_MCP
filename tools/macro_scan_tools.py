"""宏观全景扫描 MCP 工具

该模块提供宏观市场分析工具，综合分析市场量能、风格、情绪和外部环境。

注意：本工具仅适用于 15:30 收盘后执行，盘中执行可能因数据未入库导致偏差。
"""
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional, Dict, Any, Tuple
from config.token_manager import get_tushare_token
from cache.index_daily_cache_manager import index_daily_cache_manager
from cache.cache_manager import cache_manager

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP


def register_macro_scan_tools(mcp: "FastMCP"):
    """注册宏观全景扫描工具"""
    
    @mcp.tool()
    def macro_scan(
        trade_date: str = "",
        seal_rate_warning: float = 60.0,
        limit_down_warning: int = 20
    ) -> str:
        """
        宏观全景扫描 - 综合分析市场宏观态势
        
        参数:
            trade_date: 分析日期（YYYYMMDD格式，如：20241209，默认使用最新交易日）
            seal_rate_warning: 封板率预警阈值（%），低于此值触发预警，默认60%
            limit_down_warning: 跌停家数预警阈值，超过此值触发预警，默认20家
        
        返回:
            包含四个维度分析的宏观全景扫描报告
        
        注意:
            - 本工具仅适用于 15:30 收盘后执行
            - 盘中执行可能因数据未入库导致偏差
        
        分析维度:
            1. 市场量能判定 - 上证+深证全口径成交额对比
            2. 风格与赚钱效应 - 沪深300/国证2000/科创50 大小盘剪刀差
            3. 情绪极值探测 - 封板率、跌停家数、冰点期判定
            4. 龙虎榜机构态度 - 机构专用席位买入净额占比
            5. 外部验证 - 纳指ETF/中概互联ETF折算外盘干扰
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 确定分析日期
            if not trade_date:
                trade_date = _get_latest_trade_date()
            
            # 执行四个分析模块
            volume_result = _analyze_market_volume(trade_date)
            style_result = _analyze_style_and_profit_effect(trade_date)
            sentiment_result = _analyze_sentiment_extremes(trade_date, seal_rate_warning, limit_down_warning, style_result)
            inst_result = _analyze_institutional_sentiment(trade_date, volume_result.get("today_amount", 0))
            external_result = _analyze_external_validation(trade_date)
            
            # 生成综合报告
            report = _format_macro_scan_report(
                trade_date,
                volume_result,
                style_result,
                sentiment_result,
                inst_result,
                external_result
            )
            
            return report
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"宏观全景扫描失败：{str(e)}\n详细信息：{error_detail}"


def _get_latest_trade_date() -> str:
    """获取最新交易日（简单实现，使用当天或最近工作日）"""
    today = datetime.now()
    # 如果是周末，回退到周五
    while today.weekday() >= 5:  # 5=周六, 6=周日
        today -= timedelta(days=1)
    return today.strftime("%Y%m%d")


def _get_previous_trading_date(trade_date: str) -> Optional[str]:
    """
    获取前一个交易日
    
    参数:
        trade_date: 当前交易日期（YYYYMMDD格式）
    
    返回:
        前一个交易日期（YYYYMMDD格式），如果无法获取则返回None
    """
    try:
        pro = ts.pro_api()
        
        # 使用交易日历接口获取前一个交易日
        # 获取最近10个交易日，确保能找到前一个交易日
        end_date_obj = datetime.strptime(trade_date, '%Y%m%d')
        start_date_obj = end_date_obj - timedelta(days=10)
        start_date = start_date_obj.strftime('%Y%m%d')
        
        # 获取交易日历
        cal_df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=trade_date, is_open=1)
        
        if cal_df is not None and not cal_df.empty:
            # 筛选出交易日，按日期排序（最新的在前）
            cal_df = cal_df.sort_values('cal_date', ascending=False)
            # 确保cal_date列是整数类型
            if cal_df['cal_date'].dtype != 'int64':
                cal_df['cal_date'] = pd.to_numeric(cal_df['cal_date'], errors='coerce')
            end_date_int = int(trade_date) if isinstance(trade_date, str) else trade_date
            cal_df = cal_df[cal_df['cal_date'] <= end_date_int]
            
            # 转换为字符串并去重
            trading_dates = cal_df['cal_date'].astype(str).unique().tolist()
            trading_dates = list(dict.fromkeys(trading_dates))  # 保持顺序的去重
            
            if len(trading_dates) >= 2:
                # 返回前一个交易日（第二个）
                return trading_dates[1]
            elif len(trading_dates) == 1:
                # 只有一个交易日，说明可能是第一个交易日，无法获取前一个
                return None
        return None
    except Exception as e:
        return None


def _analyze_market_volume(trade_date: str) -> Dict[str, Any]:
    """
    模块1: 市场量能判定 (全口径)
    
    工具: 上证指数(000001.SH) + 深证成指(399001.SZ)
    计算: 今日总成交额 = 上证amount + 深证amount
    对比: (今日总额 / 昨日总额) - 1
    """
    result = {
        "success": False,
        "today_amount": 0,
        "yesterday_amount": 0,
        "sh_amount": 0,
        "sz_amount": 0,
        "change_pct": 0,
        "diagnosis": "",
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        
        # 使用交易日历获取前一个交易日
        yesterday_date = _get_previous_trading_date(trade_date)
        if not yesterday_date:
            result["error"] = f"无法获取 {trade_date} 的前一个交易日"
            return result
        
        # 获取上证指数今日数据
        sh_today_df = index_daily_cache_manager.get_index_daily_data(
            ts_code="000001.SH",
            trade_date=trade_date
        )
        if sh_today_df is None or sh_today_df.empty:
            sh_today_df = pro.index_daily(ts_code="000001.SH", trade_date=trade_date)
            if not sh_today_df.empty:
                index_daily_cache_manager.save_index_daily_data(sh_today_df)
        
        # 获取上证指数昨日数据
        sh_yesterday_df = index_daily_cache_manager.get_index_daily_data(
            ts_code="000001.SH",
            trade_date=yesterday_date
        )
        if sh_yesterday_df is None or sh_yesterday_df.empty:
            sh_yesterday_df = pro.index_daily(ts_code="000001.SH", trade_date=yesterday_date)
            if not sh_yesterday_df.empty:
                index_daily_cache_manager.save_index_daily_data(sh_yesterday_df)
        
        # 获取深证成指今日数据
        sz_today_df = index_daily_cache_manager.get_index_daily_data(
            ts_code="399001.SZ",
            trade_date=trade_date
        )
        if sz_today_df is None or sz_today_df.empty:
            sz_today_df = pro.index_daily(ts_code="399001.SZ", trade_date=trade_date)
            if not sz_today_df.empty:
                index_daily_cache_manager.save_index_daily_data(sz_today_df)
        
        # 获取深证成指昨日数据
        sz_yesterday_df = index_daily_cache_manager.get_index_daily_data(
            ts_code="399001.SZ",
            trade_date=yesterday_date
        )
        if sz_yesterday_df is None or sz_yesterday_df.empty:
            sz_yesterday_df = pro.index_daily(ts_code="399001.SZ", trade_date=yesterday_date)
            if not sz_yesterday_df.empty:
                index_daily_cache_manager.save_index_daily_data(sz_yesterday_df)
        
        # 检查数据是否完整
        if (sh_today_df is None or sh_today_df.empty or 
            sh_yesterday_df is None or sh_yesterday_df.empty or
            sz_today_df is None or sz_today_df.empty or
            sz_yesterday_df is None or sz_yesterday_df.empty):
            result["error"] = "无法获取完整的指数数据"
            return result
        
        # 计算今日和昨日成交额 (amount 单位为千元)
        sh_today = float(sh_today_df.iloc[0]['amount']) if pd.notna(sh_today_df.iloc[0]['amount']) else 0
        sh_yesterday = float(sh_yesterday_df.iloc[0]['amount']) if pd.notna(sh_yesterday_df.iloc[0]['amount']) else 0
        sz_today = float(sz_today_df.iloc[0]['amount']) if pd.notna(sz_today_df.iloc[0]['amount']) else 0
        sz_yesterday = float(sz_yesterday_df.iloc[0]['amount']) if pd.notna(sz_yesterday_df.iloc[0]['amount']) else 0
        
        today_total = sh_today + sz_today
        yesterday_total = sh_yesterday + sz_yesterday
        
        # 计算变化率
        if yesterday_total > 0:
            change_pct = (today_total / yesterday_total - 1) * 100
        else:
            change_pct = 0
        
        # 获取日期信息
        sh_today_date = str(sh_today_df.iloc[0]['trade_date']) if pd.notna(sh_today_df.iloc[0]['trade_date']) else trade_date
        sh_yesterday_date = str(sh_yesterday_df.iloc[0]['trade_date']) if pd.notna(sh_yesterday_df.iloc[0]['trade_date']) else yesterday_date
        
        # 转换为亿元
        sh_amount_yuan = sh_today / 10000  # 转换为亿元
        sz_amount_yuan = sz_today / 10000
        today_amount_yuan = today_total / 10000
        yesterday_amount_yuan = yesterday_total / 10000
        
        # 设置数据字段（无论验证是否通过都先设置，确保能显示数据）
        result["sh_amount"] = sh_amount_yuan
        result["sz_amount"] = sz_amount_yuan
        result["today_amount"] = today_amount_yuan
        result["yesterday_amount"] = yesterday_amount_yuan
        result["change_pct"] = change_pct
        result["today_date"] = sh_today_date
        result["yesterday_date"] = sh_yesterday_date
        
        # 数据验证：仅作为警告，不影响数据展示
        # 移除严格的范围限制，因为实际市场成交额可能超出预期范围
        result["success"] = True
        
        # 诊断
        if change_pct > 10:
            result["diagnosis"] = "📈 显著放量"
        elif change_pct > 0:
            result["diagnosis"] = "📈 温和放量"
        elif change_pct > -10:
            result["diagnosis"] = "📉 温和缩量"
        else:
            result["diagnosis"] = "📉 显著缩量"
            
    except Exception as e:
        result["error"] = str(e)
    
    return result


def _analyze_style_and_profit_effect(trade_date: str) -> Dict[str, Any]:
    """
    模块2: 风格与赚钱效应 (大小盘剪刀差)
    
    工具: 沪深300(000300.SH), 国证2000(399303.SZ), 科创50(000688.SH)
    逻辑:
        300↑ + 2000↑ = 全面做多
        300↑ + 2000↓ = 只赚指数（谨慎）
        300↓ + 2000↑ = 题材活跃（轻指数重个股）
        300↓ + 2000↓ = 全面退潮
    """
    result = {
        "success": False,
        "hs300": {"pct_chg": 0, "close": 0},
        "gz2000": {"pct_chg": 0, "close": 0},
        "kc50": {"pct_chg": 0, "close": 0},
        "diagnosis": "",
        "diagnosis_detail": "",
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        index_codes = ["000300.SH", "399303.SZ", "000688.SH"]
        index_data = {}
        
        for code in index_codes:
            df = index_daily_cache_manager.get_index_daily_data(
                ts_code=code,
                trade_date=trade_date
            )
            
            if df is None or df.empty:
                df = pro.index_daily(ts_code=code, trade_date=trade_date)
                if not df.empty:
                    index_daily_cache_manager.save_index_daily_data(df)
            
            if df is not None and not df.empty:
                index_data[code] = {
                    "pct_chg": float(df.iloc[0]['pct_chg']) if pd.notna(df.iloc[0]['pct_chg']) else 0,
                    "close": float(df.iloc[0]['close']) if pd.notna(df.iloc[0]['close']) else 0
                }
            else:
                index_data[code] = {"pct_chg": 0, "close": 0}
        
        result["success"] = True
        result["hs300"] = index_data.get("000300.SH", {"pct_chg": 0, "close": 0})
        result["gz2000"] = index_data.get("399303.SZ", {"pct_chg": 0, "close": 0})
        result["kc50"] = index_data.get("000688.SH", {"pct_chg": 0, "close": 0})
        
        hs300_up = result["hs300"]["pct_chg"] > 0
        gz2000_up = result["gz2000"]["pct_chg"] > 0
        
        # 诊断逻辑
        if hs300_up and gz2000_up:
            result["diagnosis"] = "🟢 全面做多"
            result["diagnosis_detail"] = "大小盘共振上涨"
        elif hs300_up and not gz2000_up:
            result["diagnosis"] = "🟡 只赚指数"
            result["diagnosis_detail"] = "权重护盘，题材退潮，谨慎操作"
        elif not hs300_up and gz2000_up:
            result["diagnosis"] = "🔵 题材活跃"
            result["diagnosis_detail"] = "轻指数重个股，小盘股活跃"
        else:
            result["diagnosis"] = "🔴 全面退潮"
            result["diagnosis_detail"] = "大小盘共振下跌"
            
    except Exception as e:
        result["error"] = str(e)
    
    return result


def _analyze_sentiment_extremes(
    trade_date: str,
    seal_rate_warning: float,
    limit_down_warning: int,
    style_result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    模块3: 情绪极值探测
    
    工具: get_limit_list(limit_type='U'/'Z'/'D')
    计算: 封板率 = 涨停数 / (涨停数 + 炸板数)
    冰点期判定: 跌停家数 > 20 且 国证2000 下跌
    """
    result = {
        "success": False,
        "limit_up_count": 0,
        "limit_failed_count": 0,
        "limit_down_count": 0,
        "seal_rate": 0,
        "is_ice_period": False,
        "diagnosis": "",
        "warning": None,
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        
        # 获取涨停数据
        up_df = pro.limit_list_d(trade_date=trade_date, limit_type='U')
        limit_up_count = len(up_df) if up_df is not None else 0
        
        # 获取炸板数据
        failed_df = pro.limit_list_d(trade_date=trade_date, limit_type='Z')
        limit_failed_count = len(failed_df) if failed_df is not None else 0
        
        # 获取跌停数据
        down_df = pro.limit_list_d(trade_date=trade_date, limit_type='D')
        limit_down_count = len(down_df) if down_df is not None else 0
        
        # 计算封板率
        if limit_up_count + limit_failed_count > 0:
            seal_rate = limit_up_count / (limit_up_count + limit_failed_count) * 100
        else:
            seal_rate = 0
        
        result["success"] = True
        result["limit_up_count"] = limit_up_count
        result["limit_failed_count"] = limit_failed_count
        result["limit_down_count"] = limit_down_count
        result["seal_rate"] = seal_rate
        
        # 冰点期判定：跌停家数 > 阈值 且 国证2000 下跌
        gz2000_down = style_result.get("gz2000", {}).get("pct_chg", 0) < 0
        if limit_down_count > limit_down_warning and gz2000_down:
            result["is_ice_period"] = True
            result["warning"] = "🔴【冰点期】跌停家数过多且小盘股下跌"
        
        # 诊断
        if seal_rate < seal_rate_warning:
            result["diagnosis"] = "⚠️ 封板率偏低"
            if result["warning"] is None:
                result["warning"] = f"封板率低于 {seal_rate_warning}%"
        elif seal_rate >= 80:
            result["diagnosis"] = "🟢 市场情绪活跃"
        else:
            result["diagnosis"] = "🟡 市场情绪一般"
            
    except Exception as e:
        result["error"] = str(e)
    
    return result


def _analyze_institutional_sentiment(trade_date: str, market_total_amount_billion: float = 0) -> Dict[str, Any]:
    """
    模块5: 龙虎榜机构态度 (新版重构)
    
    工具: top_list(每日明细), top_inst(机构明细)
    分析维度:
        1. net_buy (机构净买入): 宏观基本面资金的“投票权”
        2. reason (上榜理由): 识别宏观波动的诱因
        3. amount_rate (成交占比): 衡量热点对全市场的吸金效应 (龙虎榜总成交额 / 全市场总成交额)
        4. exalter (席位名称): 识别“国家队”或“知名顶级机构”动作
    """
    result = {
        "success": False,
        "net_buy": {
            "value": 0,
            "status": "平稳"
        },
        "reason": {
            "top_reasons": [],
            "diagnosis": ""
        },
        "amount_rate": {
            "top_list_amount": 0,
            "market_total_amount": 0,
            "ratio": 0,
            "diagnosis": ""
        },
        "exalter": {
            "key_institutions": [],  # 知名席位动作
            "diagnosis": ""
        },
        "diagnosis": "",
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        
        # 1. 获取数据
        top_list_df = pro.top_list(trade_date=trade_date)
        top_inst_df = pro.top_inst(trade_date=trade_date)
        
        if top_list_df is None or top_list_df.empty:
            result["diagnosis"] = "⚪ 今日无龙虎榜数据"
            result["success"] = True
            return result
            
        # ---------------------------------------------------------------------
        # 分析维度1: net_buy (机构净买入)
        # ---------------------------------------------------------------------
        inst_net_buy = 0
        inst_buy_rate = 0
        
        if top_inst_df is not None and not top_inst_df.empty:
            inst_only = top_inst_df[top_inst_df['exalter'].str.contains('机构专用', na=False)]
            if not inst_only.empty:
                buy_sum = inst_only['buy'].sum()
                sell_sum = inst_only['sell'].sum()
                inst_net_buy = (buy_sum - sell_sum) / 10000  # 万元
                if 'buy_rate' in inst_only.columns:
                    inst_buy_rate = inst_only['buy_rate'].mean()
        
        # 判定状态
        if inst_net_buy > 10000: # 1亿以上
            if inst_buy_rate > 15:
                net_buy_status = "🟢 机构极端共识 (宏观主线确认)"
            else:
                net_buy_status = "🟢 机构配置流入 (基石力量)"
        elif inst_net_buy > 2000: # 2000万-1亿
            net_buy_status = "🟢 机构参与活跃"
        elif inst_net_buy < -10000:
            net_buy_status = "🔴 机构协同抛售 (宏观风险释放)"
        else:
            net_buy_status = "🟡 存量博弈"
            
        result["net_buy"] = {
            "value": inst_net_buy,
            "status": net_buy_status
        }
        
        # ---------------------------------------------------------------------
        # 分析维度2: reason (上榜理由)
        # ---------------------------------------------------------------------
        if 'reason' in top_list_df.columns:
            reasons = top_list_df['reason'].dropna().astype(str).tolist()
            
            # 关键词映射
            categories = {
                "波段趋势 (Trend)": ["连续三个交易日"],  # 这是宏观资金最核心的战场
                "单日脉冲 (Impulse)": ["涨幅偏离", "涨幅达", "跌幅偏离", "跌幅达"],
                "博弈换手 (Churn)": ["换手率", "振幅", "异常波动"],
                "其它": []
            }
            
            # 统计
            cat_stats = {k: 0 for k in categories.keys()}
            
            for r in reasons:
                found = False
                for cat, keywords in categories.items():
                    if any(kw in r for kw in keywords):
                        cat_stats[cat] += 1
                        found = True
                        break
                if not found:
                    cat_stats["其它"] += 1
            
            # 找出主要诱因
            sorted_cats = sorted(cat_stats.items(), key=lambda x: x[1], reverse=True)
            top_cat = sorted_cats[0][0]
            
            top_reasons_list = [f"{k}({v})" for k, v in sorted_cats if v > 0]
            
            # 诊断逻辑重构
            diagnosis = f"波动诱因: {top_cat}"
            if top_cat == "波段趋势 (Trend)":
                # 结合之前计算的 inst_net_buy
                if inst_net_buy > 0:
                    diagnosis = "宏观画像: 强趋势确认 (机构合力主推)"
                else:
                    diagnosis = "宏观画像: 情绪驱动的波段行情"
            elif top_cat == "单日脉冲 (Impulse)":
                diagnosis = "宏观画像: 突发消息刺激/情绪脉冲"
            elif top_cat == "博弈换手 (Churn)":
                diagnosis = "宏观画像: 分歧加大 (高换手博弈)"
                
            result["reason"] = {
                "top_reasons": top_reasons_list[:3],
                "diagnosis": diagnosis
            }
        
        # ---------------------------------------------------------------------
        # 分析维度3: amount_rate (成交占比)
        # ---------------------------------------------------------------------
        # 龙虎榜总成交额 (amount 字段通常是总成交额，如果没有则用 l_buy+l_sell 近似，但 amount 更准)
        # 注意去重：同一只股票可能有多条记录
        top_list_dedup = top_list_df.drop_duplicates(subset=['ts_code'])
        list_amount = top_list_dedup['amount'].sum() / 100000000  # 亿元
        
        # 全市场总成交额 (传入参数，单位亿元)
        market_amount = market_total_amount_billion
        
        ratio = 0
        amt_diagnosis = ""
        
        if market_amount > 0:
            ratio = (list_amount / market_amount) * 100
            
            if ratio > 15:
                amt_diagnosis = "🔴 极度吸金 (流动性枯竭风险)"
            elif ratio > 10:
                amt_diagnosis = "🟡 热点吸金明显"
            elif ratio < 2:
                amt_diagnosis = "🔵 游资活跃度低"
            else:
                amt_diagnosis = "🟢 流动性分布健康"
        else:
            amt_diagnosis = "⚠️ 无法计算 (缺全市场数据)"
            
        result["amount_rate"] = {
            "top_list_amount": list_amount,
            "market_total_amount": market_amount,
            "ratio": ratio,
            "diagnosis": amt_diagnosis
        }
        
        # ---------------------------------------------------------------------
        # 分析维度4: exalter (重点席位)
        # ---------------------------------------------------------------------
        key_institutions = []
        inst_diagnosis = "常规博弈"
        
        if top_inst_df is not None and not top_inst_df.empty:
            # 定义关注名单
            # 注意：实际龙虎榜中“国家队”往往隐身或使用特定席位，这里列举常见头部/代表性席位
            watch_list = [
                "中信证券股份有限公司总部", "中金公司", "中央汇金", 
                "沪股通专用", "深股通专用", # 北向
                "国泰君安证券股份有限公司总部", "中国银河证券股份有限公司总部"
            ]
            
            # 扫描
            for inst_name in watch_list:
                # 模糊匹配
                matched = top_inst_df[top_inst_df['exalter'].str.contains(inst_name, na=False)]
                if not matched.empty:
                    # 统计动作
                    buy = matched[matched['side'] == 0]['buy'].sum()
                    sell = matched[matched['side'] == 1]['sell'].sum()
                    net = buy - sell
                    
                    if buy + sell > 0: # 有操作
                        action = "买入" if net > 0 else "卖出"
                        amt = abs(net) / 10000 # 万元
                        key_institutions.append(f"{inst_name[:4]}: 净{action} {amt:.0f}万")
            
            if len(key_institutions) > 0:
                inst_diagnosis = "🔥 顶级资金现身"
            else:
                inst_diagnosis = "常规机构博弈"

        result["exalter"] = {
            "key_institutions": key_institutions[:5], # 只取前5个
            "diagnosis": inst_diagnosis
        }
        
        result["success"] = True
        
        # 生成模块综合诊断
        diags = []
        diags.append(net_buy_status.split(' ')[0]) # 🟢/🔴
        if result["amount_rate"]["ratio"] > 10:
            diags.append("热点吸金")
        if len(key_institutions) > 0:
            diags.append("主力现身")
            
        result["diagnosis"] = " ".join(diags)
            
    except Exception as e:
        result["error"] = str(e)
        
    return result


def _analyze_external_validation(trade_date: str) -> Dict[str, Any]:
    """
    模块4: 外部验证 (ETF折算)
    
    工具: 纳指ETF(513100.SH), 中概互联ETF(513050.SH)
    逻辑: 通过场内ETF涨跌，反推外围环境对A股今日情绪的实际扰动
    """
    result = {
        "success": False,
        "nasdaq_etf": {"pct_chg": 0, "close": 0},
        "china_internet_etf": {"pct_chg": 0, "close": 0},
        "diagnosis": "",
        "error": None
    }
    
    try:
        pro = ts.pro_api()
        etf_codes = ["513100.SH", "513050.SH"]
        etf_data = {}
        
        for code in etf_codes:
            # ETF 使用 fund_daily 接口
            df = pro.fund_daily(ts_code=code, trade_date=trade_date)
            
            if df is not None and not df.empty:
                etf_data[code] = {
                    "pct_chg": float(df.iloc[0]['pct_chg']) if pd.notna(df.iloc[0].get('pct_chg')) else 0,
                    "close": float(df.iloc[0]['close']) if pd.notna(df.iloc[0]['close']) else 0
                }
            else:
                etf_data[code] = {"pct_chg": 0, "close": 0}
        
        result["success"] = True
        result["nasdaq_etf"] = etf_data.get("513100.SH", {"pct_chg": 0, "close": 0})
        result["china_internet_etf"] = etf_data.get("513050.SH", {"pct_chg": 0, "close": 0})
        
        # 诊断
        avg_pct = (result["nasdaq_etf"]["pct_chg"] + result["china_internet_etf"]["pct_chg"]) / 2
        if avg_pct > 1:
            result["diagnosis"] = "🟢 外盘环境积极"
        elif avg_pct > -1:
            result["diagnosis"] = "🟢 外盘干扰有限"
        else:
            result["diagnosis"] = "🔴 外盘拖累明显"
            
    except Exception as e:
        result["error"] = str(e)
    
    return result


def _format_macro_scan_report(
    trade_date: str,
    volume_result: Dict[str, Any],
    style_result: Dict[str, Any],
    sentiment_result: Dict[str, Any],
    inst_result: Dict[str, Any],
    external_result: Dict[str, Any]
) -> str:
    """格式化宏观全景扫描报告"""
    
    # 格式化日期
    formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}" if len(trade_date) == 8 else trade_date
    
    lines = []
    lines.append("📊 宏观全景扫描报告")
    lines.append("=" * 50)
    lines.append(f"📅 分析日期: {formatted_date}")
    lines.append("")
    
    # 模块1: 市场量能判定
    lines.append("【一、市场量能判定】")
    if volume_result["success"]:
        today_date = volume_result.get('today_date', '')
        yesterday_date = volume_result.get('yesterday_date', '')
        
        # 格式化日期显示
        if today_date and len(today_date) == 8:
            today_date_fmt = f"{today_date[:4]}-{today_date[4:6]}-{today_date[6:8]}"
        else:
            today_date_fmt = today_date
        
        if yesterday_date and len(yesterday_date) == 8:
            yesterday_date_fmt = f"{yesterday_date[:4]}-{yesterday_date[4:6]}-{yesterday_date[6:8]}"
        else:
            yesterday_date_fmt = yesterday_date
        
        lines.append(f"- 今日({today_date_fmt})总成交额: {volume_result['today_amount']:.2f} 亿元")
        lines.append(f"  • 上证: {volume_result['sh_amount']:.2f} 亿元")
        lines.append(f"  • 深证: {volume_result['sz_amount']:.2f} 亿元")
        lines.append(f"- 昨日({yesterday_date_fmt})总成交额: {volume_result['yesterday_amount']:.2f} 亿元")
        change_sign = "+" if volume_result['change_pct'] >= 0 else ""
        lines.append(f"- 变化: {volume_result['diagnosis']} {change_sign}{volume_result['change_pct']:.1f}%")
    else:
        lines.append(f"- ⚠️ 数据获取失败: {volume_result.get('error', '未知错误')}")
    lines.append("")
    
    # 模块2: 风格与赚钱效应
    lines.append("【二、风格与赚钱效应】")
    if style_result["success"]:
        lines.append("| 指数       | 涨跌幅        | 收盘点位     |")
        lines.append("|-----------|--------------|-------------|")
        
        hs300 = style_result["hs300"]
        gz2000 = style_result["gz2000"]
        kc50 = style_result["kc50"]
        
        lines.append(f"| 沪深300   | {hs300['pct_chg']:+.2f}%      | {hs300['close']:.2f}      |")
        lines.append(f"| 国证2000  | {gz2000['pct_chg']:+.2f}%      | {gz2000['close']:.2f}      |")
        lines.append(f"| 科创50    | {kc50['pct_chg']:+.2f}%      | {kc50['close']:.2f}      |")
        lines.append(f"- 诊断: {style_result['diagnosis']}（{style_result['diagnosis_detail']}）")
    else:
        lines.append(f"- ⚠️ 数据获取失败: {style_result.get('error', '未知错误')}")
    lines.append("")
    
    # 模块3: 情绪极值探测
    lines.append("【三、情绪极值探测】")
    if sentiment_result["success"]:
        lines.append(f"- 涨停家数: {sentiment_result['limit_up_count']}")
        lines.append(f"- 炸板家数: {sentiment_result['limit_failed_count']}")
        lines.append(f"- 跌停家数: {sentiment_result['limit_down_count']}")
        lines.append(f"- 封板率: {sentiment_result['seal_rate']:.1f}%")
        lines.append(f"- 诊断: {sentiment_result['diagnosis']}")
        if sentiment_result["warning"]:
            lines.append(f"- 预警: {sentiment_result['warning']}")
    else:
        lines.append(f"- ⚠️ 数据获取失败: {sentiment_result.get('error', '未知错误')}")
    lines.append("")

    # 模块4: 龙虎榜机构态度
    lines.append("【四、龙虎榜机构态度】")
    if inst_result["success"]:
        net_buy = inst_result["net_buy"]
        reason = inst_result["reason"]
        amount_rate = inst_result["amount_rate"]
        exalter = inst_result["exalter"]
        
        # 1. 机构净买入
        lines.append(f"1. 机构净买 (net_buy): {net_buy['value']:+.1f} 万元")
        lines.append(f"   • 状态: {net_buy['status']}")
        
        # 2. 上榜理由
        lines.append(f"2. 上榜理由 (reason): {reason['diagnosis']}")
        if reason['top_reasons']:
            lines.append(f"   • 分布: {', '.join(reason['top_reasons'])}")
            
        # 3. 成交占比
        lines.append(f"3. 成交占比 (amount_rate): {amount_rate['ratio']:.2f}% (龙虎榜/全市场)")
        lines.append(f"   • 诊断: {amount_rate['diagnosis']}")
        
        # 4. 重点席位
        lines.append(f"4. 重点席位 (exalter): {exalter['diagnosis']}")
        if exalter['key_institutions']:
            lines.append(f"   • 动作: {', '.join(exalter['key_institutions'])}")
        
    else:
        lines.append(f"- ⚠️ 数据获取失败: {inst_result.get('error', '未知错误')}")
    lines.append("")
    
    # 模块5: 外部验证
    lines.append("【五、外部验证】")
    if external_result["success"]:
        lines.append("| ETF           | 涨跌幅        | 收盘价       |")
        lines.append("|--------------|--------------|-------------|")
        
        nasdaq = external_result["nasdaq_etf"]
        china_internet = external_result["china_internet_etf"]
        
        lines.append(f"| 纳指ETF      | {nasdaq['pct_chg']:+.2f}%      | {nasdaq['close']:.3f}      |")
        lines.append(f"| 中概互联     | {china_internet['pct_chg']:+.2f}%      | {china_internet['close']:.3f}      |")
        lines.append(f"- 诊断: {external_result['diagnosis']}")
    else:
        lines.append(f"- ⚠️ 数据获取失败: {external_result.get('error', '未知错误')}")
    lines.append("")
    
    # 综合诊断
    lines.append("=" * 50)
    lines.append("综合诊断")
    lines.append("=" * 50)
    
    # 综合评估
    overall_score = 0
    issues = []
    
    if volume_result["success"]:
        if volume_result["change_pct"] > 0:
            overall_score += 1
        else:
            issues.append("量能萎缩")
    
    if style_result["success"]:
        if style_result["diagnosis"].startswith("🟢"):
            overall_score += 2
        elif style_result["diagnosis"].startswith("🔵"):
            overall_score += 1
        elif style_result["diagnosis"].startswith("🔴"):
            overall_score -= 1
            issues.append("全面退潮")
    
    if sentiment_result["success"]:
        if sentiment_result["is_ice_period"]:
            overall_score -= 2
            issues.append("冰点期")
        elif sentiment_result["seal_rate"] >= 70:
            overall_score += 1
        elif sentiment_result["seal_rate"] < 60:
            issues.append("封板率偏低")
    
    if inst_result["success"]:
        # 机构净买入状态
        net_buy_val = inst_result["net_buy"]["value"]
        if net_buy_val > 5000: # 5000万
            overall_score += 1
        elif net_buy_val < -5000:
            overall_score -= 1
            issues.append("机构抛压")
            
        # 虹吸效应风险
        if inst_result["amount_rate"]["ratio"] > 15:
            issues.append("热点虹吸严重")
            
        # 主力动作
        if "顶级资金" in inst_result["exalter"]["diagnosis"]:
            overall_score += 1 # 权重加分
    
    if external_result["success"]:
        avg_pct = (external_result["nasdaq_etf"]["pct_chg"] + external_result["china_internet_etf"]["pct_chg"]) / 2
        if avg_pct < -1:
            issues.append("外盘拖累")
    
    # 输出综合评估
    if overall_score >= 3:
        lines.append("🟢 市场整体【健康】")
    elif overall_score >= 1:
        lines.append("🟡 市场整体【谨慎乐观】")
    elif overall_score >= -1:
        lines.append("🟡 市场整体【震荡分化】")
    else:
        lines.append("🔴 市场整体【高风险】")
    
    if issues:
        lines.append(f"- 主要关注点: {', '.join(issues)}")
    
    return "\n".join(lines)
