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
            4. 外部验证 - 纳指ETF/中概互联ETF折算外盘干扰
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
            external_result = _analyze_external_validation(trade_date)
            
            # 生成综合报告
            report = _format_macro_scan_report(
                trade_date,
                volume_result,
                style_result,
                sentiment_result,
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
    
    # 模块4: 外部验证
    lines.append("【四、外部验证】")
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
