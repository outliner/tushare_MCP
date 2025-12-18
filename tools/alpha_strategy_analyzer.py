"""
相对强度Alpha模型策略分析器

策略说明：
1. 计算板块和沪深300在2天和5天的区间收益率
2. 计算超额收益Alpha = 板块收益 - 基准收益
3. 综合评分：Score = α2 × 60% + α5 × 40%
"""
import sys
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from config.token_manager import get_tushare_token
from cache.index_daily_cache_manager import index_daily_cache_manager
from cache.cache_manager import cache_manager
from cache.concept_cache_manager import concept_cache_manager

# [Fix] Override print to suppress excessive stderr logging that might cause MCP crashes
def print(*args, **kwargs):
    pass


def calculate_period_return(prices: pd.Series, days: int) -> float:
    """
    计算区间收益率
    
    参数:
        prices: 价格序列（按日期排序，最新的在前）
        days: 时间窗口（天数）
    
    返回:
        收益率（小数形式，如0.05表示5%）
    """
    if len(prices) < days + 1:
        return None
    
    # 最新价格
    p_end = prices.iloc[0]
    # N天前的价格
    p_start = prices.iloc[days]
    
    if pd.isna(p_end) or pd.isna(p_start) or p_start == 0:
        return None
    
    return (p_end - p_start) / p_start

def calculate_alpha(sector_return: float, benchmark_return: float) -> float:
    """
    计算超额收益Alpha
    
    参数:
        sector_return: 板块收益率
        benchmark_return: 基准收益率
    
    返回:
        Alpha值（小数形式）
    """
    if sector_return is None or benchmark_return is None:
        return None
    return sector_return - benchmark_return

def calculate_alpha_score(alpha_2: float, alpha_5: float) -> float:
    """
    计算综合Alpha得分
    
    参数:
        alpha_2: 2天Alpha
        alpha_5: 5天Alpha
    
    返回:
        综合得分（如果5日Alpha缺失，则仅使用2日Alpha）
    """
    if alpha_2 is None:
        return None
    if alpha_5 is None:
        # 如果5日Alpha缺失，仅使用2日Alpha（权重100%）
        return alpha_2
    return alpha_2 * 0.6 + alpha_5 * 0.4

def analyze_sector_alpha(
    sector_code: str,
    benchmark_code: str = "000001.SH",
    end_date: str = None
) -> Dict:
    """
    分析单个板块的Alpha
    
    参数:
        sector_code: 板块指数代码
        benchmark_code: 基准指数代码（默认上证指数）
        end_date: 结束日期（YYYYMMDD格式，默认今天）
    
    返回:
        包含Alpha分析结果的字典
    """
    token = get_tushare_token()
    if not token:
        return {"error": "请先配置Tushare token"}
    
    if end_date is None or end_date == "":
        end_date = datetime.now().strftime('%Y%m%d')
    
    try:
        # 获取板块数据（至少需要5个交易日，预留30天以确保有足够数据）
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
        
        # 判断是申万行业指数、东财概念板块还是普通指数
        is_sw_industry = sector_code.endswith('.SI')
        is_eastmoney_concept = sector_code.endswith('.DC')
        
        if is_sw_industry:
            # 申万行业指数使用sw_daily接口
            # 自动检测行业级别：二级行业代码通常是6位数字（如801011.SI），一级行业是5位（如801010.SI）
            # 更准确的方法：尝试L1，如果失败再尝试L2
            level = 'L1'  # 默认L1
            if len(sector_code.split('.')[0]) == 6:  # 二级行业代码通常是6位
                level = 'L2'
            
            cache_params = {
                'ts_code': sector_code,
                'level': level,
                'start_date': start_date,
                'end_date': end_date
            }
            sector_df = cache_manager.get_dataframe('sw_industry_daily', **cache_params)
            
            if sector_df is None or sector_df.empty:
                # 从API获取
                pro = ts.pro_api()
                sector_df = pro.sw_daily(ts_code=sector_code, level=level, start_date=start_date, end_date=end_date)
                if not sector_df.empty:
                    cache_manager.set('sw_industry_daily', sector_df, **cache_params)
                elif level == 'L1':
                    # 如果L1失败，尝试L2
                    level = 'L2'
                    cache_params['level'] = 'L2'
                    sector_df = pro.sw_daily(ts_code=sector_code, level='L2', start_date=start_date, end_date=end_date)
                    if not sector_df.empty:
                        cache_manager.set('sw_industry_daily', sector_df, **cache_params)
            
            # 筛选指定指数的数据
            if not sector_df.empty:
                # sw_daily返回的字段可能是index_code或ts_code
                if 'ts_code' in sector_df.columns:
                    sector_df = sector_df[sector_df['ts_code'] == sector_code].copy()
                elif 'index_code' in sector_df.columns:
                    sector_df = sector_df[sector_df['index_code'] == sector_code].copy()
                    # 统一字段名，确保有ts_code字段
                    if 'ts_code' not in sector_df.columns:
                        sector_df['ts_code'] = sector_df['index_code']
        elif is_eastmoney_concept:
            # 东财概念板块使用dc_daily接口
            # 优先从专用缓存管理器获取数据
            sector_df = concept_cache_manager.get_concept_daily_data(
                ts_code=sector_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if sector_df is None or sector_df.empty:
                # 从API获取
                pro = ts.pro_api()
                sector_df = pro.dc_daily(ts_code=sector_code, start_date=start_date, end_date=end_date)
                if not sector_df.empty:
                    # 保存到专用缓存管理器
                    concept_cache_manager.save_concept_daily_data(sector_df)
            
            # 筛选指定板块的数据
            if not sector_df.empty and 'ts_code' in sector_df.columns:
                sector_df = sector_df[sector_df['ts_code'] == sector_code].copy()
        else:
            # 普通指数使用index_daily接口
            sector_df = index_daily_cache_manager.get_index_daily_data(
                ts_code=sector_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if sector_df is None or sector_df.empty:
                # 从API获取
                pro = ts.pro_api()
                sector_df = pro.index_daily(ts_code=sector_code, start_date=start_date, end_date=end_date)
                if not sector_df.empty:
                    index_daily_cache_manager.save_index_daily_data(sector_df)
        
        # 获取基准数据
        benchmark_df = index_daily_cache_manager.get_index_daily_data(
            ts_code=benchmark_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if benchmark_df is None or benchmark_df.empty:
            # 从API获取
            pro = ts.pro_api()
            benchmark_df = pro.index_daily(ts_code=benchmark_code, start_date=start_date, end_date=end_date)
            if not benchmark_df.empty:
                index_daily_cache_manager.save_index_daily_data(benchmark_df)
        
        if sector_df.empty or benchmark_df.empty:
            return {"error": f"无法获取 {sector_code} 或 {benchmark_code} 的数据"}
        
        # 按日期排序（最新的在前）
        sector_df = sector_df.sort_values('trade_date', ascending=False)
        benchmark_df = benchmark_df.sort_values('trade_date', ascending=False)
        
        # 提取收盘价序列（申万行业可能使用不同的字段名）
        if 'close' in sector_df.columns:
            sector_val = 'close'
        elif 'index' in sector_df.columns:
            sector_val = 'index'  # sw_daily可能使用index字段
        else:
            return {"error": f"无法找到 {sector_code} 的收盘价字段"}
            
        # 确保数据按日期对齐
        # 将 trade_date 设为索引并转为 datetime 类型
        sector_df['trade_date'] = pd.to_datetime(sector_df['trade_date'].astype(str))
        benchmark_df['trade_date'] = pd.to_datetime(benchmark_df['trade_date'].astype(str))
        
        sector_df = sector_df.set_index('trade_date')
        benchmark_df = benchmark_df.set_index('trade_date')
        
        # 取交集索引（共同的交易日），并按日期降序排列
        common_dates = sector_df.index.intersection(benchmark_df.index).sort_values(ascending=False)
        
        if len(common_dates) < 6: # 至少需要6天数据（计算5日收益率需要第6天的数据作为基准）
             return {"error": f"数据不足，共同交易日仅 {len(common_dates)} 天"}
             
        # 基于共同日期对齐数据
        sector_prices = sector_df.loc[common_dates][sector_val]
        benchmark_prices = benchmark_df.loc[common_dates]['close']
        
        # 检查最新日期是否是请求的 end_date (或者最近的交易日)
        # 只有当显式请求了 end_date 且不是今天时才进行此检查
        # (如果是今天，可能数据还没更新，我们接受最新可用的数据)
        latest_date = sector_prices.index[0].strftime('%Y%m%d')
        
        # 记录实际使用的日期，用于返回结果
        actual_date = latest_date
        
        # 计算收益率
        r_sector_1 = calculate_period_return(sector_prices, 1)
        r_sector_2 = calculate_period_return(sector_prices, 2)
        r_sector_5 = calculate_period_return(sector_prices, 5)
        r_benchmark_1 = calculate_period_return(benchmark_prices, 1)
        r_benchmark_2 = calculate_period_return(benchmark_prices, 2)
        r_benchmark_5 = calculate_period_return(benchmark_prices, 5)
        
        # 计算Alpha
        alpha_1 = calculate_alpha(r_sector_1, r_benchmark_1)
        alpha_2 = calculate_alpha(r_sector_2, r_benchmark_2)
        alpha_5 = calculate_alpha(r_sector_5, r_benchmark_5)
        
        # 计算综合得分
        score = calculate_alpha_score(alpha_2, alpha_5)
        
        # 确保score不为None（如果alpha_2存在，score应该被设置）
        if score is None and alpha_2 is not None:
            score = alpha_2  # 使用2日Alpha作为综合得分
        
        return {
            "sector_code": sector_code,
            "benchmark_code": benchmark_code,
            "end_date": end_date,
            "actual_date": actual_date,  # 添加实际使用的日期
            "r_sector_1": r_sector_1,
            "r_sector_2": r_sector_2,
            "r_sector_5": r_sector_5,
            "r_benchmark_1": r_benchmark_1,
            "r_benchmark_2": r_benchmark_2,
            "r_benchmark_5": r_benchmark_5,
            "alpha_1": alpha_1,
            "alpha_2": alpha_2,
            "alpha_5": alpha_5,
            "score": score
        }
    
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        return {"error": f"分析失败：{str(e)}\n详细信息：{error_detail}"}

def rank_sectors_alpha(
    sector_codes: List[str],
    benchmark_code: str = "000001.SH",
    end_date: str = None
) -> pd.DataFrame:
    """
    对多个板块进行Alpha排名
    
    参数:
        sector_codes: 板块指数代码列表
        benchmark_code: 基准指数代码
        end_date: 结束日期（None或空字符串时使用今天）
    
    返回:
        包含排名结果的DataFrame
    """
    # 如果end_date为空字符串，转换为None
    if end_date == "":
        end_date = None
    
    results = []
    errors = []
    
    for sector_code in sector_codes:
        result = analyze_sector_alpha(sector_code, benchmark_code, end_date)
        if "error" not in result:
            results.append(result)
        else:
            errors.append(f"{sector_code}: {result['error']}")
    
    if not results:
        # 如果所有都失败，打印前几个错误信息用于调试
        if errors:
            error_summary = "\n".join(errors[:10])  # 显示前10个错误
            print(f"所有板块数据获取失败，前10个错误:\n{error_summary}", file=sys.stderr)
        else:
            print("所有板块数据获取失败，但没有错误信息", file=sys.stderr)
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    
    # 确保score列存在且正确填充（如果score为None但alpha_2存在，使用alpha_2）
    if 'score' in df.columns:
        df['score'] = df['score'].fillna(df['alpha_2'])
    else:
        df['score'] = df['alpha_2']
    
    # 按得分排序（降序）
    df = df.sort_values('score', ascending=False, na_position='last')
    
    # 添加排名
    df['rank'] = range(1, len(df) + 1)
    
    return df

def format_alpha_analysis(df: pd.DataFrame) -> str:
    """
    格式化Alpha分析结果
    
    参数:
        df: Alpha分析结果DataFrame
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到有效的分析结果"
    
    result = []
    result.append("📊 相对强度Alpha模型分析结果")
    result.append("=" * 120)
    
    # 添加日期信息检查
    if 'actual_date' in df.columns:
        actual_dates = df['actual_date'].dropna().unique()
        requested_dates = df['end_date'].dropna().unique() if 'end_date' in df.columns else []
        
        if len(actual_dates) == 1:
            actual_date_str = str(actual_dates[0])
            result.append(f"实际数据日期: {actual_date_str}")
            
            # 检查与请求日期是否一致
            if len(requested_dates) == 1:
                req_date = str(requested_dates[0])
                if req_date and req_date != actual_date_str:
                    result.append(f"⚠️ 注意：实际数据日期 ({actual_date_str}) 与请求日期 ({req_date}) 不一致")
                    result.append("          可能是当天数据尚未更新，系统自动使用了最近的交易日数据")
        elif len(actual_dates) > 1:
            dates_str = ", ".join([str(d) for d in actual_dates[:3]])
            if len(actual_dates) > 3:
                dates_str += "..."
            result.append(f"实际数据日期: {dates_str} (存在多个日期)")
            result.append("⚠️ 注意：排名中包含不同日期的数据，请谨慎对比")
            
    result.append("")
    
    # 检查是否有name列
    has_name = 'name' in df.columns
    
    if has_name:
        result.append(f"{'排名':<6} {'板块代码':<12} {'板块名称':<12} {'当天Alpha':<12} {'2日Alpha':<12} {'5日Alpha':<12} {'综合得分':<12} {'当天收益':<12} {'2日收益':<12} {'5日收益':<12}")
    else:
        result.append(f"{'排名':<6} {'板块代码':<12} {'当天Alpha':<12} {'2日Alpha':<12} {'5日Alpha':<12} {'综合得分':<12} {'当天收益':<12} {'2日收益':<12} {'5日收益':<12}")
    result.append("-" * 140)
    
    for _, row in df.iterrows():
        rank = f"{int(row['rank'])}"
        sector_code = row['sector_code']
        
        if has_name:
            sector_name = str(row['name'])
            # 截断过长的名称
            if len(sector_name) > 6:
                sector_name = sector_name[:6]
        
        alpha_1 = f"{row['alpha_1']*100:.2f}%" if pd.notna(row['alpha_1']) else "-"
        alpha_2 = f"{row['alpha_2']*100:.2f}%" if pd.notna(row['alpha_2']) else "-"
        alpha_5 = f"{row['alpha_5']*100:.2f}%" if pd.notna(row['alpha_5']) else "-"
        
        # 计算综合得分（使用score列）
        if pd.notna(row['score']):
            score = f"{row['score']*100:.2f}%"
        elif pd.notna(row['alpha_2']):
            # 备用方案：如果score缺失但alpha_2存在
            score = f"{row['alpha_2']*100:.2f}%"
        else:
            score = "-"
        
        r_1 = f"{row['r_sector_1']*100:.2f}%" if pd.notna(row['r_sector_1']) else "-"
        r_2 = f"{row['r_sector_2']*100:.2f}%" if pd.notna(row['r_sector_2']) else "-"
        r_5 = f"{row['r_sector_5']*100:.2f}%" if pd.notna(row['r_sector_5']) else "-"
        
        if has_name:
            result.append(f"{rank:<6} {sector_code:<12} {sector_name:<12} {alpha_1:<12} {alpha_2:<12} {alpha_5:<12} {score:<12} {r_1:<12} {r_2:<12} {r_5:<12}")
        else:
            result.append(f"{rank:<6} {sector_code:<12} {alpha_1:<12} {alpha_2:<12} {alpha_5:<12} {score:<12} {r_1:<12} {r_2:<12} {r_5:<12}")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - Alpha = 板块收益率 - 基准收益率（上证指数）")
    result.append("  - 综合得分 = Alpha_2 × 60% + Alpha_5 × 40%（如果5日数据不足，则仅使用2日Alpha）")
    result.append("  - 得分越高，表示板块相对大盘越强势")
    result.append("  - 建议关注得分前5-10名的板块")
    result.append("")
    result.append(f"📊 统计：共分析 {len(df)} 个行业，其中 {len(df[df['alpha_5'].notna()])} 个行业有5日数据")
    
    return "\n".join(result)

def get_previous_trading_dates(end_date: str, days: int = 5) -> List[str]:
    """
    获取前N个交易日（包括当天）
    
    参数:
        end_date: 结束日期（YYYYMMDD格式，如果是周末会自动使用最近的交易日）
        days: 需要获取的天数（默认5天，确保有足够数据）
    
    返回:
        交易日列表（从新到旧）
    """
    token = get_tushare_token()
    if not token:
        print("警告：无法获取token", file=sys.stderr)
        return []
    
    try:
        pro = ts.pro_api()
        
        # 首先尝试使用交易日历接口获取最近的交易日
        # 如果end_date是周末，需要找到最近的交易日
        end_date_obj = datetime.strptime(end_date, '%Y%m%d')
        start_date = (end_date_obj - timedelta(days=days*3)).strftime('%Y%m%d')
        
        print(f"调试：获取交易日 - end_date={end_date}, start_date={start_date}, days={days}", file=sys.stderr)
        
        # 方法1：使用交易日历接口（更可靠，可以处理周末）
        try:
            # 获取交易日历，找到end_date之前（包括end_date）的最近N个交易日
            cal_df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open=1)
            
            if cal_df is not None and not cal_df.empty:
                # 筛选出交易日（is_open=1），按日期排序（最新的在前）
                cal_df = cal_df.sort_values('cal_date', ascending=False)
                # 只取end_date及之前的交易日
                # 确保cal_date列和end_date都是整数类型进行比较
                if cal_df['cal_date'].dtype != 'int64':
                    cal_df['cal_date'] = pd.to_numeric(cal_df['cal_date'], errors='coerce')
                end_date_int = int(end_date) if isinstance(end_date, str) else end_date
                cal_df = cal_df[cal_df['cal_date'] <= end_date_int]
                # 转换为字符串并去重
                trading_dates = cal_df['cal_date'].astype(str).unique().tolist()[:days]
                
                # 确保日期不重复
                trading_dates = list(dict.fromkeys(trading_dates))  # 保持顺序的去重
                
                if len(trading_dates) >= 1:  # 至少需要1个交易日
                    print(f"调试：从交易日历获取到{len(trading_dates)}个交易日: {trading_dates}", file=sys.stderr)
                    return trading_dates
        except Exception as e:
            print(f"调试：交易日历接口失败，尝试备用方法: {str(e)}", file=sys.stderr)
        
        # 方法2：备用方法 - 使用基准指数的数据来确定交易日
        benchmark_code = "000001.SH"
        
        benchmark_df = index_daily_cache_manager.get_index_daily_data(
            ts_code=benchmark_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if benchmark_df is None or benchmark_df.empty:
            print(f"调试：缓存中没有数据，从API获取", file=sys.stderr)
            benchmark_df = pro.index_daily(ts_code=benchmark_code, start_date=start_date, end_date=end_date)
            if not benchmark_df.empty:
                index_daily_cache_manager.save_index_daily_data(benchmark_df)
                print(f"调试：从API获取到{len(benchmark_df)}条数据", file=sys.stderr)
        
        if benchmark_df.empty:
            print(f"警告：无法获取基准指数数据来确定交易日", file=sys.stderr)
            return []
        
        # 按日期排序（最新的在前），去重并获取前N个交易日
        benchmark_df = benchmark_df.sort_values('trade_date', ascending=False)
        # 只取end_date及之前的交易日
        # 确保trade_date列是字符串类型，以便与end_date（字符串）比较
        if benchmark_df['trade_date'].dtype != 'object':
            benchmark_df['trade_date'] = benchmark_df['trade_date'].astype(str)
        benchmark_df = benchmark_df[benchmark_df['trade_date'] <= str(end_date)]
        # 去重并转换为字符串
        trading_dates = benchmark_df['trade_date'].unique().tolist()[:days]
        trading_dates = [str(date) for date in trading_dates]
        # 再次去重（确保字符串格式的日期不重复）
        trading_dates = list(dict.fromkeys(trading_dates))
        
        print(f"调试：从指数数据获取到{len(trading_dates)}个交易日: {trading_dates}", file=sys.stderr)
        
        return trading_dates
    
    except Exception as e:
        print(f"获取交易日失败: {str(e)}", file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        return []

def calculate_alpha_rank_velocity(
    sector_codes: List[str],
    benchmark_code: str = "000001.SH",
    end_date: str = None
) -> pd.DataFrame:
    """
    计算申万二级行业的Alpha排名上升速度
    
    参数:
        sector_codes: 板块指数代码列表
        benchmark_code: 基准指数代码
        end_date: 结束日期（None或空字符串时使用今天）
    
    返回:
        包含排名上升速度的DataFrame，包含以下列：
        - sector_code: 板块代码
        - current_alpha: 当天alpha值
        - current_rank: 当天排名
        - rank_change_1d: 相较昨日上升位数（正数表示上升）
        - rank_change_2d: 相较前天上升位数（正数表示上升）
    """
    # 如果end_date为空字符串，转换为None
    if end_date == "":
        end_date = None
    
    # 确定要分析的基准日期
    if end_date is None or end_date == "":
        # 如果未指定日期，使用今天
        today = datetime.now().strftime('%Y%m%d')
        end_date = today
    
    # 获取最近5个交易日（包括end_date及之前的交易日）
    # 注意：如果end_date是周末，get_previous_trading_dates会自动返回end_date之前的交易日
    # 获取5个交易日以确保有足够的历史数据进行比较
    trading_dates = get_previous_trading_dates(end_date, days=5)
    
    if len(trading_dates) < 1:
        print(f"警告：无法获取交易日，end_date={end_date}", file=sys.stderr)
        return pd.DataFrame()
    
    # 确保获取到的交易日不重复（保持顺序）
    trading_dates = list(dict.fromkeys([str(d) for d in trading_dates]))
    
    print(f"调试：获取到的交易日列表（去重后）={trading_dates}", file=sys.stderr)
    
    # trading_dates[0] 应该是最近的交易日（可能是end_date，也可能是end_date之前的交易日）
    current_date = str(trading_dates[0])  # 当天（最近的交易日），确保是字符串格式
    
    # 检查是否有足够的交易日
    if len(trading_dates) < 2:
        print(f"警告：只能获取到1个交易日，无法计算排名变化", file=sys.stderr)
        yesterday_date = None
        day_before_yesterday_date = None
    else:
        # 使用第2个交易日作为对比日期1（前1个交易日）
        yesterday_date = str(trading_dates[1]) if len(trading_dates) > 1 else None
        # 使用第3个交易日作为对比日期2（前2个交易日）
        # 如果只有2个交易日，day_before_yesterday_date 将为 None
        day_before_yesterday_date = str(trading_dates[2]) if len(trading_dates) > 2 else None
    
    # 验证日期不重复
    if current_date == yesterday_date:
        print(f"错误：当前日期和昨天日期相同！current_date={current_date}, yesterday_date={yesterday_date}", file=sys.stderr)
        print(f"调试：交易日列表={trading_dates}, end_date={end_date}", file=sys.stderr)
        # 如果日期重复，尝试获取更多交易日
        more_dates = get_previous_trading_dates(end_date, days=10)
        more_dates = list(dict.fromkeys([str(d) for d in more_dates]))  # 去重
        if len(more_dates) >= 2 and more_dates[0] != more_dates[1]:
            current_date = str(more_dates[0])
            yesterday_date = str(more_dates[1]) if len(more_dates) > 1 else None
            day_before_yesterday_date = str(more_dates[2]) if len(more_dates) > 2 else None
            print(f"调试：重新获取交易日 - 当天={current_date}, 昨天={yesterday_date}, 前天={day_before_yesterday_date}", file=sys.stderr)
        else:
            print(f"错误：无法获取足够的交易日进行比较，more_dates={more_dates}", file=sys.stderr)
            yesterday_date = None
            day_before_yesterday_date = None
    
    # 如果end_date是周末，current_date会是最近的交易日，给出提示
    if current_date != end_date:
        print(f"调试：end_date={end_date}不是交易日，使用最近的交易日={current_date}", file=sys.stderr)
    
    print(f"调试：交易日 - 当天={current_date}, 昨天={yesterday_date}, 前天={day_before_yesterday_date}", file=sys.stderr)
    
    # 计算当天排名
    try:
        df_current = rank_sectors_alpha(sector_codes, benchmark_code, current_date)
        if df_current.empty:
            print(f"警告：无法获取当天排名，current_date={current_date}", file=sys.stderr)
            print(f"提示：可能是API限流或网络问题，请稍后重试", file=sys.stderr)
            return pd.DataFrame()
        
        print(f"调试：当天排名获取成功，共{len(df_current)}个行业", file=sys.stderr)
    except Exception as e:
        print(f"错误：获取当天排名时发生异常: {str(e)}", file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        return pd.DataFrame()
    
    # 计算昨天排名（添加延迟以避免API限流）
    df_yesterday = pd.DataFrame()
    if yesterday_date:
        try:
            import time
            time.sleep(0.5)  # 延迟0.5秒，避免API限流
            df_yesterday = rank_sectors_alpha(sector_codes, benchmark_code, yesterday_date)
            if df_yesterday.empty:
                print(f"警告：无法获取昨天排名，yesterday_date={yesterday_date}", file=sys.stderr)
            else:
                print(f"调试：昨天排名获取成功，共{len(df_yesterday)}个行业", file=sys.stderr)
        except Exception as e:
            print(f"警告：获取昨天排名时发生异常: {str(e)}", file=sys.stderr)
            df_yesterday = pd.DataFrame()
    
    # 计算前天排名（添加延迟以避免API限流）
    df_day_before = pd.DataFrame()
    if day_before_yesterday_date:
        try:
            import time
            time.sleep(0.5)  # 延迟0.5秒，避免API限流
            df_day_before = rank_sectors_alpha(sector_codes, benchmark_code, day_before_yesterday_date)
            if df_day_before.empty:
                print(f"警告：无法获取前天排名，day_before_yesterday_date={day_before_yesterday_date}", file=sys.stderr)
                print(f"提示：可能是该日期数据尚未更新或API限流，部分行业将无法显示排名变化", file=sys.stderr)
            else:
                print(f"调试：前天排名获取成功，共{len(df_day_before)}个行业", file=sys.stderr)
                # 检查获取到的行业数量是否足够
                if len(df_day_before) < len(sector_codes) * 0.8:  # 如果获取到的行业少于80%，给出警告
                    print(f"警告：前天排名数据不完整，仅获取到{len(df_day_before)}/{len(sector_codes)}个行业", file=sys.stderr)
        except Exception as e:
            print(f"警告：获取前天排名时发生异常: {str(e)}", file=sys.stderr)
            import traceback
            print(traceback.format_exc(), file=sys.stderr)
            df_day_before = pd.DataFrame()
    
    # 合并数据
    result_df = df_current[['sector_code', 'score', 'rank']].copy()
    result_df.rename(columns={'score': 'current_alpha', 'rank': 'current_rank'}, inplace=True)
    
    # 创建排名映射字典
    rank_map_yesterday = {}
    if not df_yesterday.empty:
        rank_map_yesterday = dict(zip(df_yesterday['sector_code'], df_yesterday['rank']))
        print(f"调试：昨天排名映射包含{len(rank_map_yesterday)}个行业", file=sys.stderr)
    
    rank_map_day_before = {}
    if not df_day_before.empty:
        rank_map_day_before = dict(zip(df_day_before['sector_code'], df_day_before['rank']))
        print(f"调试：前天排名映射包含{len(rank_map_day_before)}个行业", file=sys.stderr)
    
    # 计算排名变化
    rank_change_1d = []
    rank_change_2d = []
    
    for sector_code in result_df['sector_code']:
        current_rank = result_df[result_df['sector_code'] == sector_code]['current_rank'].iloc[0]
        
        # 计算相较昨日上升位数
        if sector_code in rank_map_yesterday:
            yesterday_rank = rank_map_yesterday[sector_code]
            change_1d = yesterday_rank - current_rank  # 正数表示上升
        else:
            change_1d = None
            # 调试信息：如果行业在当天排名中但不在昨天排名中
            if not df_yesterday.empty:
                print(f"调试：行业{sector_code}在当天排名中但不在昨天排名中", file=sys.stderr)
        
        # 计算相较前天上升位数
        if sector_code in rank_map_day_before:
            day_before_rank = rank_map_day_before[sector_code]
            change_2d = day_before_rank - current_rank  # 正数表示上升
        else:
            change_2d = None
            # 调试信息：如果行业在当天排名中但不在前天排名中
            if not df_day_before.empty:
                print(f"调试：行业{sector_code}在当天排名中但不在前天排名中（day_before_yesterday_date={day_before_yesterday_date}）", file=sys.stderr)
            elif day_before_yesterday_date:
                # 如果前天排名数据为空，说明获取失败
                print(f"调试：无法获取{day_before_yesterday_date}的排名数据，行业{sector_code}的排名变化无法计算", file=sys.stderr)
        
        rank_change_1d.append(change_1d)
        rank_change_2d.append(change_2d)
    
    result_df['rank_change_1d'] = rank_change_1d
    result_df['rank_change_2d'] = rank_change_2d
    
    # 统计有排名变化数据的行业数量
    has_1d_data = sum(1 for x in rank_change_1d if x is not None)
    has_2d_data = sum(1 for x in rank_change_2d if x is not None)
    print(f"调试：排名变化统计 - 有1日数据：{has_1d_data}/{len(result_df)}, 有2日数据：{has_2d_data}/{len(result_df)}", file=sys.stderr)
    
    # 将日期信息添加到DataFrame的元数据中
    # 使用attrs属性（pandas 1.3.0+支持）
    if not hasattr(result_df, 'attrs'):
        result_df.attrs = {}
    result_df.attrs['current_date'] = current_date
    result_df.attrs['yesterday_date'] = yesterday_date
    result_df.attrs['day_before_yesterday_date'] = day_before_yesterday_date
    
    print(f"调试：日期信息已添加到DataFrame - current_date={current_date}, yesterday_date={yesterday_date}, day_before_yesterday_date={day_before_yesterday_date}", file=sys.stderr)
    
    return result_df

if __name__ == "__main__":
    # 测试代码
    token = get_tushare_token()
    if token:
        ts.set_token(token)
        
        # 申万一级行业代码
        sector_codes = [
            "801010.SI", "801030.SI", "801040.SI", "801050.SI", "801080.SI",
            "801110.SI", "801120.SI", "801130.SI", "801140.SI", "801150.SI",
            "801160.SI", "801170.SI", "801180.SI", "801200.SI", "801210.SI",
            "801230.SI", "801710.SI", "801720.SI", "801730.SI", "801740.SI",
            "801750.SI", "801760.SI", "801770.SI", "801780.SI", "801790.SI",
            "801880.SI", "801890.SI", "801950.SI", "801960.SI", "801970.SI",
            "801980.SI"
        ]
        
        # 分析所有板块
        df = rank_sectors_alpha(sector_codes)
        print(format_alpha_analysis(df))

