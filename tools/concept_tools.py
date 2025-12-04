"""概念板块相关MCP工具"""
import tushare as ts
import pandas as pd
import numpy as np
import json
from typing import TYPE_CHECKING, Optional, List, Dict
from datetime import datetime, timedelta

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from config.token_manager import get_tushare_token
from cache.concept_cache_manager import concept_cache_manager
from cache.cache_manager import cache_manager
from tools.alpha_strategy_analyzer import (
    analyze_sector_alpha,
    rank_sectors_alpha,
    format_alpha_analysis,
    calculate_alpha_rank_velocity
)
from utils.common import format_date

# 自定义JSON编码器，处理numpy类型
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

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
        # 优先从缓存获取数据
        df = concept_cache_manager.get_concept_index_data(trade_date=trade_date)
        
        # 如果缓存中没有数据，从API获取
        if df is None or df.empty:
            pro = ts.pro_api()
            df = pro.dc_index(trade_date=trade_date)
            # 保存到缓存
            if not df.empty:
                concept_cache_manager.save_concept_index_data(df)
        
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

def format_concept_alpha_analysis(df: pd.DataFrame) -> str:
    """
    格式化概念板块Alpha分析结果（包含板块名称）
    
    参数:
        df: Alpha分析结果DataFrame，应包含name、pct_change等字段
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到有效的分析结果"
    
    result = []
    result.append("📊 相对强度Alpha模型分析结果")
    result.append("=" * 150)
    result.append("")
    result.append(f"{'排名':<6} {'板块代码':<12} {'板块名称':<20} {'2日Alpha':<12} {'5日Alpha':<12} {'综合得分':<12} {'2日收益':<12} {'5日收益':<12} {'今日涨跌':<10} {'换手率':<10}")
    result.append("-" * 150)
    
    for _, row in df.iterrows():
        rank = f"{int(row['rank'])}"
        sector_code = row['sector_code']
        name = str(row.get('name', sector_code))[:18] if 'name' in row else sector_code
        alpha_2 = f"{row['alpha_2']*100:.2f}%" if pd.notna(row['alpha_2']) else "-"
        alpha_5 = f"{row['alpha_5']*100:.2f}%" if pd.notna(row['alpha_5']) else "-"
        
        # 计算综合得分
        if pd.notna(row.get('score')):
            score = f"{row['score']*100:.2f}%"
        elif pd.notna(row['alpha_2']):
            score = f"{row['alpha_2']*100:.2f}%"
        else:
            score = "-"
        
        r_2 = f"{row['r_sector_2']*100:.2f}%" if pd.notna(row['r_sector_2']) else "-"
        r_5 = f"{row['r_sector_5']*100:.2f}%" if pd.notna(row['r_sector_5']) else "-"
        
        # 今日涨跌幅
        pct_change = f"{row.get('pct_change', 0):.2f}%" if 'pct_change' in row and pd.notna(row.get('pct_change')) else "-"
        
        # 换手率
        turnover = f"{row.get('turnover', 0):.2f}%" if 'turnover' in row and pd.notna(row.get('turnover')) else "-"
        
        result.append(f"{rank:<6} {sector_code:<12} {name:<20} {alpha_2:<12} {alpha_5:<12} {score:<12} {r_2:<12} {r_5:<12} {pct_change:<10} {turnover:<10}")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - Alpha = 板块收益率 - 基准收益率（沪深300）")
    result.append("  - 综合得分 = Alpha_2 × 60% + Alpha_5 × 40%（如果5日数据不足，则仅使用2日Alpha）")
    result.append("  - 得分越高，表示板块相对大盘越强势")
    result.append("  - 建议关注得分前5-10名的板块")
    result.append("")
    result.append(f"📊 统计：共分析 {len(df)} 个概念板块，其中 {len(df[df['alpha_5'].notna()])} 个板块有5日数据")
    
    return "\n".join(result)

def get_hot_concept_codes(trade_date: str = None, limit: int = 30) -> List[str]:
    """
    获取热门东财概念板块代码列表（基于综合潜力得分CP_Score筛选）
    
    使用综合潜力得分(CP_Score)算法：
    - 趋势得分(40%): 涨跌幅排名
    - 热度得分(30%): 换手率排名
    - 领涨得分(20%): 领涨股票涨跌幅排名
    - 广度得分(10%): 普涨率排名
    
    参数:
        trade_date: 交易日期（YYYYMMDD格式，默认今天）
        limit: 返回的热门板块数量（默认30）
    
    返回:
        热门概念板块代码列表
    """
    token = get_tushare_token()
    if not token:
        return []
    
    if trade_date is None:
        trade_date = datetime.now().strftime('%Y%m%d')
    
    try:
        # 优先从缓存获取数据
        df = concept_cache_manager.get_concept_index_data(trade_date=trade_date)
        
        # 如果缓存中没有数据，从API获取
        if df is None or df.empty:
            pro = ts.pro_api()
            df = pro.dc_index(trade_date=trade_date)
            # 保存到缓存
            if not df.empty:
                concept_cache_manager.save_concept_index_data(df)
        
        if df.empty:
            return []
        
        if 'ts_code' not in df.columns:
            return []
        
        # ==========================
        # 1. 数据清洗与预处理
        # ==========================
        data = df.copy()
        
        # 预计算普涨率 (Breadth)
        # 防止除以0，分母加一个小极值
        if 'up_num' in data.columns and 'down_num' in data.columns:
            # 填充缺失值为0
            up_num = data['up_num'].fillna(0)
            down_num = data['down_num'].fillna(0)
            data['up_ratio'] = up_num / (up_num + down_num + 0.0001)
        else:
            # 如果没有上涨/下跌家数数据，使用默认值0.5
            data['up_ratio'] = 0.5
        
        # ==========================
        # 2. 预筛选 (硬门槛)
        # ==========================
        # 剔除极小市值板块（容易被操纵，数据失真）和极大市值板块（大象难起舞）
        # total_mv 单位是万元，保留 50亿 ~ 5000亿 之间的板块
        # 50亿 = 500000万元，5000亿 = 50000000万元
        if 'total_mv' in data.columns:
            data = data[(data['total_mv'] > 500000) & (data['total_mv'] < 50000000)]
        
        # 剔除当天大跌的板块（可选，根据需求决定是否启用）
        # if 'pct_change' in data.columns:
        #     data = data[data['pct_change'] > -2.0]
        
        if data.empty:
            return []
        
        # ==========================
        # 3. 计算分项排名得分 (0 ~ 1)
        # ==========================
        # pct=True 表示生成百分位排名，最大值为1，最小值为0
        
        # 趋势得分：涨跌幅排名
        if 'pct_change' in data.columns:
            data['score_trend'] = data['pct_change'].rank(pct=True, na_option='keep')
        else:
            data['score_trend'] = 0.5  # 默认中等得分
        
        # 热度得分：换手率排名
        if 'turnover_rate' in data.columns:
            data['score_heat'] = data['turnover_rate'].rank(pct=True, na_option='keep')
        else:
            data['score_heat'] = 0.5  # 默认中等得分
        
        # 领涨得分：领涨股票涨跌幅排名
        if 'leading_pct' in data.columns:
            data['score_leader'] = data['leading_pct'].rank(pct=True, na_option='keep')
        else:
            data['score_leader'] = 0.5  # 默认中等得分
        
        # 广度得分：普涨率排名
        data['score_breadth'] = data['up_ratio'].rank(pct=True, na_option='keep')
        
        # 填充缺失值（如果有）
        data['score_trend'] = data['score_trend'].fillna(0.5)
        data['score_heat'] = data['score_heat'].fillna(0.5)
        data['score_leader'] = data['score_leader'].fillna(0.5)
        data['score_breadth'] = data['score_breadth'].fillna(0.5)
        
        # ==========================
        # 4. 计算综合 CP_Score
        # ==========================
        data['cp_score'] = (
            0.4 * data['score_trend'] +
            0.3 * data['score_heat'] +
            0.2 * data['score_leader'] +
            0.1 * data['score_breadth']
        )
        
        # ==========================
        # 5. 输出结果
        # ==========================
        # 按得分降序排列，取前limit个
        result = data.sort_values(by='cp_score', ascending=False).head(limit)
        
        # 提取板块代码
        codes = result['ts_code'].unique().tolist()
        return sorted(codes)
        
    except Exception as e:
        import sys
        print(f"获取热门概念板块代码失败: {str(e)}", file=sys.stderr)
        # 如果筛选失败，返回所有板块代码（降级处理）
        return get_concept_codes(trade_date)

def analyze_concept_volume_anomaly(
    concept_code: str,
    end_date: str = None,
    vol_ratio_threshold: float = 1.3,
    price_change_5d_min: float = 0.02,
    price_change_5d_max: float = 0.08,
    return_all: bool = False
) -> Optional[Dict]:
    """
    分析单个东财概念板块的成交量异动
    
    参数:
        concept_code: 概念板块代码（如：BK1184.DC）
        end_date: 结束日期（YYYYMMDD格式，默认今天）
        vol_ratio_threshold: 成交量比率阈值（默认1.8，即MA3/MA10 > 1.8）
        price_change_5d_min: 5日涨幅最小值（默认0.02，即2%）
        price_change_5d_max: 5日涨幅最大值（默认0.08，即8%）
        return_all: 是否返回所有数据（包括不符合条件的），用于找出最接近的数据
    
    返回:
        如果return_all=False且匹配条件，返回包含分析结果的字典；否则返回None
        如果return_all=True，总是返回包含分析结果的字典，并包含is_match字段
    """
    token = get_tushare_token()
    if not token:
        return None
    
    if end_date is None or end_date == "":
        end_date = datetime.now().strftime('%Y%m%d')
    
    try:
        # 获取至少60天的数据（用于计算均线）
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=60)).strftime('%Y%m%d')
        
        # 获取东财概念板块日线数据
        pro = ts.pro_api()
        
        # 优先从缓存获取
        df = concept_cache_manager.get_concept_daily_data(
            ts_code=concept_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if df is None or df.empty:
            # 从API获取（不传递idx_type，因为已经指定了ts_code）
            df = pro.dc_daily(ts_code=concept_code, start_date=start_date, end_date=end_date)
            if not df.empty:
                concept_cache_manager.save_concept_daily_data(df)
        
        if df.empty:
            return None
        
        # 筛选指定概念板块的数据
        if 'ts_code' in df.columns:
            df = df[df['ts_code'] == concept_code].copy()
        
        if df.empty:
            return None
        
        # 按日期排序（最新的在前）
        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
        
        # 检查是否有足够的数据（至少需要10个交易日用于计算MA10）
        if len(df) < 10:
            return None
        
        # 获取最新数据
        latest = df.iloc[0]
        current_price = latest.get('close', 0)
        
        if pd.isna(current_price) or current_price == 0:
            return None
        
        # 计算成交量MA（固定使用MA3和MA10）
        if 'vol' not in df.columns:
            return None
        
        vol_series = df['vol'].copy()
        vol_ma_short = 3  # 固定使用MA3
        vol_ma_long = 10  # 固定使用MA10
        max_ma = max(vol_ma_short, vol_ma_long)
        
        if vol_series.isna().all() or len(vol_series) < max_ma:
            return None
        
        # 计算移动平均（使用最新的N天数据）
        # head() 获取前N行（最新的N天，因为已按日期降序排列）
        ma3_vol = vol_series.head(vol_ma_short).mean()
        ma10_vol = vol_series.head(vol_ma_long).mean()
        
        if pd.isna(ma3_vol) or pd.isna(ma10_vol) or ma10_vol == 0:
            return None
        
        # Volume_Ratio = MA3_Vol / MA10_Vol
        vol_ratio = ma3_vol / ma10_vol
        
        # 计算5日涨幅
        # 需要至少6个交易日（今天 + 5天前）
        if len(df) < 6:
            return None
        
        # iloc[5] 是第6个数据（索引从0开始），即5天前
        # 因为数据已按日期降序排列，iloc[0]是今天，iloc[5]是5天前
        price_5d_ago = df.iloc[5].get('close', 0)
        if pd.isna(price_5d_ago) or price_5d_ago == 0:
            return None
        
        price_change_5d = (current_price - price_5d_ago) / price_5d_ago
        
        # 获取换手率（Turnover_Rate）
        turnover_rate = latest.get('turnover_rate', 0)
        if pd.isna(turnover_rate):
            turnover_rate = 0
        
        # 判断是否符合筛选条件
        is_match = (vol_ratio > vol_ratio_threshold and 
                   price_change_5d_min < price_change_5d < price_change_5d_max)
        
        # 如果return_all=False且不符合条件，返回None
        if not return_all and not is_match:
            return None
        
        # 计算距离阈值的差距（用于排序找出最接近的数据）
        vol_ratio_diff = vol_ratio - vol_ratio_threshold  # 正数表示超过阈值
        if price_change_5d < price_change_5d_min:
            price_diff = price_change_5d_min - price_change_5d  # 低于最小值
        elif price_change_5d > price_change_5d_max:
            price_diff = price_change_5d - price_change_5d_max  # 超过最大值
        else:
            price_diff = 0  # 在范围内
        
        # 综合距离分数（越小越接近条件）
        # 使用欧几里得距离的简化版本
        distance_score = abs(vol_ratio_diff) + abs(price_diff) * 10  # 价格差异权重更高
        
        return {
            'code': concept_code,
            'vol_ratio': round(vol_ratio, 2),
            'vol_ma_short': vol_ma_short,  # MA3
            'vol_ma_long': vol_ma_long,  # MA10
            'price_change_5d': round(price_change_5d, 4),
            'turnover_rate': round(turnover_rate, 2),
            'current_price': round(current_price, 2),
            'is_match': is_match,
            'distance_score': round(distance_score, 4),
            'vol_ratio_diff': round(vol_ratio_diff, 2),
            'price_diff': round(price_diff, 4)
        }
    
    except Exception as e:
        import sys
        print(f"分析 {concept_code} 失败: {str(e)}", file=sys.stderr)
        return None

def scan_concept_volume_anomaly(
    end_date: str = None,
    vol_ratio_threshold: float = 1.8,
    price_change_5d_min: float = 0.02,
    price_change_5d_max: float = 0.08,
    hot_limit: int = 160
) -> Dict:
    """
    扫描热门东财概念板块的成交量异动
    
    参数:
        end_date: 结束日期（YYYYMMDD格式，默认今天）
        vol_ratio_threshold: 成交量比率阈值（默认1.8，即MA3/MA10 > 1.8）
        price_change_5d_min: 5日涨幅最小值（默认0.02，即2%）
        price_change_5d_max: 5日涨幅最大值（默认0.08，即8%）
        hot_limit: 扫描的热门概念板块数量（默认160，根据成交额和换手率筛选）
    
    返回:
        包含扫描结果的字典，如果没有匹配的数据，会返回最接近的前10个数据
    """
    if end_date is None or end_date == "":
        end_date = datetime.now().strftime('%Y%m%d')
    
    # 获取热门概念板块代码列表
    concept_codes = get_hot_concept_codes(end_date, limit=hot_limit)
    
    matches = []
    all_results = []  # 存储所有结果（包括不符合条件的）
    
    # 获取概念板块名称映射
    name_map = {}
    try:
        pro = ts.pro_api()
        concept_codes_str = ','.join(concept_codes)
        concept_df = pro.dc_index(trade_date=end_date, ts_code=concept_codes_str)
        if not concept_df.empty and 'ts_code' in concept_df.columns and 'name' in concept_df.columns:
            for _, row in concept_df.iterrows():
                name_map[row['ts_code']] = row.get('name', row['ts_code'])
    except Exception as e:
        import sys
        print(f"获取概念板块名称失败: {str(e)}", file=sys.stderr)
    
    # 收集所有概念的数据（包括不符合条件的）
    for concept_code in concept_codes:
        result = analyze_concept_volume_anomaly(
            concept_code,
            end_date,
            vol_ratio_threshold,
            price_change_5d_min,
            price_change_5d_max,
            return_all=True  # 返回所有数据
        )
        
        if result:
            # 获取概念板块名称
            concept_name = name_map.get(concept_code, concept_code)
            
            # 构建结果数据
            match_data = {
                "code": result['code'],
                "name": concept_name,
                "metrics": {
                    "vol_ratio": result['vol_ratio'],
                    "vol_ma_short": result.get('vol_ma_short', 3),  # MA3
                    "vol_ma_long": result.get('vol_ma_long', 10),  # MA10
                    "price_change_5d": result['price_change_5d'],
                    "turnover_rate": result.get('turnover_rate', 0),
                    "current_price": result['current_price']
                },
                "distance_score": result.get('distance_score', 999),
                "vol_ratio_diff": result.get('vol_ratio_diff', 0),
                "price_diff": result.get('price_diff', 0),
                "is_match": result.get('is_match', False),
                "reasoning": _build_reasoning(result, vol_ratio_threshold, price_change_5d_min, price_change_5d_max)
            }
            
            all_results.append(match_data)
            
            # 如果符合条件，也加入matches
            if result.get('is_match', False):
                matches.append(match_data)
    
    # 对所有结果按distance_score排序
    all_results.sort(key=lambda x: x['distance_score'])
    
    # 如果没有匹配的数据，返回最接近的前80个
    if len(matches) == 0 and len(all_results) > 0:
        closest_results = all_results[:80]
        
        return {
            "scanned_count": len(concept_codes),
            "matched_count": 0,
            "matches": [],
            "closest_results": closest_results,
            "all_results": all_results[:80],  # 添加所有结果（前80名）
            "message": f"未找到符合条件的数据，以下是最接近的前80个概念板块："
        }
    
    # 如果有匹配的数据，也返回所有结果（前80名）以便完整分析
    return {
        "scanned_count": len(concept_codes),
        "matched_count": len(matches),
        "matches": matches,
        "all_results": all_results[:80]  # 添加所有结果（前80名）
    }

def _build_reasoning(result: Dict, vol_ratio_threshold: float, price_change_5d_min: float, price_change_5d_max: float) -> str:
    """
    构建推理说明文本
    
    参数:
        result: 分析结果字典
        vol_ratio_threshold: 成交量比率阈值
        price_change_5d_min: 5日涨幅最小值
        price_change_5d_max: 5日涨幅最大值
    
    返回:
        推理说明文本
    """
    vol_ratio = result['vol_ratio']
    price_change_5d = result['price_change_5d']
    vol_ratio_diff = result.get('vol_ratio_diff', 0)
    price_diff = result.get('price_diff', 0)
    
    parts = []
    
    # 成交量比率说明
    if vol_ratio > vol_ratio_threshold:
        parts.append(f"成交量比率 {vol_ratio:.2f} 超过阈值 {vol_ratio_threshold:.2f} (超出 {vol_ratio_diff:.2f})")
    else:
        parts.append(f"成交量比率 {vol_ratio:.2f} 低于阈值 {vol_ratio_threshold:.2f} (差距 {abs(vol_ratio_diff):.2f})")
    
    # 5日涨幅说明
    if price_change_5d_min <= price_change_5d <= price_change_5d_max:
        parts.append(f"5日涨幅 {price_change_5d*100:.2f}% 在范围内 ({price_change_5d_min*100:.0f}% - {price_change_5d_max*100:.0f}%)")
    elif price_change_5d < price_change_5d_min:
        parts.append(f"5日涨幅 {price_change_5d*100:.2f}% 低于最小值 {price_change_5d_min*100:.0f}% (差距 {abs(price_diff)*100:.2f}%)")
    else:
        parts.append(f"5日涨幅 {price_change_5d*100:.2f}% 超过最大值 {price_change_5d_max*100:.0f}% (超出 {abs(price_diff)*100:.2f}%)")
    
    return "; ".join(parts)

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
        获取东方财富概念板块行情数据
        
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
            
            # 优先从缓存获取数据
            df = None
            if trade_date:
                # 单日期查询，优先从缓存获取
                df = concept_cache_manager.get_concept_index_data(
                    ts_code=ts_code if ts_code else None,
                    name=name if name else None,
                    trade_date=trade_date
                )
            elif start_date and end_date:
                # 日期范围查询，检查缓存是否完整
                df = concept_cache_manager.get_concept_index_data(
                    ts_code=ts_code if ts_code else None,
                    name=name if name else None,
                    start_date=start_date,
                    end_date=end_date
                )
            
            # 如果缓存中没有数据，从API获取
            if df is None or df.empty:
                df = pro.dc_index(**params)
                # 保存到缓存
                if not df.empty:
                    concept_cache_manager.save_concept_index_data(df)
            
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
            
            # 优先从缓存获取数据
            df = concept_cache_manager.get_concept_member_data(
                ts_code=ts_code if ts_code else None,
                con_code=con_code if con_code else None,
                trade_date=trade_date if trade_date else None
            )
            
            # 如果缓存中没有数据，从API获取
            if df is None or df.empty:
                df = pro.dc_member(**params)
                # 保存到缓存
                if not df.empty:
                    concept_cache_manager.save_concept_member_data(df)
            
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
            
            # 优先从缓存获取数据
            df = None
            if trade_date:
                # 单日期查询，优先从缓存获取
                df = concept_cache_manager.get_concept_daily_data(
                    ts_code=ts_code if ts_code else None,
                    trade_date=trade_date,
                    idx_type=idx_type if idx_type else None
                )
            elif start_date and end_date:
                # 日期范围查询，检查缓存是否完整
                df = concept_cache_manager.get_concept_daily_data(
                    ts_code=ts_code if ts_code else None,
                    start_date=start_date,
                    end_date=end_date,
                    idx_type=idx_type if idx_type else None
                )
            
            # 如果缓存中没有数据，从API获取
            if df is None or df.empty:
                df = pro.dc_daily(**params)
                # 保存到缓存
                if not df.empty:
                    concept_cache_manager.save_concept_daily_data(df)
            
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
        top_n: int = 20,
        hot_limit: int = 80
    ) -> str:
        """
        对热门东财概念板块进行Alpha排名（仅查询热门概念板块以减少计算量）
        
        参数:
            benchmark_code: 基准指数代码（默认：000300.SH沪深300）
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            top_n: 显示前N名（默认20）
            hot_limit: 筛选的热门概念板块数量（默认200，根据成交额和换手率筛选）
        
        返回:
            包含排名结果的格式化字符串
        
        说明:
            - 自动获取指定日期的热门概念板块（根据成交额和换手率筛选）
            - 按综合得分降序排列
            - 显示前N名强势板块
            - 仅分析热门板块以减少计算量，提高响应速度
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 如果end_date为空，使用None让analyze_sector_alpha使用默认值
        if end_date == "":
            end_date = None
        
        try:
            # 获取热门概念板块代码列表（根据成交额和换手率筛选）
            trade_date_str = end_date if end_date else datetime.now().strftime('%Y%m%d')
            concept_codes = get_hot_concept_codes(trade_date_str, limit=hot_limit)
            
            if not concept_codes:
                return "无法获取热门概念板块列表，请检查网络连接和token配置。\n提示：可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 进行Alpha排名
            df = rank_sectors_alpha(concept_codes, benchmark_code, end_date)
            
            if df.empty:
                return "无法获取板块数据，请检查网络连接和token配置。\n提示：如果所有板块都返回错误，可能是数据获取失败，请检查Tushare token是否有效。"
            
            # 显示所有排名（如果top_n大于等于总数，显示全部）
            if top_n >= len(df):
                df_display = df
            else:
                df_display = df.head(top_n)
            
            # 获取板块名称和今日行情数据
            try:
                pro = ts.pro_api()
                concept_codes_str = ','.join(df_display['sector_code'].tolist())
                
                # 直接使用API查询，不通过缓存管理器（因为可能不支持多代码查询）
                concept_df = pro.dc_index(ts_code=concept_codes_str, trade_date=trade_date_str)
                
                # 创建板块代码到名称的映射
                name_map = {}
                pct_map = {}
                amount_map = {}
                turnover_map = {}
                
                if not concept_df.empty and 'ts_code' in concept_df.columns:
                    for _, row in concept_df.iterrows():
                        code = row['ts_code']
                        name_map[code] = row.get('name', code) if pd.notna(row.get('name')) else code
                        pct_map[code] = row.get('pct_change', 0) if pd.notna(row.get('pct_change')) else 0
                        amount_map[code] = row.get('amount', 0) if pd.notna(row.get('amount')) else 0
                        turnover_map[code] = row.get('turnover', 0) if pd.notna(row.get('turnover')) else 0
                
                # 添加板块名称等信息到DataFrame
                df_display['name'] = df_display['sector_code'].map(name_map).fillna(df_display['sector_code'])
                df_display['pct_change'] = df_display['sector_code'].map(pct_map).fillna(0)
                df_display['amount'] = df_display['sector_code'].map(amount_map).fillna(0)
                df_display['turnover'] = df_display['sector_code'].map(turnover_map).fillna(0)
                
            except Exception as e:
                # 如果获取名称失败，使用代码作为名称
                import sys
                print(f"获取板块名称失败: {str(e)}", file=sys.stderr)
                df_display['name'] = df_display['sector_code']
                df_display['pct_change'] = 0
                df_display['amount'] = 0
                df_display['turnover'] = 0
            
            # 使用自定义格式化函数显示完整信息
            result = format_concept_alpha_analysis(df_display)
            
            # 如果只显示了部分，添加提示
            if top_n < len(df):
                result += f"\n\n（共分析 {len(df)} 个热门概念板块，仅显示前 {top_n} 名）"
            else:
                result += f"\n\n（共分析 {len(df)} 个热门概念板块）"
            
            result += f"\n（从热门板块中筛选，筛选标准：成交额和换手率，筛选数量：{hot_limit}）"
            
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
    
    @mcp.tool()
    def get_concept_moneyflow_dc(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        content_type: str = ""
    ) -> str:
        """
        获取东方财富板块资金流向数据（概念、行业、地域）
        
        参数:
            ts_code: 板块代码（如：BK1184.DC，留空则查询所有板块）
            trade_date: 交易日期（YYYYMMDD格式，如：20240927，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
            content_type: 资金类型（行业、概念、地域，留空则查询所有类型）
        
        返回:
            包含板块资金流向数据的格式化字符串
        
        说明:
            - 数据来源：东方财富，每天盘后更新
            - 支持按板块代码、交易日期、日期范围、资金类型筛选
            - 显示主力净流入额、超大单/大单/中单/小单的净流入额和占比
            - 显示主力净流入最大股、排名等信息
            - 权限要求：5000积分
            - 限量：单次最大可调取5000条数据，可以根据日期和代码循环提取全部数据
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
            if content_type:
                params['content_type'] = content_type
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or '',
                'content_type': content_type or ''
            }
            df = cache_manager.get_dataframe('moneyflow_ind_dc', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('moneyflow_ind_dc', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                df = pro.moneyflow_ind_dc(**params)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('moneyflow_ind_dc', df, **cache_params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"板块代码: {ts_code}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if content_type:
                    param_info.append(f"资金类型: {content_type}")
                
                return f"未找到符合条件的板块资金流向数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期和排名排序（最新的在前，排名升序）
            sort_columns = []
            if 'trade_date' in df.columns:
                sort_columns.append('trade_date')
            if 'rank' in df.columns:
                sort_columns.append('rank')
            if sort_columns:
                df = df.sort_values(sort_columns, ascending=[False, True])
            
            # 格式化输出
            return format_concept_moneyflow_dc_data(df, ts_code, content_type or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def scan_concepts_volume_anomaly(
        end_date: str = "",
        vol_ratio_threshold: float = 1.15,
        price_change_5d_min: float = 0.02,
        price_change_5d_max: float = 0.08,
        hot_limit: int = 160
    ) -> str:
        """
        分析东财概念板块成交量异动
        
        参数:
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            vol_ratio_threshold: 成交量比率阈值（默认1.8，即MA3/MA10 > 1.8，资金进场）
            price_change_5d_min: 5日涨幅最小值（默认0.02，即2%，右侧启动）
            price_change_5d_max: 5日涨幅最大值（默认0.08，即8%，拒绝左侧死鱼）
            hot_limit: 扫描的热门概念板块数量（默认80，根据成交额和换手率筛选）
        
        返回:
            JSON格式字符串，包含扫描结果
        
        说明:
            - 扫描热门东财概念板块（根据成交额和换手率筛选）
            - 计算指标：
              * Volume_Ratio = MA3_Vol / MA10_Vol
              * Price_Change_5d（5日涨幅）
              * Turnover_Rate（换手率）
            - 筛选逻辑：
              * Volume_Ratio > vol_ratio_threshold（资金进场）
              * price_change_5d_min < Price_Change_5d < price_change_5d_max（右侧启动）
            - 如果没有符合条件的数据，会返回最接近的前10个数据，并展示具体的参数细节
        """
        token = get_tushare_token()
        if not token:
            return json.dumps({
                "error": "请先配置Tushare token",
                "scanned_count": 0,
                "matched_count": 0,
                "matches": []
            }, ensure_ascii=False, indent=2, cls=NumpyEncoder)
        
        try:
            # 如果end_date为空，使用None让函数使用默认值
            if end_date == "":
                end_date = None
            
            # 验证参数
            if vol_ratio_threshold <= 0:
                return json.dumps({
                    "error": "成交量比率阈值必须大于0",
                    "scanned_count": 0,
                    "matched_count": 0,
                    "matches": []
                }, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
            if price_change_5d_min >= price_change_5d_max:
                return json.dumps({
                    "error": "5日涨幅最小值必须小于最大值",
                    "scanned_count": 0,
                    "matched_count": 0,
                    "matches": []
                }, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
            # 扫描成交量异动（调用模块级别的函数）
            result = scan_concept_volume_anomaly(
                end_date=end_date,
                vol_ratio_threshold=vol_ratio_threshold,
                price_change_5d_min=price_change_5d_min,
                price_change_5d_max=price_change_5d_max,
                hot_limit=hot_limit
            )
            
            # 如果没有匹配的数据，格式化最接近的结果
            if result.get('matched_count', 0) == 0 and 'closest_results' in result:
                # 添加筛选条件信息
                result['filter_criteria'] = {
                    "vol_ratio_threshold": vol_ratio_threshold,
                    "price_change_5d_min": price_change_5d_min,
                    "price_change_5d_max": price_change_5d_max
                }
            
            # 返回JSON格式字符串（使用自定义编码器处理numpy类型）
            return json.dumps(result, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return json.dumps({
                "error": f"扫描失败：{str(e)}",
                "details": error_detail,
                "scanned_count": 0,
                "matched_count": 0,
                "matches": []
            }, ensure_ascii=False, indent=2, cls=NumpyEncoder)

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


def format_concept_moneyflow_dc_data(df: pd.DataFrame, ts_code: str = "", content_type: str = "") -> str:
    """
    格式化板块资金流向数据输出
    
    参数:
        df: 板块资金流向数据DataFrame
        ts_code: 板块代码（用于显示）
        content_type: 资金类型（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的板块资金流向数据"
    
    # 按交易日期和排名排序（最新的在前，排名升序）
    sort_columns = []
    if 'trade_date' in df.columns:
        sort_columns.append('trade_date')
    if 'rank' in df.columns:
        sort_columns.append('rank')
    if sort_columns:
        df = df.sort_values(sort_columns, ascending=[False, True])
    
    result = []
    
    # 如果查询的是单个板块或多个板块
    if ts_code:
        # 按板块代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                result.append(format_single_concept_moneyflow_dc(code_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 如果有多个交易日期，按日期分组显示
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
            dates = sorted(df['trade_date'].unique(), reverse=True)
            for date in dates[:10]:  # 最多显示最近10个交易日
                date_df = df[df['trade_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 交易日期: {format_date(date)}")
                    result.append("=" * 180)
                    
                    # 按资金类型分组
                    if content_type:
                        # 指定了类型，直接显示
                        result.append(format_moneyflow_table(date_df, content_type))
                    else:
                        # 未指定类型，按类型分组显示
                        if 'content_type' in date_df.columns:
                            types = date_df['content_type'].unique()
                            for ct in types:
                                type_df = date_df[date_df['content_type'] == ct]
                                if not type_df.empty:
                                    result.append(f"📊 {ct}资金流向（按主力净流入排序）：")
                                    result.append(format_moneyflow_table(type_df, ct))
                                    result.append("")
                        else:
                            result.append(format_moneyflow_table(date_df, ""))
                    result.append("")
        else:
            # 单个日期或单个板块，使用详细格式
            if ts_code and len(df['ts_code'].unique()) == 1:
                result.append(format_single_concept_moneyflow_dc(df, df['ts_code'].iloc[0]))
            else:
                # 显示所有板块
                result.append("📊 板块资金流向数据")
                result.append("=" * 180)
                
                # 按资金类型分组
                if content_type:
                    result.append(format_moneyflow_table(df, content_type))
                else:
                    if 'content_type' in df.columns:
                        types = df['content_type'].unique()
                        for ct in types:
                            type_df = df[df['content_type'] == ct]
                            if not type_df.empty:
                                result.append(f"📊 {ct}资金流向（按主力净流入排序）：")
                                result.append(format_moneyflow_table(type_df, ct))
                                result.append("")
                    else:
                        result.append(format_moneyflow_table(df, ""))
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：东方财富，每天盘后更新")
    result.append("  - 主力净流入 = 超大单净流入 + 大单净流入")
    result.append("  - 正数表示净流入，负数表示净流出")
    result.append("  - 权限要求：5000积分")
    result.append("  - 限量：单次最大可调取5000条数据")
    
    return "\n".join(result)


def format_moneyflow_table(df: pd.DataFrame, content_type: str = "") -> str:
    """
    格式化资金流向表格
    
    参数:
        df: 资金流向数据DataFrame
        content_type: 资金类型
    
    返回:
        格式化后的表格字符串
    """
    if df.empty:
        return ""
    
    # 按主力净流入额排序（降序）
    if 'net_amount' in df.columns:
        df = df.sort_values('net_amount', ascending=False)
    
    # 重置索引，以便生成连续序号
    df = df.reset_index(drop=True)
    
    result = []
    result.append(f"{'排名':<6} {'板块代码':<15} {'板块名称':<20} {'涨跌幅':<10} {'最新指数':<12} {'主力净流入(元)':<18} {'主力净流入占比':<16} {'超大单净流入(元)':<18} {'超大单占比':<14} {'大单净流入(元)':<16} {'大单占比':<12} {'中单净流入(元)':<16} {'中单占比':<12} {'小单净流入(元)':<16} {'小单占比':<12} {'主力净流入最大股':<20}")
    result.append("-" * 180)
    
    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        rank = str(idx)  # 使用连续序号，而不是原始rank字段
        code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
        name = str(row['name'])[:18] if 'name' in row and pd.notna(row['name']) else "-"
        pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
        close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
        net_amount = f"{row['net_amount']:.2f}" if 'net_amount' in row and pd.notna(row['net_amount']) else "-"
        net_amount_rate = f"{row['net_amount_rate']:+.2f}%" if 'net_amount_rate' in row and pd.notna(row['net_amount_rate']) else "-"
        buy_elg_amount = f"{row['buy_elg_amount']:.2f}" if 'buy_elg_amount' in row and pd.notna(row['buy_elg_amount']) else "-"
        buy_elg_amount_rate = f"{row['buy_elg_amount_rate']:+.2f}%" if 'buy_elg_amount_rate' in row and pd.notna(row['buy_elg_amount_rate']) else "-"
        buy_lg_amount = f"{row['buy_lg_amount']:.2f}" if 'buy_lg_amount' in row and pd.notna(row['buy_lg_amount']) else "-"
        buy_lg_amount_rate = f"{row['buy_lg_amount_rate']:+.2f}%" if 'buy_lg_amount_rate' in row and pd.notna(row['buy_lg_amount_rate']) else "-"
        buy_md_amount = f"{row['buy_md_amount']:.2f}" if 'buy_md_amount' in row and pd.notna(row['buy_md_amount']) else "-"
        buy_md_amount_rate = f"{row['buy_md_amount_rate']:+.2f}%" if 'buy_md_amount_rate' in row and pd.notna(row['buy_md_amount_rate']) else "-"
        buy_sm_amount = f"{row['buy_sm_amount']:.2f}" if 'buy_sm_amount' in row and pd.notna(row['buy_sm_amount']) else "-"
        buy_sm_amount_rate = f"{row['buy_sm_amount_rate']:+.2f}%" if 'buy_sm_amount_rate' in row and pd.notna(row['buy_sm_amount_rate']) else "-"
        max_stock = str(row['buy_sm_amount_stock'])[:18] if 'buy_sm_amount_stock' in row and pd.notna(row['buy_sm_amount_stock']) else "-"
        
        result.append(f"{rank:<6} {code:<15} {name:<20} {pct_change:<10} {close:<12} {net_amount:<18} {net_amount_rate:<16} {buy_elg_amount:<18} {buy_elg_amount_rate:<14} {buy_lg_amount:<16} {buy_lg_amount_rate:<12} {buy_md_amount:<16} {buy_md_amount_rate:<12} {buy_sm_amount:<16} {buy_sm_amount_rate:<12} {max_stock:<20}")
    
    return "\n".join(result)


def format_single_concept_moneyflow_dc(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个板块的资金流向数据
    
    参数:
        df: 单个板块的资金流向数据DataFrame
        ts_code: 板块代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的资金流向数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    sector_name = str(df.iloc[0]['name']) if 'name' in df.columns and pd.notna(df.iloc[0]['name']) else ts_code
    content_type = str(df.iloc[0]['content_type']) if 'content_type' in df.columns and pd.notna(df.iloc[0]['content_type']) else ""
    result.append(f"💰 {ts_code} {sector_name} 资金流向数据")
    if content_type:
        result.append(f"📊 类型：{content_type}")
    result.append("=" * 180)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'涨跌幅':<10} {'最新指数':<12} {'主力净流入(元)':<18} {'主力净流入占比':<16} {'超大单净流入(元)':<18} {'超大单占比':<14} {'大单净流入(元)':<16} {'大单占比':<12} {'中单净流入(元)':<16} {'中单占比':<12} {'小单净流入(元)':<16} {'小单占比':<12} {'主力净流入最大股':<20} {'排名':<6}")
    result.append("-" * 180)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(str(row['trade_date']))
        pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
        close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
        net_amount = f"{row['net_amount']:.2f}" if 'net_amount' in row and pd.notna(row['net_amount']) else "-"
        net_amount_rate = f"{row['net_amount_rate']:+.2f}%" if 'net_amount_rate' in row and pd.notna(row['net_amount_rate']) else "-"
        buy_elg_amount = f"{row['buy_elg_amount']:.2f}" if 'buy_elg_amount' in row and pd.notna(row['buy_elg_amount']) else "-"
        buy_elg_amount_rate = f"{row['buy_elg_amount_rate']:+.2f}%" if 'buy_elg_amount_rate' in row and pd.notna(row['buy_elg_amount_rate']) else "-"
        buy_lg_amount = f"{row['buy_lg_amount']:.2f}" if 'buy_lg_amount' in row and pd.notna(row['buy_lg_amount']) else "-"
        buy_lg_amount_rate = f"{row['buy_lg_amount_rate']:+.2f}%" if 'buy_lg_amount_rate' in row and pd.notna(row['buy_lg_amount_rate']) else "-"
        buy_md_amount = f"{row['buy_md_amount']:.2f}" if 'buy_md_amount' in row and pd.notna(row['buy_md_amount']) else "-"
        buy_md_amount_rate = f"{row['buy_md_amount_rate']:+.2f}%" if 'buy_md_amount_rate' in row and pd.notna(row['buy_md_amount_rate']) else "-"
        buy_sm_amount = f"{row['buy_sm_amount']:.2f}" if 'buy_sm_amount' in row and pd.notna(row['buy_sm_amount']) else "-"
        buy_sm_amount_rate = f"{row['buy_sm_amount_rate']:+.2f}%" if 'buy_sm_amount_rate' in row and pd.notna(row['buy_sm_amount_rate']) else "-"
        max_stock = str(row['buy_sm_amount_stock'])[:18] if 'buy_sm_amount_stock' in row and pd.notna(row['buy_sm_amount_stock']) else "-"
        rank = f"{int(row['rank'])}" if 'rank' in row and pd.notna(row['rank']) else "-"
        
        result.append(f"{trade_date:<12} {pct_change:<10} {close:<12} {net_amount:<18} {net_amount_rate:<16} {buy_elg_amount:<18} {buy_elg_amount_rate:<14} {buy_lg_amount:<16} {buy_lg_amount_rate:<12} {buy_md_amount:<16} {buy_md_amount_rate:<12} {buy_sm_amount:<16} {buy_sm_amount_rate:<12} {max_stock:<20} {rank:<6}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 180)
        trade_date_str = str(latest.get('trade_date', '-'))
        result.append(f"交易日期: {format_date(trade_date_str)}")
        result.append(f"板块名称: {latest.get('name', '-')}")
        if 'content_type' in latest and pd.notna(latest['content_type']):
            result.append(f"资金类型: {latest['content_type']}")
        result.append(f"涨跌幅: {latest.get('pct_change', 0):+.2f}%" if pd.notna(latest.get('pct_change')) else "涨跌幅: -")
        result.append(f"最新指数: {latest.get('close', 0):.2f}" if pd.notna(latest.get('close')) else "最新指数: -")
        if 'rank' in latest and pd.notna(latest['rank']):
            result.append(f"排名: {int(latest['rank'])}")
        result.append("")
        result.append("资金流向：")
        result.append(f"  主力净流入: {latest.get('net_amount', 0):.2f} 元 ({latest.get('net_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('net_amount')) else "  主力净流入: -")
        result.append(f"  超大单净流入: {latest.get('buy_elg_amount', 0):.2f} 元 ({latest.get('buy_elg_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_elg_amount')) else "  超大单净流入: -")
        result.append(f"  大单净流入: {latest.get('buy_lg_amount', 0):.2f} 元 ({latest.get('buy_lg_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_lg_amount')) else "  大单净流入: -")
        result.append(f"  中单净流入: {latest.get('buy_md_amount', 0):.2f} 元 ({latest.get('buy_md_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_md_amount')) else "  中单净流入: -")
        result.append(f"  小单净流入: {latest.get('buy_sm_amount', 0):.2f} 元 ({latest.get('buy_sm_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_sm_amount')) else "  小单净流入: -")
        if 'buy_sm_amount_stock' in latest and pd.notna(latest['buy_sm_amount_stock']):
            result.append(f"  主力净流入最大股: {latest['buy_sm_amount_stock']}")
    
    return "\n".join(result)
