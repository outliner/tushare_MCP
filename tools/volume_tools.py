"""成交量异动分析相关MCP工具"""
import json
import sys
import pandas as pd
import numpy as np
import tushare as ts
from typing import TYPE_CHECKING, Dict, List, Optional
from datetime import datetime, timedelta
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager

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

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

def get_l2_sector_codes() -> List[str]:
    """获取申万二级行业代码列表"""
    return [
        "801012.SI", "801014.SI", "801015.SI", "801016.SI", "801017.SI", "801018.SI",
        "801032.SI", "801033.SI", "801034.SI", "801036.SI", "801037.SI", "801038.SI", "801039.SI",
        "801043.SI", "801044.SI", "801045.SI",
        "801051.SI", "801053.SI", "801054.SI", "801055.SI", "801056.SI",
        "801072.SI", "801074.SI", "801076.SI", "801077.SI", "801078.SI",
        "801081.SI", "801082.SI", "801083.SI", "801084.SI", "801085.SI", "801086.SI",
        "801092.SI", "801093.SI", "801095.SI", "801096.SI",
        "801101.SI", "801102.SI", "801103.SI", "801104.SI",
        "801111.SI", "801112.SI", "801113.SI", "801114.SI", "801115.SI", "801116.SI",
        "801124.SI", "801125.SI", "801126.SI", "801127.SI", "801128.SI", "801129.SI",
        "801131.SI", "801132.SI", "801133.SI",
        "801141.SI", "801142.SI", "801143.SI", "801145.SI",
        "801151.SI", "801152.SI", "801153.SI", "801154.SI", "801155.SI", "801156.SI",
        "801161.SI", "801163.SI",
        "801178.SI", "801179.SI",
        "801181.SI", "801183.SI",
        "801191.SI", "801193.SI", "801194.SI",
        "801202.SI", "801203.SI", "801204.SI", "801206.SI",
        "801218.SI", "801219.SI",
        "801223.SI",
        "801231.SI",
        "801711.SI", "801712.SI", "801713.SI",
        "801721.SI", "801722.SI", "801723.SI", "801724.SI", "801726.SI",
        "801731.SI", "801733.SI", "801735.SI", "801736.SI", "801737.SI", "801738.SI",
        "801741.SI", "801742.SI", "801743.SI", "801744.SI", "801745.SI",
        "801764.SI", "801765.SI", "801766.SI", "801767.SI", "801769.SI",
        "801782.SI", "801783.SI", "801784.SI", "801785.SI",
        "801881.SI",
        "801951.SI", "801952.SI",
        "801962.SI", "801963.SI",
        "801971.SI", "801972.SI",
        "801981.SI", "801982.SI",
        "801991.SI", "801992.SI", "801993.SI", "801994.SI", "801995.SI"
    ]

def get_sector_name_map(level: str = "L2") -> Dict[str, str]:
    """
    获取行业代码到名称的映射
    
    参数:
        level: 行业级别（L1、L2、L3）
    
    返回:
        字典，key为行业代码，value为行业名称
    """
    token = get_tushare_token()
    if not token:
        return {}
    
    try:
        # 尝试从缓存获取
        cache_params = {'level': level, 'src': 'SW2021'}
        df = cache_manager.get_dataframe('index_classify', **cache_params)
        
        # 检查是否需要更新
        need_update = False
        if df is None:
            need_update = True
        elif cache_manager.is_expired('index_classify', **cache_params):
            need_update = True
        
        if need_update:
            pro = ts.pro_api()
            df = pro.index_classify(level=level, src='SW2021')
            if not df.empty:
                cache_manager.set('index_classify', df, **cache_params)
        
        if df.empty:
            return {}
        
        # 创建映射字典
        name_map = {}
        for _, row in df.iterrows():
            index_code = str(row.get('index_code', ''))
            industry_name = str(row.get('industry_name', ''))
            if index_code and industry_name:
                name_map[index_code] = industry_name
        
        return name_map
    
    except Exception as e:
        print(f"获取行业名称映射失败: {str(e)}", file=sys.stderr)
        return {}

def analyze_volume_anomaly(
    sector_code: str,
    end_date: str = None,
    vol_ma_short: int = 3,
    vol_ma_long: int = 10,
    vol_ratio_threshold: float = 1.1,
    price_change_5d_threshold: float = 0.03
) -> Optional[Dict]:
    """
    分析单个行业的成交量异动
    
    参数:
        sector_code: 行业代码
        end_date: 结束日期（YYYYMMDD格式，默认今天）
        vol_ma_short: 成交量短期MA天数（默认3，即MA3）
        vol_ma_long: 成交量长期MA天数（默认10，即MA10）
        vol_ratio_threshold: 成交量比率阈值（默认1.1，即MA短/MA长 > 1.1）
        price_change_5d_threshold: 5日涨幅阈值（默认0.03，即3%）
    
    返回:
        如果匹配条件，返回包含分析结果的字典；否则返回None
    """
    token = get_tushare_token()
    if not token:
        return None
    
    if end_date is None or end_date == "":
        end_date = datetime.now().strftime('%Y%m%d')
    
    try:
        # 获取至少60天的数据（用于计算均线和近1个月价格区间）
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=60)).strftime('%Y%m%d')
        
        # 获取申万行业数据
        cache_params = {
            'ts_code': sector_code,
            'level': 'L2',
            'start_date': start_date,
            'end_date': end_date
        }
        df = cache_manager.get_dataframe('sw_industry_daily', **cache_params)
        
        if df is None or df.empty:
            # 从API获取
            pro = ts.pro_api()
            df = pro.sw_daily(ts_code=sector_code, level='L2', start_date=start_date, end_date=end_date)
            if not df.empty:
                cache_manager.set('sw_industry_daily', df, **cache_params)
        
        if df.empty:
            return None
        
        # 筛选指定指数的数据
        if 'ts_code' in df.columns:
            df = df[df['ts_code'] == sector_code].copy()
        elif 'index_code' in df.columns:
            df = df[df['index_code'] == sector_code].copy()
            if 'ts_code' not in df.columns:
                df['ts_code'] = df['index_code']
        
        if df.empty:
            return None
        
        # 按日期排序（最新的在前）
        df = df.sort_values('trade_date', ascending=False)
        
        # 检查是否有足够的数据（至少需要22个交易日用于计算近1个月价格区间）
        if len(df) < 22:
            return None
        
        # 获取最新数据
        latest = df.iloc[0]
        current_price = latest.get('close') or latest.get('index', 0)
        
        if pd.isna(current_price) or current_price == 0:
            return None
        
        # 计算成交量MA（使用可配置的天数）
        vol_series = df['vol'].copy()
        max_ma = max(vol_ma_short, vol_ma_long)
        if vol_series.isna().all() or len(vol_series) < max_ma:
            return None
        
        # 计算移动平均
        ma_short = vol_series.head(vol_ma_short).mean()
        ma_long = vol_series.head(vol_ma_long).mean()
        
        if pd.isna(ma_short) or pd.isna(ma_long) or ma_long == 0:
            return None
        
        vol_ratio = ma_short / ma_long
        
        # 计算5日涨幅
        if len(df) < 6:
            return None
        
        price_5d_ago = df.iloc[5].get('close') or df.iloc[5].get('index', 0)
        if pd.isna(price_5d_ago) or price_5d_ago == 0:
            return None
        
        price_change_5d = (current_price - price_5d_ago) / price_5d_ago
        
        # 检查是否匹配基本条件
        if not (vol_ratio > vol_ratio_threshold and price_change_5d < price_change_5d_threshold):
            return None
        
        # 计算价格位置指标
        price_series = df['close'] if 'close' in df.columns else df['index']
        
        # 计算20日均线和60日均线
        ma20 = price_series.head(20).mean() if len(price_series) >= 20 else None
        ma60 = price_series.head(60).mean() if len(price_series) >= 60 else None
        
        # 计算近1个月（22个交易日）的价格区间
        price_1m = price_series.head(22) if len(price_series) >= 22 else price_series
        price_high_1m = price_1m.max()
        price_low_1m = price_1m.min()
        price_range_1m = price_high_1m - price_low_1m
        
        # 计算当前价格在近1个月价格区间中的位置（0-1之间，0为最低，1为最高）
        if price_range_1m > 0:
            price_position = (current_price - price_low_1m) / price_range_1m
        else:
            price_position = 0.5  # 如果价格没有波动，默认为中间位置
        
        # 判断价格位置（确保返回Python原生bool类型，而非numpy bool）
        is_above_ma20 = bool(ma20 is not None and current_price > ma20)
        is_above_ma60 = bool(ma60 is not None and current_price > ma60)
        is_low_position = bool(price_position < 0.3)  # 处于近1个月低位（低于30%分位）
        is_high_position = bool(price_position > 0.7)  # 处于近1个月高位（高于70%分位）
        
        # 判断信号类型
        # 潜伏信号：价格在均线之上 或 处于长期低位
        # 出货信号：价格处于高位
        if is_high_position:
            signal_type = "Distribution (出货)"
            signal_reason = f"价格处于近1个月高位（{price_position*100:.1f}%分位），成交量放大但价格下跌，可能是出货信号"
        elif is_above_ma20 or is_above_ma60 or is_low_position:
            signal_type = "Accumulation (潜伏)"
            signal_reason = f"价格在均线之上或处于低位（{price_position*100:.1f}%分位），成交量放大但价格涨幅较小，可能是建仓信号"
        else:
            # 中等位置，需要更多信息判断
            signal_type = "Uncertain (待确认)"
            signal_reason = f"价格处于中等位置（{price_position*100:.1f}%分位），需要结合其他指标判断"
        
        return {
            'code': sector_code,
            'vol_ratio': round(vol_ratio, 2),
            'vol_ma_short': vol_ma_short,
            'vol_ma_long': vol_ma_long,
            'price_change_5d': round(price_change_5d, 4),
            'current_price': round(current_price, 2),
            'signal_type': signal_type,
            'price_position': round(price_position, 3),
            'ma20': round(ma20, 2) if ma20 is not None else None,
            'ma60': round(ma60, 2) if ma60 is not None else None,
            'is_above_ma20': is_above_ma20,
            'is_above_ma60': is_above_ma60,
            'signal_reason': signal_reason
        }
    
    except Exception as e:
        print(f"分析 {sector_code} 失败: {str(e)}", file=sys.stderr)
        return None

def scan_volume_anomaly(
    end_date: str = None,
    vol_ma_short: int = 3,
    vol_ma_long: int = 10,
    vol_ratio_threshold: float = 1.5,
    price_change_5d_threshold: float = 0.03
) -> Dict:
    """
    扫描所有申万二级行业的成交量异动
    
    参数:
        end_date: 结束日期（YYYYMMDD格式，默认今天）
        vol_ma_short: 成交量短期MA天数（默认3，即MA3）
        vol_ma_long: 成交量长期MA天数（默认10，即MA10）
        vol_ratio_threshold: 成交量比率阈值（默认1.5，即MA短/MA长 > 1.5）
        price_change_5d_threshold: 5日涨幅阈值（默认0.03，即3%）
    
    返回:
        包含扫描结果的字典
    """
    sector_codes = get_l2_sector_codes()
    name_map = get_sector_name_map(level='L2')
    
    matches = []
    
    for sector_code in sector_codes:
        result = analyze_volume_anomaly(
            sector_code,
            end_date,
            vol_ma_short,
            vol_ma_long,
            vol_ratio_threshold,
            price_change_5d_threshold
        )
        
        if result:
            # 获取行业名称
            sector_name = name_map.get(sector_code, sector_code)
            
            # 构建匹配结果（确保所有值都可以JSON序列化）
            match = {
                "code": result['code'],
                "name": sector_name,
                "signal_type": result.get('signal_type', 'Uncertain (待确认)'),
                "metrics": {
                    "vol_ratio": result['vol_ratio'],
                    "vol_ma_short": result.get('vol_ma_short', vol_ma_short),
                    "vol_ma_long": result.get('vol_ma_long', vol_ma_long),
                    "price_change_5d": result['price_change_5d'],
                    "current_price": result['current_price'],
                    "price_position": result.get('price_position', 0.5),
                    "ma20": result.get('ma20'),
                    "ma60": result.get('ma60'),
                    "is_above_ma20": bool(result.get('is_above_ma20', False)),  # 确保是Python bool，JSON可以序列化
                    "is_above_ma60": bool(result.get('is_above_ma60', False))   # 确保是Python bool，JSON可以序列化
                },
                "reasoning": result.get('signal_reason', f"MA{result.get('vol_ma_short', vol_ma_short)}/MA{result.get('vol_ma_long', vol_ma_long)} Volume is {result['vol_ratio']} (>{vol_ratio_threshold}), but 5-day price change is only {result['price_change_5d']*100:.2f}% (<{price_change_5d_threshold*100:.0f}%)")
            }
            matches.append(match)
    
    return {
        "scanned_count": len(sector_codes),
        "matched_count": len(matches),
        "matches": matches
    }

def register_volume_tools(mcp: "FastMCP"):
    """注册成交量异动分析工具"""
    import sys
    
    @mcp.tool()
    def scan_l2_volume_anomaly(
        end_date: str = "",
        vol_ma_short: int = 3,
        vol_ma_long: int = 10,
        vol_ratio_threshold: float = 1.5,
        price_change_5d_threshold: float = 0.03
    ) -> str:
        """
        分析申万二级行业成交量异动
        
        参数:
            end_date: 结束日期（YYYYMMDD格式，默认今天）
            vol_ma_short: 成交量短期MA天数（默认3，即MA3，可设置为1、3、5等）
            vol_ma_long: 成交量长期MA天数（默认10，即MA10，可设置为3、5、10、20等）
            vol_ratio_threshold: 成交量比率阈值（默认1.5，即MA短/MA长 > 1.5）
            price_change_5d_threshold: 5日涨幅阈值（默认0.03，即3%）
        
        返回:
            JSON格式字符串，包含扫描结果
        
        说明:
            - 扫描所有申万二级行业
            - 筛选条件：(成交量MA短/成交量MA长) > 阈值 且 近5日涨幅 < 阈值
            - 示例：vol_ma_short=1, vol_ma_long=3 表示计算MA1/MA3的成交量比率
            - 根据价格位置判断信号类型：
              * 高位（>70%分位）：出货信号（Distribution）
              * 低位（<30%分位）或均线之上：潜伏信号（Accumulation）
              * 其他：待确认（Uncertain）
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
            
            # 验证MA参数
            if vol_ma_short <= 0 or vol_ma_long <= 0:
                return json.dumps({
                    "error": "MA天数必须大于0",
                    "scanned_count": 0,
                    "matched_count": 0,
                    "matches": []
                }, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
            if vol_ma_short >= vol_ma_long:
                return json.dumps({
                    "error": "短期MA天数必须小于长期MA天数",
                    "scanned_count": 0,
                    "matched_count": 0,
                    "matches": []
                }, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
            # 扫描成交量异动
            result = scan_volume_anomaly(
                end_date=end_date,
                vol_ma_short=vol_ma_short,
                vol_ma_long=vol_ma_long,
                vol_ratio_threshold=vol_ratio_threshold,
                price_change_5d_threshold=price_change_5d_threshold
            )
            
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

