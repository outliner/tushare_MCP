"""测试东财概念板块成交量异动分析函数（调试版本）"""
import sys
import os

# 设置UTF-8编码（Windows兼容）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.concept_tools import analyze_concept_volume_anomaly, get_hot_concept_codes
from datetime import datetime
import pandas as pd

def test_single_concept_debug(concept_code: str, end_date: str = None):
    """调试单个概念板块的详细数据"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    
    print(f"\n{'='*80}")
    print(f"调试分析: {concept_code}")
    print(f"{'='*80}")
    
    # 先获取原始数据看看
    try:
        import tushare as ts
        from config.token_manager import get_tushare_token
        from cache.concept_cache_manager import concept_cache_manager
        from datetime import timedelta
        
        token = get_tushare_token()
        if not token:
            print("[ERROR] 未配置Tushare token")
            return
        
        pro = ts.pro_api()
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=60)).strftime('%Y%m%d')
        
        # 获取数据
        df = concept_cache_manager.get_concept_daily_data(
            ts_code=concept_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if df is None or df.empty:
            df = pro.dc_daily(ts_code=concept_code, start_date=start_date, end_date=end_date)
            if not df.empty:
                concept_cache_manager.save_concept_daily_data(df)
        
        if df.empty:
            print("[ERROR] 无法获取数据")
            return
        
        # 筛选并排序
        if 'ts_code' in df.columns:
            df = df[df['ts_code'] == concept_code].copy()
        
        if df.empty:
            print("[ERROR] 筛选后数据为空")
            return
        
        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
        
        print(f"\n数据概览:")
        print(f"  - 数据条数: {len(df)}")
        print(f"  - 日期范围: {df['trade_date'].iloc[-1]} 至 {df['trade_date'].iloc[0]}")
        
        if len(df) < 10:
            print("[WARNING] 数据不足10条，无法计算MA10")
            return
        
        # 显示最近10条数据
        print(f"\n最近10个交易日数据:")
        print(df[['trade_date', 'close', 'vol', 'turnover_rate', 'pct_change']].head(10).to_string())
        
        # 计算指标
        latest = df.iloc[0]
        current_price = latest.get('close', 0)
        
        vol_series = df['vol'].copy()
        ma3_vol = vol_series.head(3).mean()
        ma10_vol = vol_series.head(10).mean()
        vol_ratio = ma3_vol / ma10_vol if ma10_vol > 0 else 0
        
        price_5d_ago = df.iloc[5].get('close', 0) if len(df) >= 6 else 0
        price_change_5d = (current_price - price_5d_ago) / price_5d_ago if price_5d_ago > 0 else 0
        
        turnover_rate = latest.get('turnover_rate', 0)
        
        print(f"\n计算指标:")
        print(f"  - 当前价格: {current_price:.2f}")
        print(f"  - 5天前价格: {price_5d_ago:.2f}")
        print(f"  - 5日涨幅: {price_change_5d*100:.2f}%")
        print(f"  - MA3成交量: {ma3_vol:.0f}")
        print(f"  - MA10成交量: {ma10_vol:.0f}")
        print(f"  - 成交量比率: {vol_ratio:.2f} (MA3/MA10)")
        print(f"  - 换手率: {turnover_rate:.2f}%")
        
        print(f"\n筛选条件检查:")
        print(f"  - Volume_Ratio > 1.8: {vol_ratio > 1.8} (当前: {vol_ratio:.2f})")
        print(f"  - 2% < Price_Change_5d < 8%: {0.02 < price_change_5d < 0.08} (当前: {price_change_5d*100:.2f}%)")
        
        # 调用分析函数
        print(f"\n调用 analyze_concept_volume_anomaly 函数:")
        result = analyze_concept_volume_anomaly(
            concept_code=concept_code,
            end_date=end_date,
            vol_ratio_threshold=1.8,
            price_change_5d_min=0.02,
            price_change_5d_max=0.08
        )
        
        if result:
            print(f"[OK] 函数返回结果:")
            for key, value in result.items():
                print(f"  - {key}: {value}")
        else:
            print(f"[INFO] 函数返回 None（不符合筛选条件）")
        
    except Exception as e:
        print(f"[ERROR] 调试失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("\n" + "="*80)
    print("东财概念板块成交量异动分析函数调试测试")
    print("="*80)
    
    # 获取几个热门概念板块进行测试
    end_date = datetime.now().strftime('%Y%m%d')
    hot_codes = get_hot_concept_codes(end_date, limit=5)
    
    print(f"\n测试前5个热门概念板块:")
    for code in hot_codes:
        test_single_concept_debug(code, end_date)
        print("\n" + "-"*80 + "\n")

