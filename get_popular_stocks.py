"""
获取"其他电源设备"二级行业的热门股票
"""
import sys
from datetime import datetime
from market_data_analyzer import get_popular_stocks_in_industry

def main():
    industry_name = "其他电源设备"
    top_n = 20  # 获取前20只热门股票
    
    print(f"正在获取【{industry_name}】行业的热门股票（按今日成交额排名）...")
    print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        result = get_popular_stocks_in_industry(industry_name, top_n)
        
        if result.empty:
            print(f"未找到【{industry_name}】行业的股票数据。")
            return
        
        print(f"【{industry_name}】行业热门股票（前{len(result)}只）:\n")
        print("=" * 80)
        print(f"{'排名':<6} {'股票代码':<12} {'股票名称':<20} {'成交额(万元)':<15}")
        print("=" * 80)
        
        for idx, row in result.iterrows():
            rank = result.index.get_loc(idx) + 1
            ts_code = row['ts_code']
            name = row['name']
            trade_amount = row['trade_amount'] / 10000  # 转换为万元
            
            print(f"{rank:<6} {ts_code:<12} {name:<20} {trade_amount:>15.2f}")
        
        print("=" * 80)
        print(f"\n共找到 {len(result)} 只股票")
        
    except Exception as e:
        print(f"获取数据时出错: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
















