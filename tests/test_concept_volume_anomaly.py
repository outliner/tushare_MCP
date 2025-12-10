"""测试东财概念板块成交量异动分析函数"""
import sys
import os

# 设置UTF-8编码（Windows兼容）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.concept_tools import scan_concept_volume_anomaly, analyze_concept_volume_anomaly
from datetime import datetime

def test_scan_concept_volume_anomaly():
    """测试扫描函数"""
    print("=" * 80)
    print("测试 scan_concept_volume_anomaly 函数")
    print("=" * 80)
    
    # 测试参数
    end_date = datetime.now().strftime('%Y%m%d')
    vol_ratio_threshold = 1.8
    price_change_5d_min = 0.02
    price_change_5d_max = 0.08
    hot_limit = 10  # 只测试前10个热门概念板块
    
    print(f"\n测试参数:")
    print(f"  - 结束日期: {end_date}")
    print(f"  - 成交量比率阈值: {vol_ratio_threshold}")
    print(f"  - 5日涨幅范围: {price_change_5d_min*100:.0f}% - {price_change_5d_max*100:.0f}%")
    print(f"  - 扫描数量: {hot_limit} 个热门概念板块")
    print()
    
    try:
        result = scan_concept_volume_anomaly(
            end_date=end_date,
            vol_ratio_threshold=vol_ratio_threshold,
            price_change_5d_min=price_change_5d_min,
            price_change_5d_max=price_change_5d_max,
            hot_limit=hot_limit
        )
        
        print(f"[OK] 扫描完成")
        print(f"  - 扫描数量: {result['scanned_count']}")
        print(f"  - 匹配数量: {result['matched_count']}")
        print()
        
        if result['matched_count'] > 0:
            print(f"[发现] 发现 {result['matched_count']} 个量价异动概念板块:")
            print("-" * 80)
            for i, match in enumerate(result['matches'], 1):
                print(f"\n{i}. {match['name']} ({match['code']})")
                print(f"   信号类型: {match['signal_type']}")
                print(f"   成交量比率: {match['metrics']['vol_ratio']} (MA3/MA10)")
                print(f"   5日涨幅: {match['metrics']['price_change_5d']*100:.2f}%")
                print(f"   换手率: {match['metrics']['turnover_rate']:.2f}%")
                print(f"   当前价格: {match['metrics']['current_price']:.2f}")
                print(f"   价格位置: {match['metrics']['price_position']*100:.1f}%分位")
                print(f"   判断理由: {match['reasoning']}")
        else:
            print("[INFO] 未发现符合条件的量价异动概念板块")
            print("   可能原因:")
            print("   1. 当前市场没有符合筛选条件的板块")
            print("   2. 筛选条件过于严格")
            print("   3. 数据获取失败")
        
        return result
        
    except Exception as e:
        print(f"[ERROR] 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def test_analyze_single_concept():
    """测试单个概念板块分析"""
    print("\n" + "=" * 80)
    print("测试 analyze_concept_volume_anomaly 函数（单个概念板块）")
    print("=" * 80)
    
    # 测试一个热门概念板块
    test_code = "BK0578.DC"  # 稀土永磁
    end_date = datetime.now().strftime('%Y%m%d')
    
    print(f"\n测试概念板块: {test_code}")
    print(f"测试日期: {end_date}")
    print()
    
    try:
        result = analyze_concept_volume_anomaly(
            concept_code=test_code,
            end_date=end_date,
            vol_ratio_threshold=1.8,
            price_change_5d_min=0.02,
            price_change_5d_max=0.08
        )
        
        if result:
            print(f"[OK] 发现量价异动!")
            print(f"   成交量比率: {result['vol_ratio']} (MA3/MA10)")
            print(f"   5日涨幅: {result['price_change_5d']*100:.2f}%")
            print(f"   换手率: {result['turnover_rate']:.2f}%")
            print(f"   信号类型: {result['signal_type']}")
            print(f"   判断理由: {result['signal_reason']}")
        else:
            print(f"[INFO] 该概念板块不符合筛选条件")
            print(f"   可能原因:")
            print(f"   1. 成交量比率 <= 1.8")
            print(f"   2. 5日涨幅不在 2%-8% 范围内")
            print(f"   3. 数据不足")
        
        return result
        
    except Exception as e:
        print(f"[ERROR] 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("东财概念板块成交量异动分析函数测试")
    print("=" * 80)
    
    # 测试1: 扫描多个概念板块
    result1 = test_scan_concept_volume_anomaly()
    
    # 测试2: 分析单个概念板块
    result2 = test_analyze_single_concept()
    
    print("\n" + "=" * 80)
    print("测试完成")
    print("=" * 80)

