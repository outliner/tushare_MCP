"""测试东方财富概念板块历史行情接口"""
import akshare as ak
import time
import sys

def test_concept_hist():
    """测试 stock_board_concept_hist_em 接口"""
    print("=" * 80)
    print("测试 stock_board_concept_hist_em 接口")
    print("=" * 80)
    print()
    
    # 测试参数
    symbol = "绿色电力"
    period = "daily"
    start_date = "20220101"
    end_date = "20250227"
    adjust = ""
    
    print(f"测试参数:")
    print(f"  - symbol: {symbol}")
    print(f"  - period: {period}")
    print(f"  - start_date: {start_date}")
    print(f"  - end_date: {end_date}")
    print(f"  - adjust: {adjust}")
    print()
    
    # 尝试多次重试
    max_retries = 3
    retry_delay = 2  # 秒
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"尝试第 {attempt} 次调用...")
            print("-" * 80)
            
            # 调用接口
            df = ak.stock_board_concept_hist_em(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            # 检查结果
            if df is None:
                print("❌ 接口返回 None")
                return False
            
            if df.empty:
                print("⚠️  接口返回空数据")
                return False
            
            # 显示成功信息
            print(f"✅ 接口调用成功！")
            print(f"   获取到 {len(df)} 条数据")
            print()
            
            # 显示数据信息
            print("数据列名:")
            print(f"  {list(df.columns)}")
            print()
            
            # 显示前5条数据
            print("前5条数据:")
            print(df.head(5).to_string())
            print()
            
            # 显示后5条数据
            print("后5条数据:")
            print(df.tail(5).to_string())
            print()
            
            # 显示数据统计
            print("数据统计:")
            print(f"  - 总记录数: {len(df)}")
            print(f"  - 日期范围: {df.iloc[-1]['日期']} 至 {df.iloc[0]['日期']}")
            if '收盘' in df.columns:
                print(f"  - 最新收盘价: {df.iloc[0]['收盘']:.2f}")
                print(f"  - 最早收盘价: {df.iloc[-1]['收盘']:.2f}")
            print()
            
            return True
            
        except ConnectionError as e:
            print(f"❌ 连接错误 (尝试 {attempt}/{max_retries}):")
            print(f"   {str(e)}")
            if attempt < max_retries:
                print(f"   等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print("   已达到最大重试次数")
                return False
                
        except Exception as e:
            print(f"❌ 发生错误 (尝试 {attempt}/{max_retries}):")
            print(f"   错误类型: {type(e).__name__}")
            print(f"   错误信息: {str(e)}")
            import traceback
            print(f"   详细错误:")
            traceback.print_exc()
            if attempt < max_retries:
                print(f"   等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print("   已达到最大重试次数")
                return False
    
    return False

if __name__ == "__main__":
    success = test_concept_hist()
    sys.exit(0 if success else 1)

