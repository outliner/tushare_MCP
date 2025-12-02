import akshare as ak
import sys

print("开始测试...")
try:
    df = ak.stock_board_concept_hist_em(symbol="绿色电力", period="daily", start_date="20220101", end_date="20250227", adjust="")
    print(f"成功！获取到 {len(df)} 条数据")
    print("\n前5条:")
    print(df.head(5))
    print("\n后5条:")
    print(df.tail(5))
except Exception as e:
    print(f"失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

