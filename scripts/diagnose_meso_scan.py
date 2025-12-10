"""
诊断 meso_scan 数据获取问题
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts
import pandas as pd

init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)
    print("[OK] Token loaded\n", flush=True)
else:
    print("[ERROR] Token not found", flush=True)
    sys.exit(1)

test_date = "20251209"

print("=" * 60)
print("诊断 meso_scan 数据获取问题")
print("=" * 60)

# 1. 测试获取热门概念板块代码
print("\n【1. 测试获取热门概念板块代码】")
try:
    from tools.concept_tools import get_hot_concept_codes
    concept_codes = get_hot_concept_codes(test_date, limit=10)
    print(f"[OK] Got {len(concept_codes)} concept codes")
    print(f"  示例: {concept_codes[:3]}")
except Exception as e:
    print(f"[ERROR] Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 2. 测试 Alpha 排名
print("\n【2. 测试 Alpha 排名】")
try:
    from tools.alpha_strategy_analyzer import rank_sectors_alpha
    alpha_df = rank_sectors_alpha(concept_codes[:5], "000300.SH", test_date)
    print(f"✓ 成功获取 Alpha 排名数据")
    print(f"  DataFrame 形状: {alpha_df.shape}")
    print(f"  列名: {list(alpha_df.columns)}")
    if not alpha_df.empty:
        print(f"\n  前3行数据:")
        print(alpha_df[['sector_code', 'score', 'alpha_2', 'alpha_5']].head(3))
        print(f"\n  Score 统计:")
        print(f"    非空数量: {alpha_df['score'].notna().sum()}")
        print(f"    空值数量: {alpha_df['score'].isna().sum()}")
        if alpha_df['score'].notna().any():
            print(f"    Score 范围: {alpha_df['score'].min():.4f} ~ {alpha_df['score'].max():.4f}")
    else:
        print("  ⚠️ DataFrame 为空")
except Exception as e:
    print(f"[ERROR] Failed: {e}")
    import traceback
    traceback.print_exc()

# 3. 测试获取板块名称
print("\n【3. 测试获取板块名称】")
try:
    pro = ts.pro_api()
    if not alpha_df.empty:
        concept_codes_str = ','.join(alpha_df['sector_code'].tolist()[:5])
        name_df = pro.dc_index(ts_code=concept_codes_str, trade_date=test_date)
        print(f"✓ 成功获取板块名称")
        print(f"  获取到 {len(name_df)} 个板块名称")
        if not name_df.empty:
            print(f"  示例:")
            for _, row in name_df.head(3).iterrows():
                print(f"    {row.get('ts_code', 'N/A')}: {row.get('name', 'N/A')}")
    else:
        print("  ⚠️ 跳过（Alpha DataFrame 为空）")
except Exception as e:
    print(f"[ERROR] Failed: {e}")
    import traceback
    traceback.print_exc()

# 4. 测试排名变化
print("\n【4. 测试排名变化计算】")
try:
    from tools.alpha_strategy_analyzer import calculate_alpha_rank_velocity
    velocity_df = calculate_alpha_rank_velocity(concept_codes[:10], "000300.SH", test_date)
    print(f"✓ 成功计算排名变化")
    print(f"  DataFrame 形状: {velocity_df.shape}")
    if not velocity_df.empty:
        print(f"  列名: {list(velocity_df.columns)}")
        print(f"\n  前3行数据:")
        print(velocity_df.head(3))
    else:
        print("  ⚠️ DataFrame 为空（可能是历史数据不足）")
except Exception as e:
    print(f"[ERROR] Failed: {e}")
    import traceback
    traceback.print_exc()

# 5. 测试涨停梯队
print("\n【5. 测试涨停梯队数据】")
try:
    pro = ts.pro_api()
    limit_df = pro.limit_cpt_list(trade_date=test_date)
    if limit_df is not None and not limit_df.empty:
        print(f"✓ 成功获取涨停梯队数据")
        print(f"  获取到 {len(limit_df)} 个概念板块")
        print(f"  列名: {list(limit_df.columns)}")
        print(f"\n  前3行数据:")
        print(limit_df[['name', 'limit_up_num']].head(3))
    else:
        print("  ⚠️ 未获取到涨停梯队数据")
except Exception as e:
    print(f"[ERROR] Failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("诊断完成")
print("=" * 60)

