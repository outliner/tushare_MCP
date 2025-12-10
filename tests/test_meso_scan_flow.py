"""
测试 meso_scan 完整流程
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

test_date = "20251209"

print("=" * 60)
print("测试 meso_scan 完整流程")
print("=" * 60)

# 模拟 _analyze_mainline_lifecycle 的流程
print("\n[1] 获取热门概念板块代码")
from tools.concept_tools import get_hot_concept_codes, rank_sectors_alpha
concept_codes = get_hot_concept_codes(test_date, limit=10)
print(f"  获取到 {len(concept_codes)} 个概念板块: {concept_codes[:3]}")

print("\n[2] 计算 Alpha 排名")
alpha_df = rank_sectors_alpha(concept_codes, "000300.SH", test_date)
print(f"  DataFrame 形状: {alpha_df.shape}")
print(f"  列名: {list(alpha_df.columns)}")
if not alpha_df.empty:
    print(f"\n  前3行 score 值:")
    for idx, row in alpha_df.head(3).iterrows():
        score = row.get('score', None)
        score_val = row['score'] if 'score' in row else None
        print(f"    row.get('score'): {score}, row['score']: {score_val}, pd.notna(score): {pd.notna(score) if score is not None else 'N/A'}")

print("\n[3] 获取板块名称")
pro = ts.pro_api()
if not alpha_df.empty:
    concept_codes_str = ','.join(alpha_df['sector_code'].head(5).tolist())
    name_df = pro.dc_index(ts_code=concept_codes_str, trade_date=test_date)
    name_map = {}
    if not name_df.empty:
        for _, row in name_df.iterrows():
            name_map[row['ts_code']] = row.get('name', row['ts_code'])
    alpha_df['name'] = alpha_df['sector_code'].map(name_map).fillna(alpha_df['sector_code'])
    print(f"  成功映射板块名称")
    print(f"  示例: {alpha_df[['sector_code', 'name', 'score']].head(3)}")

print("\n[4] 测试 Alpha 值提取")
if not alpha_df.empty:
    print("  测试提取逻辑:")
    for idx, row in alpha_df.head(3).iterrows():
        # 模拟第216行的逻辑
        score_val = row.get('score', None)
        alpha_val = row['score'] * 100 if pd.notna(row.get('score')) else 0
        print(f"    sector_code: {row['sector_code']}")
        print(f"      score (row.get): {score_val}")
        print(f"      score (row['score']): {row['score'] if 'score' in row else 'N/A'}")
        print(f"      pd.notna(row.get('score')): {pd.notna(row.get('score'))}")
        print(f"      alpha (计算后): {alpha_val}")

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)

