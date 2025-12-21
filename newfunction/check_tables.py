"""检查T-1表是否创建成功"""
import sqlite3
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB

conn = sqlite3.connect(CACHE_DB)
cursor = conn.cursor()

table_name = 't1_data'

print("=" * 70)
print("检查T-1数据表")
print("=" * 70)
print(f"数据库路径: {CACHE_DB}")
print(f"数据库文件存在: {CACHE_DB.exists()}")
print()

cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
exists = cursor.fetchone() is not None

if exists:
    cursor.execute(f'PRAGMA table_info({table_name})')
    columns = cursor.fetchall()
    print(f"✅ {table_name}")
    print(f"   列数: {len(columns)}")
    print()
    print("   字段详情：")
    
    # 按类别分组显示字段
    field_categories = {
        "基本信息": ['ts_code', 'float_share', 'total_mv'],
        "机构数据": ['sum_inst_net', 'list_count'],
        "筹码数据": ['winner_rate', 'cost_concentration'],
        "融资融券": ['margin_cap_ratio'],
        "日线统计": ['pre_close', 'pre_vol', 'pre_ats'],
        "元数据": ['updated_at']
    }
    
    for category, field_names in field_categories.items():
        print(f"\n   📋 {category}:")
        for col in columns:
            col_name, col_type = col[1], col[2]
            if col_name in field_names:
                nullable = "" if col[3] == 0 else " (可空)"
                pk = " [主键]" if col[5] == 1 else ""
                print(f"      - {col_name}: {col_type}{nullable}{pk}")
else:
    print(f"❌ {table_name}: 未找到")
    print()
    print("请运行 create_t1_tables.py 创建表")

conn.close()

print()
print("=" * 70)
if exists:
    print("✅ T-1数据表已创建成功！")
else:
    print("⚠️  T-1数据表未创建，请运行 create_t1_tables.py")
print("=" * 70)

