"""验证T-1表是否创建成功"""
import sqlite3
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB

conn = sqlite3.connect(CACHE_DB)
cursor = conn.cursor()

expected_tables = ['basic_info', 'inst_data', 'cyq_data', 'margin_data', 'daily_stats']

print("检查T-1数据表...")
print(f"数据库路径: {CACHE_DB}")
print("-" * 60)

for table in expected_tables:
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
    if cursor.fetchone():
        cursor.execute(f'PRAGMA table_info({table})')
        columns = cursor.fetchall()
        print(f"✅ {table}: {len(columns)} 列")
        for col in columns:
            col_name, col_type = col[1], col[2]
            nullable = "NULL" if col[3] == 0 else "NOT NULL"
            pk = " (主键)" if col[5] == 1 else ""
            print(f"   - {col_name}: {col_type} {nullable}{pk}")
    else:
        print(f"❌ {table}: 未找到")

conn.close()

