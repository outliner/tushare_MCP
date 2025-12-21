"""测试创建T-1表"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from newfunction.create_t1_tables import create_t1_tables, verify_tables

if __name__ == '__main__':
    try:
        print("=" * 60)
        print("开始创建T-1预计算数据表")
        print("=" * 60)
        create_t1_tables()
        print()
        verify_tables()
        print("=" * 60)
        print("✅ 所有操作完成！")
        print("=" * 60)
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

