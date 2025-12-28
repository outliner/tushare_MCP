"""
测试 _analyze_mainline_lifecycle 函数返回数据
测试日期: 20251209
"""
import sys
import json
import io
from pathlib import Path

# 设置输出编码为 UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts

# 初始化环境
init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)
    print(f"[OK] 已加载 Tushare token")
else:
    print("[WARN] 未找到 Tushare token")
    sys.exit(1)

# 导入函数
from tools.meso_scan_tools import _analyze_mainline_lifecycle

# 测试参数
test_date = "20251209"
top_n = 20
limit_up_threshold = 5

print("=" * 80)
print(f"测试 _analyze_mainline_lifecycle 函数")
print(f"测试日期: {test_date}")
print(f"Top N: {top_n}")
print(f"涨停阈值: {limit_up_threshold}")
print("=" * 80)
print()

# 调用函数
try:
    result = _analyze_mainline_lifecycle(test_date, top_n, limit_up_threshold)
    
    print("【返回结果概览】")
    print(f"success: {result.get('success')}")
    print(f"error: {result.get('error')}")
    print(f"concepts 数量: {len(result.get('concepts', []))}")
    print()
    
    if result.get("success") and result.get("concepts"):
        concepts = result.get("concepts")
        
        print("【数据结构示例 - 第一个概念板块】")
        print("-" * 80)
        first_concept = concepts[0]
        print(json.dumps(first_concept, ensure_ascii=False, indent=2))
        print()
        
        print("【所有概念板块的关键字段统计】")
        print("-" * 80)
        print(f"{'排名':<6} {'概念名称':<20} {'Alpha':<10} {'当前排名':<10} {'1日变化':<10} {'2日变化':<10} {'涨停数':<8} {'状态':<15}")
        print("-" * 80)
        
        for i, concept in enumerate(concepts, 1):
            name = concept.get("name", "")[:18]
            alpha = f"{concept.get('alpha', 0):.2f}%"
            current_rank = concept.get("current_rank", "N/A")
            rank_change_1d = concept.get("rank_change_1d", "N/A")
            rank_change_2d = concept.get("rank_change_2d", "N/A")
            limit_up = concept.get("limit_up_count", 0)
            status = concept.get("status", "")
            
            # 格式化排名变化
            if rank_change_1d is not None:
                rank_change_1d_str = f"{rank_change_1d:+d}"
            else:
                rank_change_1d_str = "N/A"
            
            if rank_change_2d is not None:
                rank_change_2d_str = f"{rank_change_2d:+d}"
            else:
                rank_change_2d_str = "N/A"
            
            print(f"{i:<6} {name:<20} {alpha:<10} {str(current_rank):<10} {rank_change_1d_str:<10} {rank_change_2d_str:<10} {limit_up:<8} {status:<15}")
        
        print()
        
        print("【数据完整性检查】")
        print("-" * 80)
        has_rank_change_1d = sum(1 for c in concepts if c.get("rank_change_1d") is not None)
        has_rank_change_2d = sum(1 for c in concepts if c.get("rank_change_2d") is not None)
        has_current_rank = sum(1 for c in concepts if c.get("current_rank") is not None)
        has_date_info = sum(1 for c in concepts if c.get("date_info") is not None)
        
        print(f"包含 rank_change_1d 的概念: {has_rank_change_1d}/{len(concepts)}")
        print(f"包含 rank_change_2d 的概念: {has_rank_change_2d}/{len(concepts)}")
        print(f"包含 current_rank 的概念: {has_current_rank}/{len(concepts)}")
        print(f"包含 date_info 的概念: {has_date_info}/{len(concepts)}")
        print()
        
        print("【完整数据结构（JSON格式）】")
        print("-" * 80)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        
    else:
        print(f"[FAIL] 测试失败: {result.get('error', '未知错误')}")
        
except Exception as e:
    import traceback
    print(f"[ERROR] 测试异常: {str(e)}")
    print("\n详细错误信息:")
    traceback.print_exc()

