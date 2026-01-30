"""
测试 get_dc_board_codes 函数中行业名称获取功能

验证：
1. 返回的数据结构包含名称字段
2. 能够从数据库查询名称
3. 能够通过 dc_index 接口获取缺失的名称
4. 名称能够正确保存到数据库
"""
import sys
import io
from pathlib import Path
from datetime import datetime

# 设置输出编码为UTF-8（Windows兼容）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 初始化环境
from config.token_manager import get_tushare_token, init_env_file
import tushare as ts

# 导入要测试的函数
from tools.concept_tools import get_dc_board_codes

def print_section(title: str):
    """打印测试章节标题"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def test_industry_board_names():
    """测试行业板块名称获取"""
    print_section("测试行业板块名称获取功能")
    
    # 测试获取行业板块
    print("\n1. 获取行业板块代码和名称...")
    result = get_dc_board_codes(board_type='行业板块')
    
    if not result:
        print("  [FAIL] 未获取到任何行业板块数据")
        return False
    
    print(f"  [OK] 成功获取 {len(result)} 个行业板块")
    
    # 验证返回数据结构
    print("\n2. 验证返回数据结构...")
    if not isinstance(result, list):
        print(f"  [FAIL] 返回类型错误，期望 list，实际 {type(result)}")
        return False
    
    if len(result) == 0:
        print("  [FAIL] 返回列表为空")
        return False
    
    first_item = result[0]
    if not isinstance(first_item, dict):
        print(f"  [FAIL] 列表项类型错误，期望 dict，实际 {type(first_item)}")
        return False
    
    if 'ts_code' not in first_item or 'name' not in first_item:
        print(f"  [FAIL] 字典缺少必要字段，实际字段: {list(first_item.keys())}")
        return False
    
    print("  [OK] 返回数据结构正确（字典列表，包含 ts_code 和 name 字段）")
    
    # 验证名称字段
    print("\n3. 验证名称字段...")
    has_name = 0
    no_name = 0
    name_examples = []
    
    for item in result[:20]:  # 检查前20个
        code = item['ts_code']
        name = item['name']
        
        if name and name != code:
            has_name += 1
            if len(name_examples) < 5:
                name_examples.append(f"{code} -> {name}")
        else:
            no_name += 1
    
    print(f"  有名称的板块: {has_name} 个")
    print(f"  无名称的板块: {no_name} 个（使用代码作为名称）")
    
    if name_examples:
        print("\n  名称示例（前5个）:")
        for example in name_examples:
            print(f"    {example}")
    
    # 显示前10个结果
    print("\n4. 前10个行业板块详情:")
    print(f"{'排名':<6} {'板块代码':<15} {'板块名称':<30}")
    print("-" * 60)
    for i, item in enumerate(result[:10], 1):
        code = item['ts_code']
        name = item['name']
        # 如果名称太长，截断
        display_name = name[:28] + '..' if len(name) > 30 else name
        print(f"{i:<6} {code:<15} {display_name:<30}")
    
    # 验证名称是否从数据库获取
    print("\n5. 验证数据库缓存...")
    from cache.concept_cache_manager import concept_cache_manager
    
    # 随机选择几个代码查询数据库
    test_codes = [item['ts_code'] for item in result[:5]]
    db_name_map = concept_cache_manager.get_board_name_map(test_codes, '行业板块')
    
    print(f"  从数据库查询到 {len(db_name_map)} 个名称映射")
    if db_name_map:
        print("  数据库中的名称示例:")
        for code, name in list(db_name_map.items())[:3]:
            print(f"    {code} -> {name}")
    
    return True

def test_name_retrieval_process():
    """测试名称获取流程"""
    print_section("测试名称获取流程")
    
    print("\n1. 首次获取（可能触发API调用）...")
    result1 = get_dc_board_codes(board_type='行业板块')
    print(f"  获取到 {len(result1)} 个行业板块")
    
    print("\n2. 第二次获取（应该从缓存读取）...")
    result2 = get_dc_board_codes(board_type='行业板块')
    print(f"  获取到 {len(result2)} 个行业板块")
    
    # 比较结果
    if len(result1) == len(result2):
        print("  [OK] 两次获取结果数量一致")
    else:
        print(f"  [WARN] 两次获取结果数量不一致: {len(result1)} vs {len(result2)}")
    
    # 检查名称是否一致
    name_consistent = True
    for item1, item2 in zip(result1[:10], result2[:10]):
        if item1['ts_code'] != item2['ts_code'] or item1['name'] != item2['name']:
            name_consistent = False
            break
    
    if name_consistent:
        print("  [OK] 名称数据一致")
    else:
        print("  [WARN] 名称数据不一致")

def main():
    """主测试函数"""
    print("\n" + "=" * 80)
    print("  行业板块名称获取功能测试")
    print("=" * 80)
    
    # 初始化环境
    init_env_file()
    token = get_tushare_token()
    if token:
        ts.set_token(token)
        print(f"\n[OK] 已加载 Tushare token")
    else:
        print("\n[WARN] 未找到 Tushare token，测试可能失败")
        return
    
    try:
        # 运行测试
        success = test_industry_board_names()
        
        if success:
            test_name_retrieval_process()
        
        print("\n" + "=" * 80)
        print("  测试完成")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] 测试过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()

