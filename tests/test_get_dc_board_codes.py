"""
测试 get_dc_board_codes 函数

该脚本测试获取东财板块代码列表的功能，包括：
- 概念板块
- 行业板块
- 地域板块
- 不同日期参数
- 边界情况
"""
import sys
import io
from pathlib import Path
from datetime import datetime, timedelta

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

def print_result(test_name: str, result: list, success: bool = True):
    """打印测试结果"""
    status = "[OK]" if success else "[FAIL]"
    print(f"\n{status} {test_name}")
    if isinstance(result, list):
        print(f"  返回数量: {len(result)}")
        if len(result) > 0:
            print(f"  前5个代码: {result[:5]}")
            print(f"  后5个代码: {result[-5:]}")
            # 验证代码格式
            if len(result) > 0 and all(code.endswith('.DC') for code in result[:5]):
                print("  [OK] 代码格式正确（以.DC结尾）")
            elif len(result) == 0:
                print("  [WARN] 返回结果为空")
            else:
                print("  [FAIL] 代码格式异常")
    else:
        print(f"  返回结果: {result}")

def test_basic_functionality():
    """测试基本功能"""
    print_section("测试1: 基本功能测试")
    
    # 测试1.1: 获取概念板块（默认）
    print("\n1.1 测试获取概念板块（默认参数）")
    result = get_dc_board_codes()
    print_result("获取概念板块（默认）", result, len(result) > 0)
    
    # 测试1.2: 获取概念板块（显式指定）
    print("\n1.2 测试获取概念板块（显式指定）")
    result = get_dc_board_codes(board_type='概念板块')
    print_result("获取概念板块（显式）", result, len(result) > 0)
    
    # 测试1.3: 获取行业板块
    print("\n1.3 测试获取行业板块")
    result = get_dc_board_codes(board_type='行业板块')
    print_result("获取行业板块", result, len(result) > 0)
    
    # 测试1.4: 获取地域板块
    print("\n1.4 测试获取地域板块")
    result = get_dc_board_codes(board_type='地域板块')
    print_result("获取地域板块", result, len(result) > 0)

def test_date_parameters():
    """测试日期参数"""
    print_section("测试2: 日期参数测试")
    
    # 测试2.1: 指定日期（今天）
    print("\n2.1 测试指定日期（今天）")
    today = datetime.now().strftime('%Y%m%d')
    result = get_dc_board_codes(trade_date=today, board_type='概念板块')
    print_result(f"指定日期（{today}）", result, len(result) > 0)
    
    # 测试2.2: 指定日期（昨天）
    print("\n2.2 测试指定日期（昨天）")
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    result = get_dc_board_codes(trade_date=yesterday, board_type='概念板块')
    print_result(f"指定日期（{yesterday}）", result, len(result) >= 0)  # 可能为空（非交易日）
    
    # 测试2.3: 指定日期（一周前）
    print("\n2.3 测试指定日期（一周前）")
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
    result = get_dc_board_codes(trade_date=week_ago, board_type='概念板块')
    print_result(f"指定日期（{week_ago}）", result, len(result) >= 0)

def test_edge_cases():
    """测试边界情况"""
    print_section("测试3: 边界情况测试")
    
    # 测试3.1: 历史日期（可能返回空，如果是非交易日）
    print("\n3.1 测试历史日期")
    result = get_dc_board_codes(trade_date='20240101', board_type='概念板块')
    print_result("历史日期（20240101）", result, isinstance(result, list))
    
    # 测试3.2: 未来日期（应该返回空）
    print("\n3.2 测试未来日期")
    future_date = (datetime.now() + timedelta(days=30)).strftime('%Y%m%d')
    result = get_dc_board_codes(trade_date=future_date, board_type='概念板块')
    print_result(f"未来日期（{future_date}）", result, isinstance(result, list))
    
    # 测试3.3: 无效板块类型（应该返回空或使用默认值）
    print("\n3.3 测试无效板块类型")
    result = get_dc_board_codes(board_type='无效类型')
    print_result("无效板块类型", result, isinstance(result, list))
    
    # 测试3.4: None作为trade_date（应该使用默认值）
    print("\n3.4 测试None日期参数")
    result = get_dc_board_codes(trade_date=None, board_type='概念板块')
    print_result("None日期参数", result, len(result) > 0)

def test_result_validation():
    """测试结果验证"""
    print_section("测试4: 结果验证")
    
    # 测试4.1: 验证返回类型
    print("\n4.1 验证返回类型")
    result = get_dc_board_codes(board_type='概念板块')
    is_list = isinstance(result, list)
    print_result("返回类型检查", result, is_list)
    
    # 测试4.2: 验证代码格式
    print("\n4.2 验证代码格式")
    result = get_dc_board_codes(board_type='概念板块')
    if len(result) > 0:
        # 东财板块代码格式：BKxxxx.DC 或类似格式
        all_valid = all(
            isinstance(code, str) and 
            code.endswith('.DC')
            for code in result
        )
        print_result("代码格式验证", result, all_valid)
        if not all_valid:
            invalid_codes = [code for code in result if not code.endswith('.DC')]
            print(f"  无效代码示例: {invalid_codes[:5]}")
        else:
            print(f"  [OK] 所有代码格式正确（以.DC结尾）")
    
    # 测试4.3: 验证排序
    print("\n4.3 验证结果排序")
    result = get_dc_board_codes(board_type='概念板块')
    if len(result) > 0:
        is_sorted = result == sorted(result)
        print_result("排序验证", result, is_sorted)
        if not is_sorted:
            print("  结果未排序")

def test_comparison():
    """测试不同板块类型的对比"""
    print_section("测试5: 不同板块类型对比")
    
    today = datetime.now().strftime('%Y%m%d')
    
    # 获取三种板块类型
    concept_codes = get_dc_board_codes(trade_date=today, board_type='概念板块')
    industry_codes = get_dc_board_codes(trade_date=today, board_type='行业板块')
    region_codes = get_dc_board_codes(trade_date=today, board_type='地域板块')
    
    print(f"\n概念板块数量: {len(concept_codes)}")
    print(f"行业板块数量: {len(industry_codes)}")
    print(f"地域板块数量: {len(region_codes)}")
    
    # 检查是否有重叠（理论上不应该有）
    concept_set = set(concept_codes)
    industry_set = set(industry_codes)
    region_set = set(region_codes)
    
    overlap_ci = concept_set & industry_set
    overlap_cr = concept_set & region_set
    overlap_ir = industry_set & region_set
    
    print(f"\n概念与行业重叠: {len(overlap_ci)} 个")
    if overlap_ci:
        print(f"  重叠代码: {list(overlap_ci)[:5]}")
    
    print(f"概念与地域重叠: {len(overlap_cr)} 个")
    if overlap_cr:
        print(f"  重叠代码: {list(overlap_cr)[:5]}")
    
    print(f"行业与地域重叠: {len(overlap_ir)} 个")
    if overlap_ir:
        print(f"  重叠代码: {list(overlap_ir)[:5]}")

def test_consistency():
    """测试结果一致性"""
    print_section("测试6: 结果一致性测试")
    
    # 测试6.1: 多次调用结果是否一致
    print("\n6.1 测试多次调用结果一致性")
    result1 = get_dc_board_codes(board_type='概念板块')
    result2 = get_dc_board_codes(board_type='概念板块')
    is_consistent = result1 == result2
    print_result("多次调用一致性", result1, is_consistent)
    if not is_consistent:
        print(f"  第一次调用: {len(result1)} 个代码")
        print(f"  第二次调用: {len(result2)} 个代码")
        diff = set(result1) ^ set(result2)
        if diff:
            print(f"  差异代码数量: {len(diff)}")
    
    # 测试6.2: 验证结果唯一性（不应有重复）
    print("\n6.2 测试结果唯一性")
    result = get_dc_board_codes(board_type='概念板块')
    if len(result) > 0:
        unique_result = list(set(result))
        is_unique = len(result) == len(unique_result)
        print_result("结果唯一性", result, is_unique)
        if not is_unique:
            duplicates = len(result) - len(unique_result)
            print(f"  重复代码数量: {duplicates}")

def test_performance():
    """测试性能"""
    print_section("测试7: 性能测试")
    
    import time
    
    # 测试缓存效果（第二次调用应该更快）
    print("\n7.1 测试首次调用（无缓存）")
    start = time.time()
    result1 = get_dc_board_codes(board_type='概念板块')
    time1 = time.time() - start
    print(f"  耗时: {time1:.2f}秒, 返回: {len(result1)} 个代码")
    
    print("\n7.2 测试第二次调用（有缓存）")
    start = time.time()
    result2 = get_dc_board_codes(board_type='概念板块')
    time2 = time.time() - start
    print(f"  耗时: {time2:.2f}秒, 返回: {len(result2)} 个代码")
    
    if time2 < time1:
        print(f"  [OK] 缓存生效，速度提升: {(time1/time2):.2f}x")
    else:
        print(f"  [WARN] 缓存可能未生效或数据量较小")
    
    # 测试不同板块类型的性能
    print("\n7.3 测试不同板块类型的性能")
    for board_type in ['概念板块', '行业板块', '地域板块']:
        start = time.time()
        result = get_dc_board_codes(board_type=board_type)
        elapsed = time.time() - start
        print(f"  {board_type}: {elapsed:.2f}秒, {len(result)} 个代码")

def main():
    """主测试函数"""
    print("\n" + "=" * 80)
    print("  get_dc_board_codes 函数测试")
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
    
    # 统计测试结果
    test_results = {
        'passed': 0,
        'failed': 0,
        'warnings': 0
    }
    
    try:
        # 运行所有测试
        print("\n开始执行测试...")
        test_basic_functionality()
        test_date_parameters()
        test_edge_cases()
        test_result_validation()
        test_comparison()
        test_consistency()
        test_performance()
        
        print("\n" + "=" * 80)
        print("  所有测试完成")
        print("=" * 80)
        
        # 生成测试摘要
        print("\n测试摘要:")
        print(f"  - 基本功能测试: 完成")
        print(f"  - 日期参数测试: 完成")
        print(f"  - 边界情况测试: 完成")
        print(f"  - 结果验证测试: 完成")
        print(f"  - 板块类型对比: 完成")
        print(f"  - 结果一致性测试: 完成")
        print(f"  - 性能测试: 完成")
        
    except Exception as e:
        print(f"\n[ERROR] 测试过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()

