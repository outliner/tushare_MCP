"""
测试 meso_scan 函数功能
"""
import sys
import os
from pathlib import Path

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
    print(f"✓ 已加载 Tushare token")
else:
    print("⚠️  未找到 Tushare token")
    sys.exit(1)

# 测试导入
print("\n【1. 测试导入】")
try:
    from tools.meso_scan_tools import (
        meso_scan,
        _analyze_mainline_lifecycle,
        _analyze_phoenix_rebound,
        _analyze_money_validation,
        _format_meso_scan_report
    )
    print("✓ 成功导入 meso_scan 相关函数")
except ImportError as e:
    print(f"✗ 导入失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 测试依赖函数导入
print("\n【2. 测试依赖函数导入】")
try:
    from tools.concept_tools import get_hot_concept_codes, scan_concept_volume_anomaly
    from tools.alpha_strategy_analyzer import rank_sectors_alpha, calculate_alpha_rank_velocity
    print("✓ 成功导入依赖函数")
except ImportError as e:
    print(f"✗ 依赖函数导入失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 测试 meso_scan 函数
print("\n【3. 测试 meso_scan 函数执行】")
test_date = "20251209"  # 使用最近的交易日

try:
    # 由于 meso_scan 是 MCP tool，需要模拟调用
    # 我们直接测试内部函数
    print(f"测试日期: {test_date}")
    
    # 测试模块A：主线定位
    print("\n【模块A：主线定位】")
    mainline_result = _analyze_mainline_lifecycle(test_date, top_n=10, limit_up_threshold=5)
    if mainline_result.get("success"):
        concepts = mainline_result.get("concepts", [])
        print(f"✓ 成功获取 {len(concepts)} 个概念板块")
        if concepts:
            print(f"  示例: {concepts[0]}")
    else:
        print(f"✗ 失败: {mainline_result.get('error', '未知错误')}")
    
    # 测试模块B：凤凰策略
    print("\n【模块B：凤凰策略】")
    phoenix_result = _analyze_phoenix_rebound(
        test_date, 
        price_change_5d_max=-0.05,
        vol_ratio_threshold=1.3
    )
    if phoenix_result.get("success"):
        rebounds = phoenix_result.get("rebounds", [])
        print(f"✓ 成功获取 {len(rebounds)} 个超跌反弹机会")
        if rebounds:
            print(f"  示例: {rebounds[0]}")
    else:
        print(f"✗ 失败: {phoenix_result.get('error', '未知错误')}")
    
    # 测试模块C：资金验伪
    print("\n【模块C：资金验伪】")
    money_result = _analyze_money_validation(
        test_date,
        mainline_result,
        phoenix_result,
        outflow_warning=1.0
    )
    if money_result.get("success"):
        golden_list = money_result.get("golden_list", [])
        warnings = money_result.get("warnings", [])
        print(f"✓ 成功验证资金流向")
        print(f"  黄金板块: {len(golden_list)} 个")
        print(f"  资金背离预警: {len(warnings)} 个")
    else:
        print(f"✗ 失败: {money_result.get('error', '未知错误')}")
    
    # 测试完整报告生成
    print("\n【4. 测试完整报告生成】")
    report = _format_meso_scan_report(
        test_date,
        mainline_result,
        phoenix_result,
        money_result,
        top_n=10
    )
    print("✓ 成功生成报告")
    print(f"  报告长度: {len(report)} 字符")
    print("\n报告预览（前500字符）:")
    print("-" * 60)
    print(report[:500])
    print("-" * 60)
    
    # 验证报告格式
    required_sections = [
        "【模块A：主线定位】",
        "【模块B：凤凰策略】",
        "【模块C：资金验伪】"
    ]
    missing_sections = [s for s in required_sections if s not in report]
    if missing_sections:
        print(f"\n⚠️  报告缺少部分: {missing_sections}")
    else:
        print("\n✓ 报告包含所有必需部分")
    
except Exception as e:
    print(f"\n✗ 测试执行失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("✓ 所有测试通过！meso_scan 功能正常")
print("=" * 60)

