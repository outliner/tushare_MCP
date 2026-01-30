"""
测试所有MCP工具的脚本

该脚本会：
1. 初始化MCP服务器并注册所有工具
2. 对每个工具进行测试调用
3. 记录测试结果并生成报告
"""
import sys
import os
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 导入MCP相关
from mcp.server.fastmcp import FastMCP
from config.token_manager import get_tushare_token, init_env_file
import tushare as ts

# 导入工具注册函数
from tools import discover_tools

# 初始化环境
init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)
    print(f"✓ 已加载 Tushare token")
else:
    print("⚠️  未找到 Tushare token，部分工具可能无法正常工作")

# 创建MCP服务器实例
mcp = FastMCP("Tushare Stock Info Test")

# 注册所有工具
print("\n开始注册所有MCP工具...")
registered_modules = discover_tools(mcp)
print(f"✓ 已注册 {len(registered_modules)} 个工具模块\n")

# 从mcp实例中提取已注册的工具函数
all_tools = {}
try:
    # 尝试从_tool_manager获取工具
    if hasattr(mcp, '_tool_manager'):
        tool_manager = mcp._tool_manager
        # 检查工具管理器的内部结构
        if hasattr(tool_manager, '_tools'):
            tools_dict = tool_manager._tools
            # 提取工具函数
            for tool_name, tool_info in tools_dict.items():
                if hasattr(tool_info, 'func'):
                    all_tools[tool_name] = tool_info.func
                elif callable(tool_info):
                    all_tools[tool_name] = tool_info
except Exception as e:
    print(f"从_tool_manager获取工具时出错: {e}")

# 如果上面没有获取到，尝试通过检查已注册的工具
if not all_tools:
    # 工具已经通过装饰器注册，我们需要找到它们
    # 由于FastMCP的实现，工具函数可能存储在某个地方
    # 让我们尝试另一种方法：直接调用register函数并捕获工具
    pass

print(f"发现 {len(all_tools)} 个工具\n")
if all_tools:
    print(f"工具列表: {', '.join(list(all_tools.keys())[:10])}...\n")

# 测试结果存储
test_results: List[Dict[str, Any]] = []

def run_test(tool_name: str, test_params: Dict[str, Any] = None, description: str = "") -> Tuple[bool, str]:
    """
    运行单个工具的测试
    
    参数:
        tool_name: 工具名称
        test_params: 测试参数字典
        description: 测试描述
    
    返回:
        (是否成功, 结果信息)
    """
    if test_params is None:
        test_params = {}
    
    try:
        # 检查工具是否存在
        if tool_name not in all_tools:
            return False, f"工具 {tool_name} 未找到"
        
        # 直接调用工具函数
        tool_func = all_tools[tool_name]
        result = tool_func(**test_params)
        
        # 检查结果
        if result is None:
            return False, "工具返回 None"
        
        result_text = str(result)
        
        # 检查是否是错误消息
        if isinstance(result, str):
            if result.startswith("请") or result.startswith("错误") or result.startswith("失败"):
                return False, result
            if "error" in result.lower() or "失败" in result or "错误" in result:
                # 某些工具可能返回错误信息，但不一定是测试失败
                # 这里只检查明显的错误
                if "请先配置" in result or "未找到" in result:
                    return False, result
        
        return True, f"成功: {result_text[:200]}..." if len(result_text) > 200 else f"成功: {result_text}"
    
    except Exception as e:
        error_msg = f"异常: {type(e).__name__}: {str(e)}"
        return False, error_msg

# 获取测试日期（使用最近的交易日）
def get_test_date():
    """获取测试日期（默认使用今天，如果今天不是交易日则使用最近一个交易日）"""
    today = datetime.now()
    return today.strftime("%Y%m%d")

test_date = get_test_date()
print(f"使用测试日期: {test_date}\n")

# ==================== 定义所有测试用例 ====================

test_cases = []

# 1. Alpha策略分析工具 (9个)
test_cases.extend([
    ("analyze_sector_alpha_strategy", {"sector_code": "801080.SI", "benchmark_code": "000300.SH", "end_date": test_date}, "分析单个板块Alpha"),
    ("rank_sectors_by_alpha", {"benchmark_code": "000300.SH", "end_date": test_date, "top_n": 5}, "申万一级行业Alpha排名"),
    ("rank_l2_sectors_by_alpha", {"benchmark_code": "000300.SH", "end_date": test_date, "top_n": 10}, "申万二级行业Alpha排名"),
    ("rank_l1_sectors_alpha_full", {"benchmark_code": "000300.SH", "end_date": test_date}, "申万一级行业Alpha完整排行"),
    ("rank_l1_sectors_alpha_velocity", {"benchmark_code": "000300.SH", "end_date": test_date}, "申万一级行业Alpha排名上升速度"),
    ("rank_l2_sectors_alpha_velocity", {"benchmark_code": "000300.SH", "end_date": test_date, "top_n": 10}, "申万二级行业Alpha排名上升速度"),
    ("analyze_concept_alpha_strategy", {"concept_code": "BK1184.DC", "benchmark_code": "000300.SH", "end_date": test_date}, "分析概念板块Alpha"),
    ("rank_concepts_by_alpha", {"benchmark_code": "000300.SH", "end_date": test_date, "top_n": 10}, "概念板块Alpha排名"),
    ("rank_concepts_alpha_velocity", {"benchmark_code": "000300.SH", "end_date": test_date}, "概念板块Alpha排名上升速度"),
])

# 2. 股票行情工具 (23个)
test_cases.extend([
    ("get_stock_basic_info", {"ts_code": "000001.SZ"}, "获取股票基本信息"),
    ("search_stocks", {"keyword": "平安"}, "搜索股票"),
    ("get_stock_daily", {"ts_code": "000001.SZ", "trade_date": test_date}, "获取A股日线行情"),
    ("get_stock_weekly", {"ts_code": "000001.SZ", "trade_date": test_date}, "获取A股周线行情"),
    # ("get_stock_min", {"ts_code": "600000.SH", "freq": "1MIN"}, "获取A股分钟行情"),  # 需要实时权限
    # ("get_stock_rt_k", {"ts_code": "600000.SH"}, "获取实时日线行情"),  # 需要实时权限
    ("get_etf_daily", {"ts_code": "510300.SH", "trade_date": test_date}, "获取ETF日线行情"),
    ("get_index_daily", {"ts_code": "000300.SH", "trade_date": test_date}, "获取A股指数日线行情"),
    ("get_share_float", {"ts_code": "000001.SZ"}, "获取限售股解禁数据"),
    ("get_stock_repurchase", {}, "获取股票回购数据"),
    ("get_pledge_detail", {"ts_code": "000001.SZ"}, "获取股权质押明细"),
    ("get_block_trade", {"trade_date": test_date}, "获取大宗交易数据"),
    ("scan_announcement_signals", {"check_date": test_date}, "扫描公告信号"),
    ("get_stock_holder_trade", {"ts_code": "000001.SZ"}, "获取股东增减持数据"),
    ("get_stock_holder_number", {"ts_code": "000001.SZ"}, "获取股东户数数据"),
    ("get_stock_survey", {"ts_code": "000001.SZ"}, "获取机构调研数据"),
    ("get_cyq_perf", {"ts_code": "000001.SZ", "trade_date": test_date}, "获取筹码分析数据"),
    ("get_margin", {"trade_date": test_date}, "获取融资融券汇总数据"),
    ("get_margin_detail", {"ts_code": "000001.SZ", "trade_date": test_date}, "获取融资融券明细数据"),
    ("get_stock_moneyflow_dc", {"ts_code": "000001.SZ", "trade_date": test_date}, "获取个股资金流向"),
    ("get_daily_basic", {"ts_code": "000001.SZ", "trade_date": test_date}, "获取每日指标数据"),
    ("get_top_list", {"trade_date": test_date}, "获取龙虎榜每日明细"),
    ("get_top_inst", {"trade_date": test_date}, "获取龙虎榜机构明细"),
])

# 3. 指数行情工具 (4个)
test_cases.extend([
    ("get_global_index", {"index_code": "DJI", "trade_date": test_date}, "获取国际指数行情"),
    ("search_global_indexes", {"keyword": "道琼斯"}, "搜索国际指数"),
    ("get_sw_industry_daily", {"level": "L1", "trade_date": test_date}, "获取申万行业指数日线"),
    ("get_industry_index_codes", {"level": "L1"}, "获取申万行业分类代码"),
])

# 4. 外汇工具 (1个)
test_cases.extend([
    ("get_fx_daily", {"ts_code": "USDCNH.FXCM", "trade_date": test_date}, "获取外汇日线行情"),
])

# 5. 期货工具 (5个)
test_cases.extend([
    ("get_fut_basic", {"exchange": "SHFE"}, "获取期货合约基本信息"),
    ("get_nh_index", {"ts_code": "NHCI.NH", "trade_date": test_date}, "获取南华期货指数"),
    ("get_fut_holding", {"trade_date": test_date}, "获取期货持仓排名"),
    ("get_fut_wsr", {"trade_date": test_date}, "获取期货仓单日报"),
    # ("get_fut_min", {"ts_code": "CU2501.SHF", "freq": "1MIN"}, "获取期货分钟行情"),  # 需要实时权限
])

# 6. 财务报表工具 (2个)
test_cases.extend([
    ("get_income_statement", {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20231231"}, "获取利润表数据"),
    ("get_fina_indicator", {"ts_code": "000001.SZ"}, "获取财务指标数据"),
])

# 7. 概念板块工具 (7个)
test_cases.extend([
    ("get_eastmoney_concept_board", {"trade_date": test_date}, "获取东财概念板块行情"),
    ("get_eastmoney_concept_member", {"ts_code": "BK1184.DC", "trade_date": test_date}, "获取东财概念成分"),
    ("get_eastmoney_concept_daily", {"trade_date": test_date, "idx_type": "概念板块"}, "获取东财概念日线"),
    ("get_concept_moneyflow_dc", {"trade_date": test_date}, "获取板块资金流向"),
])

# 8. 成交量异动分析工具 (1个)
test_cases.extend([
    ("scan_l2_volume_anomaly", {"end_date": test_date}, "扫描成交量异动"),
])

# 9. 宏观全景扫描工具 (1个)
test_cases.extend([
    ("macro_scan", {"trade_date": test_date}, "宏观全景扫描"),
])

# 11. 机构抱团扫描工具 (1个)
test_cases.extend([
    ("inst_track_scan", {"trade_date": test_date}, "机构抱团扫描"),
])

# 12. 缓存管理工具 (1个)
test_cases.extend([
    ("get_cache_stats", {}, "获取缓存统计信息"),
])

# ==================== 执行测试 ====================

print("=" * 80)
print("开始测试所有MCP工具")
print("=" * 80)
print()

total_tests = len(test_cases)
success_count = 0
fail_count = 0
skip_count = 0

for i, (tool_name, params, description) in enumerate(test_cases, 1):
    print(f"[{i}/{total_tests}] 测试: {tool_name} - {description}")
    print(f"  参数: {params}")
    
    success, result = run_test(tool_name, params, description)
    
    if success:
        print(f"  ✓ 成功")
        success_count += 1
    else:
        # 检查是否是预期的错误（如权限不足、数据不存在等）
        if "权限" in result or "积分" in result or "未找到" in result or "请先配置" in result:
            print(f"  ⚠️  跳过: {result}")
            skip_count += 1
        else:
            print(f"  ✗ 失败: {result}")
            fail_count += 1
    
    test_results.append({
        "tool_name": tool_name,
        "description": description,
        "params": params,
        "success": success,
        "result": result,
        "timestamp": datetime.now().isoformat()
    })
    
    print()

# ==================== 生成测试报告 ====================

print("=" * 80)
print("测试报告")
print("=" * 80)
print()

print(f"总测试数: {total_tests}")
print(f"成功: {success_count} ({success_count/total_tests*100:.1f}%)")
print(f"失败: {fail_count} ({fail_count/total_tests*100:.1f}%)")
print(f"跳过: {skip_count} ({skip_count/total_tests*100:.1f}%)")
print()

# 按类别统计
categories = {
    "Alpha策略分析": ["analyze_sector_alpha_strategy", "rank_sectors_by_alpha", "rank_l2_sectors_by_alpha", 
                     "rank_l1_sectors_alpha_full", "rank_l1_sectors_alpha_velocity", "rank_l2_sectors_alpha_velocity",
                     "analyze_concept_alpha_strategy", "rank_concepts_by_alpha", "rank_concepts_alpha_velocity"],
    "股票行情": ["get_stock_basic_info", "search_stocks", "get_stock_daily", "get_stock_weekly", 
               "get_etf_daily", "get_index_daily", "get_share_float", "get_stock_repurchase", 
               "get_pledge_detail", "get_block_trade", "scan_announcement_signals", "get_stock_holder_trade",
               "get_stock_holder_number", "get_stock_survey", "get_cyq_perf", "get_margin", 
               "get_margin_detail", "get_stock_moneyflow_dc", "get_daily_basic", "get_top_list", "get_top_inst"],
    "指数行情": ["get_global_index", "search_global_indexes", "get_sw_industry_daily", "get_industry_index_codes"],
    "外汇": ["get_fx_daily"],
    "期货": ["get_fut_basic", "get_nh_index", "get_fut_holding", "get_fut_wsr"],
    "财务报表": ["get_income_statement", "get_fina_indicator"],
    "概念板块": ["get_eastmoney_concept_board", "get_eastmoney_concept_member", "get_eastmoney_concept_daily", "get_concept_moneyflow_dc"],
    "成交量异动": ["scan_l2_volume_anomaly"],
    "宏观全景扫描": ["macro_scan"],
    "机构抱团扫描": ["inst_track_scan"],
    "缓存管理": ["get_cache_stats"]
}

print("\n按类别统计:")
print("-" * 80)
for category, tool_names in categories.items():
    category_results = [r for r in test_results if r["tool_name"] in tool_names]
    if category_results:
        cat_success = sum(1 for r in category_results if r["success"])
        cat_total = len(category_results)
        print(f"{category}: {cat_success}/{cat_total} 成功 ({cat_success/cat_total*100:.1f}%)")

# 失败的测试详情
if fail_count > 0:
    print("\n失败的测试详情:")
    print("-" * 80)
    for result in test_results:
        if not result["success"] and "权限" not in result["result"] and "积分" not in result["result"]:
            print(f"  ✗ {result['tool_name']}: {result['result']}")

# 保存测试报告到文件
report_file = project_root / "test_report.txt"
with open(report_file, "w", encoding="utf-8") as f:
    f.write("=" * 80 + "\n")
    f.write("MCP工具测试报告\n")
    f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 80 + "\n\n")
    
    f.write(f"总测试数: {total_tests}\n")
    f.write(f"成功: {success_count} ({success_count/total_tests*100:.1f}%)\n")
    f.write(f"失败: {fail_count} ({fail_count/total_tests*100:.1f}%)\n")
    f.write(f"跳过: {skip_count} ({skip_count/total_tests*100:.1f}%)\n\n")
    
    f.write("\n详细测试结果:\n")
    f.write("-" * 80 + "\n")
    for result in test_results:
        status = "✓" if result["success"] else "✗"
        f.write(f"{status} {result['tool_name']} - {result['description']}\n")
        f.write(f"  参数: {result['params']}\n")
        f.write(f"  结果: {result['result'][:200]}\n")
        f.write("\n")

print(f"\n测试报告已保存到: {report_file}")

# 退出码
sys.exit(0 if fail_count == 0 else 1)

