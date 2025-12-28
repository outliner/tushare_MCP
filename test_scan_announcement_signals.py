#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本：scan_announcement_signals 公告信号扫描功能

测试目标：
1. 测试单日查询 (check_date)
2. 测试日期范围查询 (start_date + end_date)
3. 测试指定股票代码查询 (ts_code_list)
4. 测试格式化输出功能
"""

import os
import sys
from datetime import datetime, timedelta

# 设置控制台编码为UTF-8（Windows兼容）
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tushare as ts
import pandas as pd

# 从项目的token管理模块获取token
from config.token_manager import get_tushare_token


def format_announcement_signals_data(df: pd.DataFrame, ts_code_list: str = "", date_filter: str = "") -> str:
    """
    格式化公告信号数据输出
    """
    if df.empty:
        return "未找到符合条件的关键公告信号"
    
    result = []
    result.append("📢 上市公司公告信号扫描")
    result.append("=" * 180)
    result.append("")
    
    # 统计信息
    total_count = len(df)
    bear_count = len(df[df['signal'] == '利空警报 (Bear)'])
    bull_count = len(df[df['signal'] == '利好催化 (Bull)'])
    event_count = len(df[df['signal'] == '重大事项 (Event)'])
    
    result.append(f"📊 扫描结果统计：")
    result.append(f"  - 总信号数: {total_count} 条")
    result.append(f"  - 🔴 利空警报: {bear_count} 条")
    result.append(f"  - 🟢 利好催化: {bull_count} 条")
    result.append(f"  - 🟡 重大事项: {event_count} 条")
    result.append("")
    
    if date_filter:
        result.append(f"查询日期: {date_filter}")
    if ts_code_list:
        result.append(f"股票代码: {ts_code_list}")
    result.append("")
    
    # 按信号类型分组显示
    signal_groups = {
        '利空警报 (Bear)': '🔴 利空警报 (Bear) - 避雷优先',
        '利好催化 (Bull)': '🟢 利好催化 (Bull)',
        '重大事项 (Event)': '🟡 重大事项 (Event)'
    }
    
    for signal_type, header in signal_groups.items():
        signal_df = df[df['signal'] == signal_type]
        if signal_df.empty:
            continue
        
        result.append(header)
        result.append("-" * 120)
        result.append(f"{'公告日期':<12} {'股票代码':<12} {'股票名称':<12} {'信号类型':<20} {'公告标题':<60}")
        result.append("-" * 120)
        
        # 按公告日期排序（最新的在前）
        if 'ann_date' in signal_df.columns:
            signal_df = signal_df.sort_values('ann_date', ascending=False)
        
        display_count = min(50, len(signal_df))
        for i, (_, row) in enumerate(signal_df.head(display_count).iterrows()):
            ann_date = str(row.get('ann_date', '-'))
            ts_code = str(row.get('ts_code', '-'))
            name = str(row.get('name', '-'))[:10]
            signal = str(row.get('signal', '-'))
            title = str(row.get('title', '-'))[:55]
            
            result.append(f"{ann_date:<12} {ts_code:<12} {name:<12} {signal:<20} {title}")
        
        if len(signal_df) > display_count:
            result.append(f"... 还有 {len(signal_df) - display_count} 条 {signal_type} 信号未显示")
        
        result.append("")
    
    return "\n".join(result)


def scan_announcement_signals(
    ts_code_list: str = "",
    check_date: str = "",
    start_date: str = "",
    end_date: str = ""
) -> str:
    """
    扫描上市公司公告标题，捕捉【重大利好】或【重大利空】信号
    
    参数:
        ts_code_list: 股票代码列表（多个代码用逗号分隔，如：000001.SZ,600000.SH，可选。若为空则扫描全市场）
        check_date: 公告日期（YYYYMMDD格式，如：20230621，可选，默认当天）
        start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
        end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
    
    返回:
        包含关键公告信号的格式化字符串
    """
    token = get_tushare_token()
    if not token:
        return "请先配置Tushare token（设置环境变量 TUSHARE_TOKEN）"
    
    try:
        ts.set_token(token)
        pro = ts.pro_api()
        
        # 解析股票代码列表
        ts_code_filter = None
        if ts_code_list:
            ts_code_filter = [code.strip() for code in ts_code_list.split(',') if code.strip()]
        
        # 构建API查询参数
        api_params = {}
        
        # 日期参数处理
        if check_date:
            # 单日查询使用 ann_date
            api_params['ann_date'] = check_date
            date_info = check_date
        elif start_date and end_date:
            # 日期范围查询使用 start_date 和 end_date（API原生支持）
            api_params['start_date'] = start_date
            api_params['end_date'] = end_date
            date_info = f"{start_date} 至 {end_date}"
        else:
            # 默认使用当天
            api_params['ann_date'] = datetime.now().strftime('%Y%m%d')
            date_info = api_params['ann_date']
        
        print(f"📡 查询参数: {api_params}")
        
        # 获取公告数据
        all_results = []
        last_error = None  # 记录最后一个错误
        
        try:
            if ts_code_filter:
                # 有股票代码过滤时，逐个股票查询（API原生支持ts_code参数）
                for ts_code in ts_code_filter:
                    try:
                        print(f"  正在查询 {ts_code} ...")
                        df = pro.anns_d(ts_code=ts_code, **api_params)
                        if df is not None and not df.empty:
                            all_results.append(df)
                            print(f"    ✅ 获取到 {len(df)} 条公告")
                        else:
                            print(f"    ⚠️ 无数据")
                    except Exception as e:
                        print(f"    ❌ 查询失败: {e}")
                        last_error = str(e)  # 记录错误信息
                        continue
                
                if all_results:
                    df = pd.concat(all_results, ignore_index=True)
                else:
                    df = pd.DataFrame()
                    # 如果所有查询都失败且有权限错误，返回错误信息
                    if last_error and ('没有接口访问权限' in last_error or '权限' in last_error):
                        return f"API调用失败：{last_error}\n\n请检查：\n1. Tushare token是否有效\n2. 账户是否有anns_d接口权限（本接口为单独权限）\n\n建议：\n- 请查看Tushare文档确认anns_d接口权限\n- 访问 https://tushare.pro/document/1?doc_id=108 查看权限说明"
            else:
                # 全市场查询
                print(f"  正在查询全市场公告...")
                df = pro.anns_d(**api_params)
                if df is not None and not df.empty:
                    print(f"    ✅ 获取到 {len(df)} 条公告")
                else:
                    print(f"    ⚠️ 无数据")
                    
        except Exception as api_error:
            error_msg = str(api_error)
            if '没有接口访问权限' in error_msg or '权限' in error_msg:
                return f"API调用失败：{error_msg}\n\n请检查：\n1. Tushare token是否有效\n2. 账户是否有anns_d接口权限（本接口为单独权限）\n\n建议：\n- 请查看Tushare文档确认anns_d接口权限\n- 访问 https://tushare.pro/document/1?doc_id=108 查看权限说明"
            else:
                return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户是否有anns_d接口权限（本接口为单独权限）\n3. 网络连接是否正常"
        
        if df is None or df.empty:
            stock_info = f"（股票：{ts_code_list}）" if ts_code_list else ""
            return f"未找到 {date_info} {stock_info}的公告数据"
        
        # --- 核心逻辑：关键词字典 ---
        
        # 1. 进攻关键词 (利好)
        keywords_bull = ['中标', '合同', '签署', '收购', '增持', '回购', '获得', '通过', '预增', '扭亏', 
                       '盈利', '增长', '突破', '创新', '合作', '投资', '建设', '投产', '上市', '获批']
        
        # 2. 防守关键词 (利空)
        keywords_bear = ['立案', '调查', '警示', '监管函', '问询', '诉讼', '冻结', '减持', '终止', '亏损', 
                       '下修', '风险', '违规', '处罚', '退市', 'ST', '停牌', '破产', '清算', '违约']
        
        # 3. 中性/重要关键词 (关注)
        keywords_neutral = ['重组', '复牌', '定增', '激励', '调研', '股东大会', '董事会', '变更', '转让', 
                          '质押', '解押', '分红', '配股', '可转债']
        
        results = []
        
        for index, row in df.iterrows():
            title = str(row.get('title', ''))
            if not title:
                continue
            
            signal_type = None
            
            # 匹配逻辑：优先匹配利空（避雷第一）
            if any(k in title for k in keywords_bear):
                signal_type = "利空警报 (Bear)"
            elif any(k in title for k in keywords_bull):
                # 简单排除法：比如 "终止收购" 虽然有收购，但是利空
                if '终止' not in title and '取消' not in title and '失败' not in title:
                    signal_type = "利好催化 (Bull)"
            elif any(k in title for k in keywords_neutral):
                signal_type = "重大事项 (Event)"
            
            if signal_type:
                results.append({
                    'ts_code': row.get('ts_code', '-'),
                    'name': row.get('name', '-'),
                    'ann_date': row.get('ann_date', '-'),
                    'title': title,
                    'signal': signal_type,
                    'url': row.get('url', '-')
                })
        
        if not results:
            stock_info = f"（股票：{ts_code_list}）" if ts_code_list else ""
            return f"未发现 {date_info} {stock_info}的关键信号公告\n\n说明：共扫描 {len(df)} 条公告，未匹配到利好/利空/重大事项关键词"
        
        # 转换为DataFrame
        res_df = pd.DataFrame(results)
        
        # 优先展示利空，因为避雷第一
        signal_order = {'利空警报 (Bear)': 1, '利好催化 (Bull)': 2, '重大事项 (Event)': 3}
        res_df['signal_order'] = res_df['signal'].map(signal_order)
        res_df = res_df.sort_values(['signal_order', 'ann_date'], ascending=[True, False])
        res_df = res_df.drop('signal_order', axis=1)
        
        # 格式化输出
        return format_announcement_signals_data(res_df, ts_code_list or "", check_date or start_date or "")
        
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        return f"扫描失败：{str(e)}\n详细信息：{error_detail}"


def test_with_mock_data():
    """使用模拟数据测试格式化输出功能"""
    print("=" * 80)
    print("🧪 模拟数据测试 - 测试格式化输出功能")
    print("=" * 80)
    print()
    
    # 模拟公告数据 - 包含均胜电子相关
    mock_data = [
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20251215', 'title': '关于收购某汽车电子公司股权的公告', 'signal': '利好催化 (Bull)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20251210', 'title': '关于签署新能源汽车战略合作协议的公告', 'signal': '利好催化 (Bull)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20251205', 'title': '关于股东减持计划的预披露公告', 'signal': '利空警报 (Bear)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20251128', 'title': '关于重大资产重组进展的公告', 'signal': '重大事项 (Event)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20251120', 'title': '关于股票激励计划的公告', 'signal': '重大事项 (Event)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20251015', 'title': '关于中标智能驾驶项目的公告', 'signal': '利好催化 (Bull)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20250920', 'title': '关于收到问询函的公告', 'signal': '利空警报 (Bear)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20250815', 'title': '关于获批新专利的公告', 'signal': '利好催化 (Bull)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20250710', 'title': '关于董事会决议的公告', 'signal': '重大事项 (Event)'},
        {'ts_code': '600699.SH', 'name': '均胜电子', 'ann_date': '20250625', 'title': '关于投资建设新工厂的公告', 'signal': '利好催化 (Bull)'},
    ]
    
    mock_df = pd.DataFrame(mock_data)
    
    # 测试格式化输出
    result = format_announcement_signals_data(mock_df, "600699.SH", "20250619 至 20251219")
    print(result)
    print()


def test_api_direct():
    """直接测试API调用（不经过关键词过滤）"""
    print("=" * 80)
    print("🔬 直接API测试 - 检查anns_d接口连通性")
    print("=" * 80)
    print()
    
    token = get_tushare_token()
    if not token:
        print("❌ 未找到 TUSHARE_TOKEN")
        return
    
    ts.set_token(token)
    pro = ts.pro_api()
    
    # 测试不同的查询方式
    test_cases = [
        {"desc": "单日查询", "params": {"ann_date": "20241218"}},
        {"desc": "指定股票单日查询", "params": {"ts_code": "600699.SH", "ann_date": "20241218"}},
        {"desc": "日期范围查询", "params": {"start_date": "20241215", "end_date": "20241218"}},
        {"desc": "指定股票日期范围查询", "params": {"ts_code": "600699.SH", "start_date": "20241201", "end_date": "20241218"}},
    ]
    
    for test in test_cases:
        print(f"\n📋 {test['desc']}")
        print(f"   参数: {test['params']}")
        try:
            df = pro.anns_d(**test['params'])
            if df is not None and not df.empty:
                print(f"   ✅ 返回 {len(df)} 条记录")
                print(f"   字段: {list(df.columns)}")
                if len(df) > 0:
                    print(f"   示例: {df.iloc[0].to_dict()}")
            else:
                print(f"   ⚠️ 无数据返回")
        except Exception as e:
            print(f"   ❌ 错误: {e}")


def main():
    """主测试函数"""
    print("=" * 80)
    print("测试 scan_announcement_signals 公告信号扫描功能")
    print("=" * 80)
    print()
    
    # 检查Token
    token = get_tushare_token()
    if not token:
        print("❌ 错误：未检测到 TUSHARE_TOKEN 环境变量")
        print("请先设置环境变量：")
        print("  Windows: set TUSHARE_TOKEN=your_token_here")
        print("  Linux/Mac: export TUSHARE_TOKEN=your_token_here")
        print()
        print("将使用模拟数据进行测试...")
        print()
        test_with_mock_data()
        return
    
    print(f"✅ 已检测到 Tushare Token: {token[:8]}...{token[-4:]}")
    print()
    
    # 先测试API连通性
    print("\n" + "=" * 80)
    print("📡 第一步：测试API连通性")
    print("=" * 80)
    test_api_direct()
    
    # 测试用例
    test_cases = [
        {
            "name": "测试1: 查询均胜电子（600699.SH）近期公告",
            "params": {"ts_code_list": "600699.SH", "start_date": "20241201", "end_date": "20241219"}
        },
        {
            "name": "测试2: 查询均胜电子指定日期",
            "params": {"ts_code_list": "600699.SH", "check_date": "20241218"}
        },
        {
            "name": "测试3: 查询全市场指定日期公告",
            "params": {"check_date": "20241218"}
        },
        {
            "name": "测试4: 查询多只股票",
            "params": {"ts_code_list": "600699.SH,000001.SZ,600000.SH", "check_date": "20241218"}
        },
    ]
    
    api_permission_error = False
    
    for i, test in enumerate(test_cases):
        print(f"\n{'='*80}")
        print(f"🔍 {test['name']}")
        print(f"参数: {test['params']}")
        print(f"{'='*80}")
        
        try:
            result = scan_announcement_signals(**test['params'])
            print(result)
            
            # 检查是否有权限问题
            if "没有接口访问权限" in result or "权限" in result or "API调用失败" in result:
                api_permission_error = True
                break
                
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()
        
        print()
    
    # 如果API没有权限，使用模拟数据测试
    if api_permission_error:
        print("\n" + "=" * 80)
        print("⚠️  检测到API权限不足，切换到模拟数据测试")
        print("=" * 80 + "\n")
        test_with_mock_data()


if __name__ == "__main__":
    main()
