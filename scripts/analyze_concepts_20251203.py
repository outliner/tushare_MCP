"""
分析东财热门概念板块的各项数据并保存到本地文件
支持指定日期，包含5个任务：
1. 量价异动分析
2. Alpha收益排行
3. 资金流入情况
4. Alpha增长速度排行
5. 涨跌停股票数据
"""
import sys
import argparse
from pathlib import Path
import time
import json
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts
import pandas as pd
from tools.concept_tools import (
    scan_concept_volume_anomaly,
    get_hot_concept_codes,
    get_concept_codes
)
from tools.alpha_strategy_analyzer import (
    rank_sectors_alpha,
    calculate_alpha_rank_velocity,
    format_alpha_analysis
)
from tools.stock_tools import format_limit_list_data, format_large_number
from cache.cache_manager import cache_manager
from utils.common import format_date

# 解析命令行参数
parser = argparse.ArgumentParser(description='分析东财热门概念板块数据')
parser.add_argument(
    '--date', 
    type=str, 
    default=None,
    help='分析日期，格式：YYYYMMDD（如：20251204），默认为今天'
)
args = parser.parse_args()

# 初始化
init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)
    print("✓ 已加载 Tushare token\n")
else:
    print("⚠️  未找到 Tushare token")
    sys.exit(1)

# 确定分析日期
if args.date:
    end_date = args.date
else:
    # 默认使用今天
    end_date = datetime.now().strftime("%Y%m%d")

print(f"分析日期: {end_date}\n")

# 创建doc文件夹
doc_dir = Path("doc")
doc_dir.mkdir(exist_ok=True)

# 生成时间戳
timestamp = int(time.time())

def save_result_to_file(content: str, task_name: str, task_number: int = None):
    """保存结果到单独文件"""
    # 生成文件名：日期-时间戳-任务X-简短描述.txt
    task_descriptions = {
        1: "量价异动分析",
        2: "Alpha收益排行",
        3: "资金流入情况",
        4: "Alpha增长速度排行",
        5: "涨跌停股票数据"
    }
    
    if task_number and task_number in task_descriptions:
        description = task_descriptions[task_number]
        if "错误" in task_name:
            description += "-错误"
        filename = f"{end_date}-{timestamp}-任务{task_number}-{description}.txt"
    else:
        # 如果没有任务编号，使用任务名简化版
        task_suffix = task_name.replace("任务", "").replace("：", "-").replace("（错误）", "-错误")
        task_suffix = task_suffix.replace("东财热门概念板块", "").replace("东方财富概念", "")
        task_suffix = task_suffix.replace(" ", "").strip("-")
        filename = f"{end_date}-{timestamp}-{task_suffix}.txt"
    
    # 清理文件名中的特殊字符
    filename = filename.replace("/", "-").replace("\\", "-").replace(":", "-").replace("*", "-").replace("?", "-").replace('"', "-").replace("|", "-")
    filepath = doc_dir / filename
    
    # 写入文件
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"{task_name}\n")
        f.write(f"{'='*80}\n\n")
        f.write(content)
    
    print(f"✓ {task_name} 结果已保存到: {filepath}")
    return filepath

# 任务1：分析东财热门概念板块20251203量价异动情况
print("=" * 80)
print("任务1：分析东财热门概念板块量价异动情况")
print("=" * 80)
try:
    scan_result = scan_concept_volume_anomaly(
        end_date=end_date,
        vol_ratio_threshold=1.15,
        price_change_5d_min=0.02,
        price_change_5d_max=0.08,
        hot_limit=160
    )
    
    # 格式化结果 - 保存所有热门板块的数据
    result_text = []
    result_text.append(f"📊 东财热门概念板块量价异动分析")
    result_text.append(f"分析日期: {end_date}")
    result_text.append(f"扫描板块数量: {scan_result.get('scanned_count', 0)}")
    result_text.append(f"匹配异动数量: {scan_result.get('matched_count', 0)}")
    result_text.append("")
    
    # 保存所有板块数据（all_results包含所有扫描的板块）
    all_results = scan_result.get('all_results', [])
    if all_results:
        result_text.append(f"📋 所有热门板块量价数据（共{len(all_results)}个）:")
        result_text.append("-" * 120)
        result_text.append(f"{'序号':<6} {'板块代码':<15} {'板块名称':<30} {'成交量比率':<12} {'5日涨幅':<12} {'换手率':<12} {'当前价格':<12} {'是否匹配':<10} {'说明':<30}")
        result_text.append("-" * 120)
        
        for i, match in enumerate(all_results, 1):
            code = match.get('code', '')
            name = match.get('name', code)
            metrics = match.get('metrics', {})
            vol_ratio = metrics.get('vol_ratio', 0)
            price_change_5d = metrics.get('price_change_5d', 0)
            turnover_rate = metrics.get('turnover_rate', 0)
            current_price = metrics.get('current_price', 0)
            is_match = "是" if match.get('is_match', False) else "否"
            reasoning = match.get('reasoning', '无')
            
            result_text.append(f"{i:<6} {code:<15} {str(name)[:28]:<30} {vol_ratio:>10.2f} {price_change_5d*100:>10.2f}% {turnover_rate:>10.2f}% {current_price:>10.2f} {is_match:<10} {str(reasoning)[:28]:<30}")
        
        result_text.append("")
        result_text.append("-" * 120)
        result_text.append("")
    
    # 如果有匹配的异动板块，单独列出
    if scan_result.get('matched_count', 0) > 0:
        result_text.append("✅ 符合异动条件的板块:")
        result_text.append("-" * 120)
        for i, match in enumerate(scan_result.get('matches', []), 1):
            result_text.append(f"\n{i}. {match.get('name', match.get('code'))} ({match.get('code')})")
            metrics = match.get('metrics', {})
            result_text.append(f"   成交量比率: {metrics.get('vol_ratio', 0):.2f}")
            result_text.append(f"   5日涨幅: {metrics.get('price_change_5d', 0)*100:.2f}%")
            result_text.append(f"   换手率: {metrics.get('turnover_rate', 0):.2f}%")
            result_text.append(f"   当前价格: {metrics.get('current_price', 0):.2f}")
            result_text.append(f"   说明: {match.get('reasoning', '无')}")
    
    result_content = "\n".join(result_text)
    save_result_to_file(result_content, "任务1：东财热门概念板块量价异动分析", task_number=1)
    print("✓ 任务1完成\n")
    
except Exception as e:
    import traceback
    error_msg = f"任务1执行失败: {str(e)}\n详细信息:\n{traceback.format_exc()}"
    print(error_msg)
    save_result_to_file(error_msg, "任务1：东财热门概念板块量价异动分析（错误）", task_number=1)

# 任务2：分析东财热门概念板块20251203 alpha 收益排行
print("=" * 80)
print("任务2：分析东财热门概念板块Alpha收益排行")
print("=" * 80)
try:
    # 获取热门概念板块代码列表
    concept_codes = get_hot_concept_codes(end_date, limit=80)
    
    if not concept_codes:
        result_content = "无法获取热门概念板块列表"
        save_result_to_file(result_content, "任务2：东财热门概念板块Alpha收益排行（错误）", task_number=2)
        print("⚠️ 无法获取热门概念板块列表\n")
    else:
        # 进行Alpha排名
        df = rank_sectors_alpha(concept_codes, "000300.SH", end_date)
        
        if df.empty:
            result_content = "无法获取板块Alpha数据"
            save_result_to_file(result_content, "任务2：东财热门概念板块Alpha收益排行（错误）", task_number=2)
            print("⚠️ 无法获取板块Alpha数据\n")
        else:
            # 获取板块名称 - 获取所有板块的名称
            try:
                pro = ts.pro_api()
                # 分批获取板块名称（API可能有限制）
                all_codes = df['sector_code'].tolist()
                name_map = {}
                pct_map = {}
                amount_map = {}
                turnover_map = {}
                
                # 每次查询最多50个代码
                batch_size = 50
                for i in range(0, len(all_codes), batch_size):
                    batch_codes = all_codes[i:i+batch_size]
                    concept_codes_str = ','.join(batch_codes)
                    concept_df = pro.dc_index(ts_code=concept_codes_str, trade_date=end_date)
                    
                    if not concept_df.empty and 'ts_code' in concept_df.columns:
                        for _, row in concept_df.iterrows():
                            code = row['ts_code']
                            name_map[code] = row.get('name', code) if pd.notna(row.get('name')) else code
                            pct_map[code] = row.get('pct_change', 0) if pd.notna(row.get('pct_change')) else 0
                            amount_map[code] = row.get('amount', 0) if pd.notna(row.get('amount')) else 0
                            turnover_map[code] = row.get('turnover', 0) if pd.notna(row.get('turnover')) else 0
                
                # 添加板块名称等信息
                df['name'] = df['sector_code'].map(name_map).fillna(df['sector_code'])
                df['pct_change'] = df['sector_code'].map(pct_map).fillna(0)
                df['amount'] = df['sector_code'].map(amount_map).fillna(0)
                df['turnover'] = df['sector_code'].map(turnover_map).fillna(0)
                
            except Exception as e:
                import sys
                print(f"获取板块名称失败: {str(e)}", file=sys.stderr)
                df['name'] = df['sector_code']
                df['pct_change'] = 0
                df['amount'] = 0
                df['turnover'] = 0
            
            # 格式化输出
            result_text = []
            result_text.append(f"📊 东财热门概念板块Alpha收益排行")
            result_text.append(f"分析日期: {end_date}")
            result_text.append(f"基准指数: 000300.SH (沪深300)")
            result_text.append(f"分析板块数量: {len(df)}")
            result_text.append("")
            result_text.append(f"{'排名':<6} {'板块代码':<15} {'板块名称':<20} {'Alpha得分':<12} {'Alpha_2':<12} {'Alpha_5':<12} {'今日涨跌':<10} {'成交额(亿)':<12}")
            result_text.append("-" * 120)
            
            # 保存所有板块数据，不截断
            for i, row in df.iterrows():
                rank = row.get('rank', i+1)
                code = row.get('sector_code', '')
                name = row.get('name', code)
                score = row.get('alpha_score', 0)
                alpha_2 = row.get('alpha_2', 0)
                alpha_5 = row.get('alpha_5', 0)
                pct = row.get('pct_change', 0)
                amount = row.get('amount', 0) / 100000000  # 转换为亿元
                
                result_text.append(f"{rank:<6} {code:<15} {name[:18]:<20} {score*100:>10.2f}% {alpha_2*100:>10.2f}% {alpha_5*100:>10.2f}% {pct:>8.2f}% {amount:>10.2f}")
            
            result_content = "\n".join(result_text)
            save_result_to_file(result_content, "任务2：东财热门概念板块Alpha收益排行", task_number=2)
            print("✓ 任务2完成\n")
            
except Exception as e:
    import traceback
    error_msg = f"任务2执行失败: {str(e)}\n详细信息:\n{traceback.format_exc()}"
    print(error_msg)
    save_result_to_file(error_msg, "任务2：东财热门概念板块Alpha收益排行（错误）", task_number=2)

# 任务3：获取东方财富概念20251203资金流入情况
print("=" * 80)
print("任务3：获取东方财富概念资金流入情况")
print("=" * 80)
try:
    pro = ts.pro_api()
    
    # 获取概念板块资金流向数据
    df = pro.moneyflow_ind_dc(trade_date=end_date, content_type="概念")
    
    if df.empty:
        result_content = f"未找到{end_date}的概念板块资金流向数据"
        save_result_to_file(result_content, "任务3：东方财富概念资金流入情况（错误）", task_number=3)
        print("⚠️ 未找到资金流向数据\n")
    else:
        # 按主力净流入排序
        if 'net_mf_amount' in df.columns:
            df = df.sort_values('net_mf_amount', ascending=False)
        
        # 格式化输出
        result_text = []
        result_text.append(f"📊 东方财富概念板块资金流入情况")
        result_text.append(f"分析日期: {end_date}")
        result_text.append(f"板块数量: {len(df)}")
        result_text.append("")
        result_text.append(f"{'排名':<6} {'板块代码':<15} {'板块名称':<25} {'主力净流入(万)':<18} {'超大单(万)':<15} {'大单(万)':<15} {'中单(万)':<15} {'小单(万)':<15}")
        result_text.append("-" * 140)
        
        # 保存所有板块数据，不截断
        for i, row in df.iterrows():
            rank = i + 1
            code = row.get('ts_code', '')
            name = row.get('name', code) if 'name' in row else code
            net_mf = row.get('net_mf_amount', 0) / 10000  # 转换为万元
            super_large = row.get('super_large_amount', 0) / 10000
            large = row.get('large_amount', 0) / 10000
            mid = row.get('mid_amount', 0) / 10000
            small = row.get('small_amount', 0) / 10000
            
            result_text.append(f"{rank:<6} {code:<15} {str(name)[:23]:<25} {net_mf:>15.2f} {super_large:>13.2f} {large:>13.2f} {mid:>13.2f} {small:>13.2f}")
        
        result_content = "\n".join(result_text)
        save_result_to_file(result_content, "任务3：东方财富概念资金流入情况", task_number=3)
        print("✓ 任务3完成\n")
        
except Exception as e:
    import traceback
    error_msg = f"任务3执行失败: {str(e)}\n详细信息:\n{traceback.format_exc()}"
    print(error_msg)
    save_result_to_file(error_msg, "任务3：东方财富概念资金流入情况（错误）", task_number=3)

# 任务4：分析东财热门概念板块20251203alpha增长速度排行
print("=" * 80)
print("任务4：分析东财热门概念板块Alpha增长速度排行")
print("=" * 80)
try:
    # 获取概念板块代码列表
    concept_codes = get_concept_codes(end_date)
    
    if not concept_codes:
        result_content = "无法获取概念板块列表"
        save_result_to_file(result_content, "任务4：东财热门概念板块Alpha增长速度排行（错误）", task_number=4)
        print("⚠️ 无法获取概念板块列表\n")
    else:
        # 计算排名上升速度
        df = calculate_alpha_rank_velocity(concept_codes, "000300.SH", end_date)
        
        if df.empty:
            result_content = "无法获取Alpha排名上升速度数据"
            save_result_to_file(result_content, "任务4：东财热门概念板块Alpha增长速度排行（错误）", task_number=4)
            print("⚠️ 无法获取Alpha排名上升速度数据\n")
        else:
            # 获取实际使用的日期信息
            current_date = df.attrs.get('current_date', end_date)
            yesterday_date = df.attrs.get('yesterday_date', None)
            day_before_yesterday_date = df.attrs.get('day_before_yesterday_date', None)
            
            # 格式化输出
            result_text = []
            result_text.append(f"📊 东财热门概念板块Alpha排名上升速度分析")
            result_text.append(f"分析日期: {current_date}")
            if yesterday_date:
                result_text.append(f"对比日期1（较昨日）: {yesterday_date}")
            if day_before_yesterday_date:
                result_text.append(f"对比日期2（较前天）: {day_before_yesterday_date}")
            result_text.append("")
            
            # 按1日上升位数排序 - 保存所有数据
            if 'rank_change_1d' in df.columns:
                df_sorted_1d = df.sort_values('rank_change_1d', ascending=False)
                result_text.append(f"📈 一天内排名上升速度排行（共{len(df_sorted_1d)}个板块）:")
                result_text.append("-" * 120)
                result_text.append(f"{'排名':<6} {'板块代码':<15} {'Alpha值':<12} {'较昨日上升':<15} {'较前天上升':<15} {'当前排名':<12}")
                result_text.append("-" * 120)
                
                # 保存所有数据，不截断
                for i, row in df_sorted_1d.iterrows():
                    rank = i + 1
                    code = row.get('sector_code', '')
                    alpha = row.get('alpha_score', 0)
                    change_1d = row.get('rank_change_1d', 0)
                    change_2d = row.get('rank_change_2d', 0)
                    current_rank = row.get('current_rank', 0)
                    
                    result_text.append(f"{rank:<6} {code:<15} {alpha*100:>10.2f}% {change_1d:>13.0f} {change_2d:>13.0f} {current_rank:>10.0f}")
            
            result_text.append("")
            
            # 按2日上升位数排序 - 保存所有数据
            if 'rank_change_2d' in df.columns:
                df_sorted_2d = df.sort_values('rank_change_2d', ascending=False)
                result_text.append(f"📈 两天内排名上升速度排行（共{len(df_sorted_2d)}个板块）:")
                result_text.append("-" * 120)
                result_text.append(f"{'排名':<6} {'板块代码':<15} {'Alpha值':<12} {'较昨日上升':<15} {'较前天上升':<15} {'当前排名':<12}")
                result_text.append("-" * 120)
                
                # 保存所有数据，不截断
                for i, row in df_sorted_2d.iterrows():
                    rank = i + 1
                    code = row.get('sector_code', '')
                    alpha = row.get('alpha_score', 0)
                    change_1d = row.get('rank_change_1d', 0)
                    change_2d = row.get('rank_change_2d', 0)
                    current_rank = row.get('current_rank', 0)
                    
                    result_text.append(f"{rank:<6} {code:<15} {alpha*100:>10.2f}% {change_1d:>13.0f} {change_2d:>13.0f} {current_rank:>10.0f}")
            
            result_content = "\n".join(result_text)
            save_result_to_file(result_content, "任务4：东财热门概念板块Alpha增长速度排行", task_number=4)
            print("✓ 任务4完成\n")
            
except Exception as e:
    import traceback
    error_msg = f"任务4执行失败: {str(e)}\n详细信息:\n{traceback.format_exc()}"
    print(error_msg)
    save_result_to_file(error_msg, "任务4：东财热门概念板块Alpha增长速度排行（错误）", task_number=4)

# 任务5：获取涨跌停股票数据
print("=" * 80)
print("任务5：获取涨跌停股票数据")
print("=" * 80)
try:
    pro = ts.pro_api()
    
    # 获取涨跌停数据
    df = pro.limit_list_d(trade_date=end_date)
    
    if df.empty:
        result_content = f"未找到{end_date}的涨跌停股票数据\n\n提示：\n- 请确认该日期是否为交易日\n- 该日期是否有股票涨跌停或炸板\n- 注意：本接口不提供ST股票的统计"
        save_result_to_file(result_content, "任务5：涨跌停股票数据（错误）", task_number=5)
        print("⚠️ 未找到涨跌停数据\n")
    else:
        # 格式化输出
        result_text = []
        result_text.append(f"📊 涨跌停股票数据")
        result_text.append(f"分析日期: {end_date}")
        result_text.append("")
        
        # 统计信息
        if 'limit' in df.columns:
            limit_stats = df['limit'].value_counts()
            result_text.append("📊 统计信息：")
            result_text.append("-" * 120)
            limit_type_map = {'U': '涨停', 'D': '跌停', 'Z': '炸板'}
            for limit_val, count in limit_stats.items():
                type_name = limit_type_map.get(str(limit_val), str(limit_val))
                result_text.append(f"  - {type_name}: {count} 只")
            result_text.append("")
        
        # 按连板数排序（降序），然后按封单金额排序（降序）
        if 'limit_times' in df.columns:
            df = df.sort_values(['limit_times', 'fd_amount'], ascending=[False, False], na_position='last')
        elif 'fd_amount' in df.columns:
            df = df.sort_values('fd_amount', ascending=False, na_position='last')
        
        result_text.append(f"共找到 {len(df)} 条涨跌停记录，涉及 {len(df['ts_code'].unique()) if 'ts_code' in df.columns else len(df)} 只股票")
        result_text.append("")
        result_text.append(f"{'交易日期':<12} {'股票代码':<15} {'股票名称':<20} {'行业':<15} {'类型':<8} {'收盘价':<10} {'涨跌幅':<10} {'成交额(元)':<18} {'封单金额(元)':<18} {'首次封板':<12} {'最后封板':<12} {'炸板次数':<10} {'连板数':<8} {'涨停统计':<15}")
        result_text.append("-" * 200)
        
        # 保存所有数据，不截断
        for _, row in df.iterrows():
            trade_date_str = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
            code = str(row.get('ts_code', '-'))[:13]
            name = str(row.get('name', '-'))[:18]
            industry = str(row.get('industry', '-'))[:13]
            limit_val = str(row.get('limit', '-'))
            limit_type_map = {'U': '涨停', 'D': '跌停', 'Z': '炸板'}
            limit_type_name = limit_type_map.get(limit_val, limit_val)
            close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
            pct_chg = f"{row.get('pct_chg', 0):+.2f}%" if pd.notna(row.get('pct_chg')) else "-"
            amount = format_large_number(row.get('amount', 0)) if pd.notna(row.get('amount')) else "-"
            fd_amount = format_large_number(row.get('fd_amount', 0)) if pd.notna(row.get('fd_amount')) else "-"
            first_time = str(row.get('first_time', '-'))[:10] if pd.notna(row.get('first_time')) else "-"
            last_time = str(row.get('last_time', '-'))[:10] if pd.notna(row.get('last_time')) else "-"
            open_times = str(int(row.get('open_times', 0))) if pd.notna(row.get('open_times')) else "-"
            limit_times = str(int(row.get('limit_times', 0))) if pd.notna(row.get('limit_times')) else "-"
            up_stat = str(row.get('up_stat', '-'))[:13] if pd.notna(row.get('up_stat')) else "-"
            
            result_text.append(f"{trade_date_str:<12} {code:<15} {name:<20} {industry:<15} {limit_type_name:<8} {close:<10} {pct_chg:<10} {amount:<18} {fd_amount:<18} {first_time:<12} {last_time:<12} {open_times:<10} {limit_times:<8} {up_stat:<15}")
        
        result_text.append("")
        result_text.append("📊 详细统计：")
        result_text.append("-" * 120)
        
        if 'ts_code' in df.columns:
            result_text.append(f"涉及股票数量: {len(df['ts_code'].unique())} 只")
        
        if 'trade_date' in df.columns:
            result_text.append(f"涉及交易日期: {len(df['trade_date'].unique())} 个")
        
        # 计算总成交额
        if 'amount' in df.columns:
            total_amount = df['amount'].sum()
            result_text.append(f"总成交额: {format_large_number(total_amount)} 元")
        
        # 计算总封单金额
        if 'fd_amount' in df.columns:
            total_fd_amount = df['fd_amount'].sum()
            result_text.append(f"总封单金额: {format_large_number(total_fd_amount)} 元")
        
        # 统计连板情况
        if 'limit_times' in df.columns:
            max_limit_times = df['limit_times'].max()
            if pd.notna(max_limit_times):
                result_text.append(f"最高连板数: {int(max_limit_times)} 板")
        
        # 统计炸板情况
        if 'open_times' in df.columns:
            total_open_times = df['open_times'].sum()
            result_text.append(f"总炸板次数: {int(total_open_times)} 次")
            avg_open_times = df['open_times'].mean()
            if pd.notna(avg_open_times):
                result_text.append(f"平均炸板次数: {avg_open_times:.2f} 次")
        
        result_text.append("")
        result_text.append("📝 说明：")
        result_text.append("  - 数据来源：Tushare limit_list_d接口")
        result_text.append("  - 数据历史：2020年至今（不提供ST股票的统计）")
        result_text.append("  - 类型说明：U=涨停，D=跌停，Z=炸板")
        result_text.append("  - 封单金额：以涨停价买入挂单的资金总量（跌停无此数据）")
        result_text.append("  - 首次封板时间：股票首次达到涨停价的时间（跌停无此数据）")
        result_text.append("  - 炸板次数：涨停后开板的次数（跌停为开板次数）")
        result_text.append("  - 连板数：个股连续封板数量")
        result_text.append("  - 涨停统计：格式为N/T，表示T天内有N次涨停")
        result_text.append("  - 权限要求：5000积分（每分钟200次，每天总量1万次），8000积分以上（每分钟500次，每天总量不限制）")
        result_text.append("  - 限量：单次最大可获取2500条数据，可通过日期或股票循环提取")
        
        result_content = "\n".join(result_text)
        save_result_to_file(result_content, "任务5：涨跌停股票数据", task_number=5)
        print("✓ 任务5完成\n")
        
except Exception as e:
    import traceback
    error_msg = f"任务5执行失败: {str(e)}\n详细信息:\n{traceback.format_exc()}"
    print(error_msg)
    save_result_to_file(error_msg, "任务5：涨跌停股票数据（错误）", task_number=5)

print("=" * 80)
print("所有任务完成！")
print("=" * 80)
print(f"结果已分别保存到 doc/ 文件夹，文件名格式：{end_date}-{timestamp}-任务X-*.txt")

