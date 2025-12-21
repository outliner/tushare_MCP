"""
计算20251218日筛选股票在20251219日的收益率
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token
import tushare as ts


def calculate_returns(filter_date: str, next_date: str):
    """
    计算筛选股票的收益率
    
    Args:
        filter_date: 筛选日期 (20251218)
        next_date: 下一个交易日 (20251219)
    """
    # 1. 读取筛选结果
    filter_file = project_root / "doc" / f"filter_result_{filter_date}.csv"
    
    if not filter_file.exists():
        print(f"[ERROR] 筛选结果文件不存在: {filter_file}")
        return None
    
    df_filter = pd.read_csv(filter_file, encoding='utf-8-sig')
    print(f"[INFO] 读取筛选结果: {len(df_filter)} 只股票")
    
    # 提取股票代码和筛选日收盘价
    stock_codes = df_filter['ts_code'].tolist()
    filter_prices = dict(zip(df_filter['ts_code'], df_filter['close']))
    
    # 2. 获取下一个交易日的行情数据
    pro = ts.pro_api(get_tushare_token())
    print(f"[INFO] 获取 {next_date} 日行情数据...")
    
    df_next = pro.daily(trade_date=next_date)
    
    if df_next is None or df_next.empty:
        print(f"[ERROR] {next_date} 无行情数据")
        return None
    
    print(f"[INFO] {next_date} 日行情数据: {len(df_next)} 条")
    
    # 3. 匹配股票并计算收益率
    result_list = []
    
    for ts_code in stock_codes:
        filter_price = filter_prices[ts_code]
        
        # 查找下一个交易日的行情
        next_row = df_next[df_next['ts_code'] == ts_code]
        
        if next_row.empty:
            # 股票可能停牌或退市
            result_list.append({
                'ts_code': ts_code,
                'filter_date': filter_date,
                'filter_close': filter_price,
                'next_date': next_date,
                'next_close': None,
                'return_pct': None,
                'status': '停牌/无数据'
            })
        else:
            next_close = next_row.iloc[0]['close']
            return_pct = (next_close - filter_price) / filter_price * 100
            
            result_list.append({
                'ts_code': ts_code,
                'filter_date': filter_date,
                'filter_close': filter_price,
                'next_date': next_date,
                'next_close': next_close,
                'return_pct': return_pct,
                'status': '正常'
            })
    
    # 4. 生成结果DataFrame
    df_result = pd.DataFrame(result_list)
    
    # 合并原始筛选信息
    df_result = df_result.merge(
        df_filter[['ts_code', 'pct_chg', 'turnover_rate', 'winner_rate', 'Chip_Tier', 'Final_Score']],
        on='ts_code',
        how='left'
    )
    
    # 5. 统计分析
    valid_returns = df_result[df_result['return_pct'].notna()]['return_pct']
    
    print(f"\n{'='*80}")
    print(f"收益率分析报告")
    print(f"{'='*80}")
    print(f"筛选日期: {filter_date}")
    print(f"计算日期: {next_date}")
    print(f"筛选股票数: {len(df_result)}")
    print(f"有效数据数: {len(valid_returns)}")
    print(f"停牌/无数据: {len(df_result) - len(valid_returns)}")
    
    if len(valid_returns) > 0:
        print(f"\n收益率统计:")
        print(f"  平均收益率: {valid_returns.mean():.3f}%")
        print(f"  中位数收益率: {valid_returns.median():.3f}%")
        print(f"  最大收益率: {valid_returns.max():.3f}%")
        print(f"  最小收益率: {valid_returns.min():.3f}%")
        print(f"  标准差: {valid_returns.std():.3f}%")
        print(f"  正收益数量: {(valid_returns > 0).sum()} ({((valid_returns > 0).sum() / len(valid_returns) * 100):.1f}%)")
        print(f"  负收益数量: {(valid_returns < 0).sum()} ({((valid_returns < 0).sum() / len(valid_returns) * 100):.1f}%)")
        print(f"  持平数量: {(valid_returns == 0).sum()}")
        
        # 计算胜率（收益率>0）
        win_rate = (valid_returns > 0).sum() / len(valid_returns) * 100
        print(f"\n  胜率: {win_rate:.1f}%")
        
        # 计算平均盈亏比
        positive_returns = valid_returns[valid_returns > 0]
        negative_returns = valid_returns[valid_returns < 0]
        if len(positive_returns) > 0 and len(negative_returns) > 0:
            avg_win = positive_returns.mean()
            avg_loss = abs(negative_returns.mean())
            profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
            print(f"  平均盈利: {avg_win:.3f}%")
            print(f"  平均亏损: {avg_loss:.3f}%")
            print(f"  盈亏比: {profit_loss_ratio:.2f}")
    
    # 6. 详细结果输出
    print(f"\n{'='*80}")
    print(f"详细收益率列表")
    print(f"{'='*80}")
    
    # 按收益率排序
    df_result_sorted = df_result.sort_values('return_pct', ascending=False, na_last=True)
    
    display_cols = ['ts_code', 'filter_close', 'next_close', 'return_pct', 
                    'pct_chg', 'winner_rate', 'Chip_Tier', 'Final_Score', 'status']
    available_cols = [c for c in display_cols if c in df_result_sorted.columns]
    
    print(df_result_sorted[available_cols].to_string(index=False, float_format=lambda x: "{:.3f}".format(x) if pd.notna(x) else "N/A"))
    
    # 7. Top 5 和 Bottom 5
    if len(valid_returns) > 0:
        print(f"\n{'='*80}")
        print(f"Top 5 收益率")
        print(f"{'='*80}")
        top5 = df_result_sorted.head(5)
        print(top5[['ts_code', 'filter_close', 'next_close', 'return_pct']].to_string(index=False, float_format=lambda x: "{:.3f}".format(x)))
        
        print(f"\n{'='*80}")
        print(f"Bottom 5 收益率")
        print(f"{'='*80}")
        bottom5 = df_result_sorted[df_result_sorted['return_pct'].notna()].tail(5)
        print(bottom5[['ts_code', 'filter_close', 'next_close', 'return_pct']].to_string(index=False, float_format=lambda x: "{:.3f}".format(x)))
    
    # 8. 保存结果
    output_file = project_root / "doc" / f"return_analysis_{filter_date}_to_{next_date}.csv"
    df_result_sorted.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n[INFO] 结果已保存至: {output_file}")
    
    return df_result_sorted


def main():
    """主函数"""
    filter_date = "20251218"  # 筛选日期
    next_date = "20251219"    # 下一个交易日
    
    print(f"\n{'#'*80}")
    print(f"# 收益率计算分析")
    print(f"# 筛选日期: {filter_date}")
    print(f"# 计算日期: {next_date}")
    print(f"{'#'*80}\n")
    
    result = calculate_returns(filter_date, next_date)
    
    if result is not None:
        print(f"\n[SUMMARY] 收益率计算完成")


if __name__ == '__main__':
    main()


