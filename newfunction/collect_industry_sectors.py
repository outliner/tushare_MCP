"""
行业板块数据采集脚本
- 读取指定日期所有行业板块信息
- 获取所有行业板块的成分股
- 存入本地数据库
- 支持根据股票代码查询所属板块
"""
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.token_manager import get_tushare_token
from cache.concept_cache_manager import concept_cache_manager
import tushare as ts


def collect_industry_sectors(trade_date: str) -> Dict:
    """
    采集指定日期的所有行业板块数据
    
    参数:
        trade_date: 交易日期（YYYYMMDD格式，如：20251218）
    
    返回:
        包含采集结果的字典
    """
    token = get_tushare_token()
    if not token:
        return {"error": "请先配置Tushare token"}
    
    pro = ts.pro_api(token)
    
    result = {
        "trade_date": trade_date,
        "sectors_count": 0,
        "members_count": 0,
        "skipped_sectors": 0,
        "skipped_members": 0,
        "errors": []
    }
    
    print(f"\n{'='*60}")
    print(f"开始采集 {trade_date} 的行业板块数据")
    print(f"{'='*60}\n")
    
    # Step 1: 获取所有行业板块行情数据（使用dc_daily）
    print(f"[Step 1] 获取行业板块行情数据...")
    try:
        # 检查是否已有该日期的行业板块数据
        existing_sectors = concept_cache_manager.get_concept_daily_data(
            trade_date=trade_date,
            idx_type='行业板块'
        )
        
        if existing_sectors is not None and not existing_sectors.empty:
            existing_codes = set(existing_sectors['ts_code'].unique())
            print(f"  [INFO] 数据库中已有 {len(existing_codes)} 个行业板块数据")
        else:
            existing_codes = set()
        
        # 获取所有行业板块数据
        df_sectors = pro.dc_daily(trade_date=trade_date, idx_type='行业板块')
        
        if df_sectors.empty:
            return {"error": f"{trade_date} 无行业板块数据"}
        
        # 确保有 idx_type 字段
        if 'idx_type' not in df_sectors.columns:
            df_sectors['idx_type'] = '行业板块'
        
        # 筛选出需要采集的板块（数据库中不存在的）
        all_codes = set(df_sectors['ts_code'].unique())
        missing_codes = all_codes - existing_codes
        
        if missing_codes:
            # 只保存缺失的板块数据
            df_to_save = df_sectors[df_sectors['ts_code'].isin(missing_codes)].copy()
            saved_count = concept_cache_manager.save_concept_daily_data(df_to_save)
            result["sectors_count"] = saved_count
            print(f"  [OK] 获取到 {len(df_sectors)} 个行业板块，其中 {len(missing_codes)} 个需要采集，保存 {saved_count} 条行情记录")
        else:
            result["sectors_count"] = len(existing_codes)
            print(f"  [SKIP] 所有 {len(df_sectors)} 个行业板块数据已存在，跳过采集")
        
        # 同时保存到 index_data 表（用于快速查询板块基本信息）
        # 检查 index_data 表是否已有数据
        existing_index = concept_cache_manager.get_concept_index_data(
            trade_date=trade_date,
            idx_type='行业板块'
        )
        
        if existing_index is None or existing_index.empty or len(existing_index) < len(df_sectors):
            # 尝试获取板块名称（如果dc_daily没有name字段，需要从dc_index获取）
            try:
                df_index = pro.dc_index(trade_date=trade_date, idx_type='行业板块')
                if not df_index.empty and 'name' in df_index.columns:
                    # 如果已有部分数据，只保存缺失的
                    if existing_index is not None and not existing_index.empty:
                        existing_index_codes = set(existing_index['ts_code'].unique())
                        df_index_to_save = df_index[~df_index['ts_code'].isin(existing_index_codes)].copy()
                        if not df_index_to_save.empty:
                            concept_cache_manager.save_concept_index_data(df_index_to_save)
                            print(f"  [OK] 补充保存 {len(df_index_to_save)} 条板块基本信息")
                    else:
                        # 保存所有板块基本信息
                        concept_cache_manager.save_concept_index_data(df_index)
                        print(f"  [OK] 保存 {len(df_index)} 条板块基本信息")
            except Exception as e:
                print(f"  [WARN] 获取板块基本信息失败: {str(e)}")
                pass  # 如果获取失败，不影响主流程
        
    except Exception as e:
        error_msg = f"获取行业板块列表失败: {str(e)}"
        print(f"  [ERROR] {error_msg}")
        result["errors"].append(error_msg)
        return result
    
    # Step 2: 获取每个行业板块的成分股
    print(f"\n[Step 2] 获取行业板块成分股...")
    sector_codes = df_sectors['ts_code'].unique().tolist()
    total_members = 0
    skipped_count = 0
    result["skipped_sectors"] = len(existing_codes) if 'existing_codes' in locals() else 0
    
    for idx, sector_code in enumerate(sector_codes, 1):
        try:
            # 检查该板块该日期是否已有成分股数据，且数量>0
            existing_members = concept_cache_manager.get_concept_member_data(
                ts_code=sector_code,
                trade_date=trade_date
            )
            
            if existing_members is not None and not existing_members.empty:
                member_count = len(existing_members)
                if member_count > 0:
                    skipped_count += 1
                    print(f"  [{idx}/{len(sector_codes)}] {sector_code}: 成分股数据已存在({member_count}只)，跳过")
                    continue
                else:
                    # 成分股数量为0，视为失败，需要重新采集
                    print(f"  [{idx}/{len(sector_codes)}] {sector_code}: 成分股数量为0，重新采集")
            
            # 获取该板块的成分股
            df_members = pro.dc_member(ts_code=sector_code, trade_date=trade_date)
            
            if not df_members.empty:
                # 保存成分股数据
                saved_members = concept_cache_manager.save_concept_member_data(df_members)
                total_members += saved_members
                print(f"  [{idx}/{len(sector_codes)}] {sector_code}: {len(df_members)} 只成分股，保存 {saved_members} 条")
            else:
                print(f"  [{idx}/{len(sector_codes)}] {sector_code}: 无成分股数据")
            
            # 避免请求过快，添加小延迟
            if idx % 10 == 0:
                time.sleep(0.5)
                
        except Exception as e:
            error_msg = f"获取 {sector_code} 成分股失败: {str(e)}"
            print(f"  [ERROR] {error_msg}")
            result["errors"].append(error_msg)
            continue
    
    if skipped_count > 0:
        print(f"\n  [INFO] 跳过 {skipped_count} 个已有成分股数据的板块")
    
    result["members_count"] = total_members
    result["skipped_members"] = skipped_count
    
    print(f"\n{'='*60}")
    print(f"采集完成！")
    print(f"  行业板块数: {result['sectors_count']}")
    print(f"  成分股记录数: {result['members_count']}")
    if skipped_count > 0:
        print(f"  跳过板块数: {skipped_count}")
    if result["errors"]:
        print(f"  错误数: {len(result['errors'])}")
    print(f"{'='*60}\n")
    
    return result


def query_stock_sectors(con_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    根据股票代码查询所属行业板块
    
    参数:
        con_code: 股票代码（如：000001.SZ）
        trade_date: 交易日期（YYYYMMDD格式，可选，默认查询最新日期）
    
    返回:
        包含所属板块信息的DataFrame
    """
    # 查询成分股数据
    df_members = concept_cache_manager.get_concept_member_data(
        con_code=con_code,
        trade_date=trade_date
    )
    
    if df_members is None or df_members.empty:
        return pd.DataFrame()
    
    # 如果没有指定日期，使用最新的日期
    if not trade_date:
        latest_date = df_members['trade_date'].max()
        df_members = df_members[df_members['trade_date'] == latest_date]
    
    # 获取板块代码列表
    sector_codes = df_members['ts_code'].unique().tolist()
    
    if not sector_codes:
        return pd.DataFrame()
    
    # 查询板块信息（从 daily_data 表获取，因为使用 dc_daily 采集）
    query_date = trade_date if trade_date else df_members['trade_date'].iloc[0]
    df_sectors = concept_cache_manager.get_concept_daily_data(
        ts_code=','.join(sector_codes),
        idx_type='行业板块',
        trade_date=query_date
    )
    
    if df_sectors is None or df_sectors.empty:
        # 如果 daily_data 没有，尝试从 index_data 获取
        df_sectors = concept_cache_manager.get_concept_index_data(
            ts_code=','.join(sector_codes),
            idx_type='行业板块',
            trade_date=query_date
        )
        if df_sectors is None or df_sectors.empty:
            return pd.DataFrame()
        # 使用 index_data 的字段
        merge_cols = ['ts_code', 'name', 'pct_change', 'total_mv', 'turnover_rate']
    else:
        # 使用 daily_data 的字段
        merge_cols = ['ts_code', 'pct_change', 'turnover_rate']
        # daily_data 可能没有 name 和 total_mv，需要从 index_data 获取
        df_index = concept_cache_manager.get_concept_index_data(
            ts_code=','.join(sector_codes),
            idx_type='行业板块',
            trade_date=query_date
        )
        if df_index is not None and not df_index.empty:
            name_map = dict(zip(df_index['ts_code'], df_index.get('name', '')))
            mv_map = dict(zip(df_index['ts_code'], df_index.get('total_mv', 0)))
            df_sectors['name'] = df_sectors['ts_code'].map(name_map)
            df_sectors['total_mv'] = df_sectors['ts_code'].map(mv_map)
            merge_cols = ['ts_code', 'name', 'pct_change', 'total_mv', 'turnover_rate']
    
    # 合并数据
    available_cols = [col for col in merge_cols if col in df_sectors.columns]
    result = df_members.merge(
        df_sectors[available_cols],
        on='ts_code',
        how='left',
        suffixes=('_member', '_sector')
    )
    
    # 重命名列
    rename_map = {
        'name_sector': '板块名称',
        'name_member': '股票名称',
        'pct_change': '板块涨跌幅',
        'total_mv': '板块总市值',
        'turnover_rate': '板块换手率'
    }
    result = result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns})
    
    # 选择需要的列（只选择存在的列）
    select_cols = ['ts_code', '板块名称', 'con_code', '股票名称', 'trade_date', 
                   '板块涨跌幅', '板块总市值', '板块换手率']
    result = result[[col for col in select_cols if col in result.columns]]
    
    return result


def main():
    """主函数 - 示例用法"""
    import argparse
    
    parser = argparse.ArgumentParser(description='行业板块数据采集工具')
    parser.add_argument('--date', type=str, help='交易日期（YYYYMMDD格式，如：20251218）')
    parser.add_argument('--query', type=str, help='查询股票所属板块（股票代码，如：000001.SZ）')
    parser.add_argument('--query-date', type=str, help='查询日期（YYYYMMDD格式，可选）')
    
    args = parser.parse_args()
    
    if args.query:
        # 查询模式
        print(f"\n查询股票 {args.query} 所属行业板块...")
        df = query_stock_sectors(args.query, args.query_date)
        
        if df.empty:
            print("未找到该股票所属的行业板块")
        else:
            print(f"\n找到 {len(df)} 个行业板块：")
            print(df.to_string(index=False))
    elif args.date:
        # 采集模式
        result = collect_industry_sectors(args.date)
        if "error" in result:
            print(f"错误: {result['error']}")
    else:
        print("请指定 --date 参数进行采集，或 --query 参数进行查询")
        print("\n示例：")
        print("  采集: python collect_industry_sectors.py --date 20251218")
        print("  查询: python collect_industry_sectors.py --query 000001.SZ")


if __name__ == '__main__':
    main()

