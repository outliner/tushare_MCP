"""
批量补录板块强度快照脚本
功能：指定日期范围，为每个交易日生成所有 5 分钟时间点的板块强度快照。

用法：
    python scripts/batch_backfill_snapshots.py -s 20251201 -e 20251229
    python scripts/batch_backfill_snapshots.py --start 20251220 --end 20251229
"""
import os
import sys
from datetime import datetime
from pathlib import Path

# 将项目根目录加入 sys.path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Windows 控制台 UTF-8 编码支持
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from scripts.sector_strength_backfill import run_sector_strength_backfill


# 交易时间点（5分钟粒度，共50个时间点）
TRADING_TIME_POINTS = [
    # 早盘 09:25 - 11:30
    "09:25:00", "09:30:00", "09:35:00", "09:40:00", "09:45:00",
    "09:50:00", "09:55:00", "10:00:00", "10:05:00", "10:10:00",
    "10:15:00", "10:20:00", "10:25:00", "10:30:00", "10:35:00",
    "10:40:00", "10:45:00", "10:50:00", "10:55:00", "11:00:00",
    "11:05:00", "11:10:00", "11:15:00", "11:20:00", "11:25:00",
    # 午盘 13:00 - 15:00
    "13:00:00", "13:05:00", "13:10:00", "13:15:00", "13:20:00",
    "13:25:00", "13:30:00", "13:35:00", "13:40:00", "13:45:00",
    "13:50:00", "13:55:00", "14:00:00", "14:05:00", "14:10:00",
    "14:15:00", "14:20:00", "14:25:00", "14:30:00", "14:35:00",
    "14:40:00", "14:45:00", "14:50:00", "14:55:00", "15:00:00",
]


def get_available_dates(start_date: str, end_date: str) -> list:
    """
    由于通过 CacheManager 代理未暴露直接查询 DISTINCT 日期的 API，
    本回测脚本暂时假定传入的日期区间连续，
    或通过调用 daily_basic API 来推算实际交易日。
    简化起见：直接生成连续日期数组，让后续流程在查不到数据时自然跳过。
    """
    from datetime import timedelta
    dates = []
    curr = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    while curr <= end:
        if curr.weekday() < 5: # 跳过周末
            dates.append(curr.strftime("%Y%m%d"))
        curr += timedelta(days=1)
    return dates


def get_available_times_for_date(trade_date: str) -> list:
    """
    由于远程代理不再暴露查表级别 API，
    我们直接返回标准交易时间点列表。
    内部 backfill 遇到无数据的时刻会自行跳过/失败处理。
    """
    # 过滤掉不存在快照的逻辑原本是为了加速，
    # 现在直接复用全局 TRADING_TIME_POINTS
    return TRADING_TIME_POINTS


def run_batch_backfill(start_date: str, end_date: str, skip_existing: bool = True):
    """
    批量回溯生成板块强度快照
    
    参数:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        skip_existing: 是否跳过已存在的快照（默认 True）
    """
    print(f"=" * 60)
    print(f"📊 批量板块强度快照生成器")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"=" * 60)
    
    # 获取有效交易日
    available_dates = get_available_dates(start_date, end_date)
    
    if not available_dates:
        print(f"❌ 未找到 {start_date} ~ {end_date} 范围内的分时数据")
        return
    
    print(f"📅 找到 {len(available_dates)} 个有效交易日: {available_dates}")
    print()
    
    total_snapshots = 0
    failed_count = 0
    
    for date_idx, trade_date in enumerate(available_dates, 1):
        # 获取该日期实际存在的时间点
        available_times = get_available_times_for_date(trade_date)
        
        if not available_times:
            print(f"⚠️ [{date_idx}/{len(available_dates)}] {trade_date} 无可用时间点，跳过")
            continue
        
        print(f"\n🔄 [{date_idx}/{len(available_dates)}] 处理日期: {trade_date} ({len(available_times)} 个时间点)")
        print(f"-" * 50)
        
        for time_idx, trade_time in enumerate(available_times, 1):
            try:
                print(f"   [{time_idx:2d}/{len(available_times)}] {trade_date} {trade_time}", end=" ... ")
                run_sector_strength_backfill(target_date=trade_date, target_time=trade_time)
                total_snapshots += 1
                print("✅")
            except Exception as e:
                failed_count += 1
                print(f"❌ 错误: {str(e)[:50]}")
    
    print()
    print(f"=" * 60)
    print(f"✅ 批量处理完成!")
    print(f"   成功: {total_snapshots} 个快照")
    print(f"   失败: {failed_count} 个")
    print(f"=" * 60)


def main():
    """主函数：支持命令行参数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='批量补录板块强度快照（基于5分钟分时数据）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/batch_backfill_snapshots.py -s 20251201 -e 20251229
  python scripts/batch_backfill_snapshots.py --start 20251220 --end 20251229
        """
    )
    parser.add_argument('-s', '--start', type=str, required=True,
                        help='开始日期 (YYYYMMDD格式)')
    parser.add_argument('-e', '--end', type=str, required=True,
                        help='结束日期 (YYYYMMDD格式)')
    parser.add_argument('--no-skip', action='store_true',
                        help='不跳过已存在的快照（默认会跳过）')
    
    args = parser.parse_args()
    
    # 验证日期格式
    try:
        datetime.strptime(args.start, "%Y%m%d")
        datetime.strptime(args.end, "%Y%m%d")
    except ValueError:
        print("❌ 日期格式错误，请使用 YYYYMMDD 格式")
        sys.exit(1)
    
    if args.start > args.end:
        print("❌ 开始日期不能晚于结束日期")
        sys.exit(1)
    
    run_batch_backfill(
        start_date=args.start,
        end_date=args.end,
        skip_existing=not args.no_skip
    )


if __name__ == "__main__":
    main()
