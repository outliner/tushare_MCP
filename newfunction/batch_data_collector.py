"""
高性能批量数据采集器 - 支持字段级合并与成功率统计 (兼容旧版 SQLite)
"""
import pandas as pd
import sqlite3
import time
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB
from config.token_manager import get_tushare_token
import tushare as ts

class BatchDataCollector:
    def __init__(self, db_path: Path = CACHE_DB):
        self.db_path = db_path
        self.pro = ts.pro_api(get_tushare_token())
        self.stats = {}

    def collect_for_date(self, t1_date: str):
        """采集指定日期的全市场数据并执行字段级合并"""
        print(f"\n[INFO] 开始处理日期: {t1_date}")
        start_time = time.time()

        # 1. 基础行情 (必需)
        try:
            df_daily = self.pro.daily(trade_date=t1_date)
            if df_daily is None or df_daily.empty:
                print(f"[SKIP] {t1_date} 非交易日或无行情数据")
                return
            df_basic = self.pro.daily_basic(trade_date=t1_date)
        except Exception as e:
            print(f"[ERROR] 基础行情获取失败: {e}")
            return

        # 2. 辅助数据 (尝试获取)
        try:
            df_cyq = self.pro.cyq_perf(trade_date=t1_date)
        except: df_cyq = pd.DataFrame()

        try:
            df_margin = self.pro.margin_detail(trade_date=t1_date)
        except: df_margin = pd.DataFrame()

        try:
            df_inst = self.pro.top_inst(trade_date=t1_date)
            df_list = self.pro.top_list(trade_date=t1_date)
            df_inst_sum = df_inst.groupby('ts_code')['net_buy'].sum().reset_index() if not df_inst.empty else pd.DataFrame()
            df_list_cnt = df_list.groupby('ts_code').size().reset_index(name='list_count') if not df_list.empty else pd.DataFrame()
        except:
            df_inst_sum, df_list_cnt = pd.DataFrame(), pd.DataFrame()

        # 3. 内存整合
        main_df = df_daily[['ts_code', 'close', 'vol', 'amount']].copy()
        main_df.columns = ['ts_code', 'pre_close', 'pre_vol', 'amount_total']
        main_df['trade_date'] = t1_date
        main_df['pre_ats'] = (main_df['amount_total'] / (main_df['pre_vol'] + 1e-9)) / 10.0

        main_df = main_df.merge(df_basic[['ts_code', 'float_share', 'total_mv']], on='ts_code', how='left')
        
        if not df_inst_sum.empty:
            main_df = main_df.merge(df_inst_sum, on='ts_code', how='left')
        if not df_list_cnt.empty:
            main_df = main_df.merge(df_list_cnt, on='ts_code', how='left')
        
        if not df_cyq.empty:
            df_cyq['cost_concentration'] = (df_cyq['cost_95pct'] - df_cyq['cost_5pct']) / (df_cyq['cost_95pct'] + df_cyq['cost_5pct'] + 1e-9)
            main_df = main_df.merge(df_cyq[['ts_code', 'winner_rate', 'cost_concentration']], on='ts_code', how='left')
        
        if not df_margin.empty:
            df_margin_sum = df_margin.groupby('ts_code')['rzye'].sum().reset_index()
            main_df = main_df.merge(df_margin_sum, on='ts_code', how='left')
            main_df['margin_cap_ratio'] = main_df['rzye'] / (main_df['total_mv'] * 10000 + 1e-9)

        main_df.rename(columns={'net_buy': 'sum_inst_net'}, inplace=True)
        main_df['updated_at'] = time.time()
        main_df = main_df.fillna(0)

        # 4. 统计采集质量
        total = len(main_df)
        self.stats[t1_date] = {
            'total': total,
            'basic': int((main_df['total_mv'] > 0).sum()),
            'chip': int((main_df['winner_rate'] > 0).sum()),
            'inst': int((main_df['sum_inst_net'] != 0).sum()),
            'margin': int((main_df['margin_cap_ratio'] > 0).sum())
        }

        # 5. 字段级合并写入 (兼容旧版 SQLite)
        # 逻辑：先将新数据写入临时表，再用 SQL 合并
        conn = sqlite3.connect(self.db_path)
        valid_cols = [
            'ts_code', 'trade_date', 'float_share', 'total_mv', 'sum_inst_net', 'list_count',
            'winner_rate', 'cost_concentration', 'margin_cap_ratio',
            'pre_close', 'pre_vol', 'pre_ats', 'updated_at'
        ]
        main_df[valid_cols].to_sql('temp_t1_data', conn, if_exists='replace', index=False)
        
        # 模拟 UPSERT：先更新已存在的记录，再插入不存在的
        # 更新部分
        set_clauses = []
        for col in valid_cols:
            if col in ['ts_code', 'trade_date']: continue
            set_clauses.append(f"{col} = (SELECT CASE WHEN temp.{col} != 0 THEN temp.{col} ELSE t1_data.{col} END FROM temp_t1_data temp WHERE temp.ts_code = t1_data.ts_code AND temp.trade_date = t1_data.trade_date)")
        
        update_sql = f"""
            UPDATE t1_data SET {", ".join(set_clauses)}, updated_at = {time.time()}
            WHERE EXISTS (SELECT 1 FROM temp_t1_data temp WHERE temp.ts_code = t1_data.ts_code AND temp.trade_date = t1_data.trade_date)
        """
        conn.execute(update_sql)
        
        # 插入部分
        insert_sql = f"""
            INSERT INTO t1_data ({", ".join(valid_cols)})
            SELECT {", ".join(valid_cols)} FROM temp_t1_data temp
            WHERE NOT EXISTS (SELECT 1 FROM t1_data t WHERE t.ts_code = temp.ts_code AND t.trade_date = temp.trade_date)
        """
        conn.execute(insert_sql)
        
        conn.execute("DROP TABLE temp_t1_data")
        conn.commit()
        conn.close()

        print(f"[SUCCESS] {t1_date} 处理完成，耗时: {time.time() - start_time:.2f}s")

    def print_final_report(self):
        print("\n" + "="*60)
        print(" [REPORT] 数据采集成功率汇总报告")
        print("="*60)
        print(f"{'DATE':<10} | {'TOTAL':<6} | {'PRICE':<8} | {'CHIP':<8} | {'INST':<8} | {'MARGIN':<8}")
        print("-" * 60)
        for date, s in self.stats.items():
            t = s['total']
            print(f"{date:<10} | {t:<6} | {s['basic']/t:>7.1%} | {s['chip']/t:>7.1%} | {s['inst']/t:>7.1%} | {s['margin']/t:>7.1%}")
        print("="*60)
        print("Note: Success rates for Inst/Margin are naturally lower.")

if __name__ == '__main__':
    collector = BatchDataCollector()
    collector.collect_for_date("20251219")
    collector.print_final_report()
