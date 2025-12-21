"""数据采集器 - 用于填充 t1_data 表

该模块负责从 Tushare API 和本地缓存中搜集数据，
并将其整合到 t1_data 表中。支持批量操作和指定日期。
"""
import pandas as pd
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Dict, Union
import tushare as ts

# 添加项目根目录到路径
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import CACHE_DB
from config.token_manager import get_tushare_token
from cache.cache_manager import cache_manager
from cache.stock_daily_cache_manager import stock_daily_cache_manager
from cache.daily_basic_cache_manager import daily_basic_cache_manager
from cache.cyq_perf_cache_manager import cyq_perf_cache_manager
from cache.margin_detail_cache_manager import margin_detail_cache_manager

class DataCollector:
    """数据采集器类"""
    
    def __init__(self, db_path: Path = CACHE_DB):
        self.db_path = db_path
        self.pro = ts.pro_api(get_tushare_token())
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        
    def get_latest_trade_date(self) -> str:
        """获取最近一个交易日"""
        df = self.pro.trade_cal(exchange='', is_open='1', 
                               start_date=time.strftime('%Y%m%d', time.localtime(time.time() - 30 * 86400)),
                               end_date=time.strftime('%Y%m%d'))
        if not df.empty:
            return df['cal_date'].iloc[-1]
        return time.strftime('%Y%m%d')

    def get_previous_trade_dates(self, trade_date: str, count: int = 5) -> List[str]:
        """获取指定日期前的若干个交易日"""
        df = self.pro.trade_cal(exchange='', is_open='1', 
                               start_date=time.strftime('%Y%m%d', time.localtime(time.mktime(time.strptime(trade_date, '%Y%m%d')) - 60 * 86400)),
                               end_date=trade_date)
        if not df.empty:
            # 过滤掉大于等于当前日期的，只取之前的
            dates = df[df['cal_date'] < trade_date]['cal_date'].tolist()
            return dates[-count:]
        return []

    def collect_data(self, ts_codes: Union[str, List[str]], trade_date: Optional[str] = None) -> int:
        """
        搜集并填充数据到 t1_data 表
        
        参数:
            ts_codes: 股票代码列表或逗号分隔的字符串
            trade_date: 目标日期（T日），搜集的是 T-1 的数据。如果为 None，则取最近交易日作为T。
        
        返回:
            更新的记录数量
        """
        if isinstance(ts_codes, str):
            ts_codes = [c.strip() for c in ts_codes.split(',')]
            
        if not trade_date:
            trade_date = self.get_latest_trade_date()
            
        # 我们需要的是 T-1 的数据，所以先找到 T 前面的一个交易日
        t_minus_1_dates = self.get_previous_trade_dates(trade_date, 1)
        if not t_minus_1_dates:
            print(f"无法找到 {trade_date} 的前一交易日")
            return 0
        
        t1_date = t_minus_1_dates[0]
        print(f"正在搜集日期为 {t1_date} (T-1, 对应 T={trade_date}) 的数据...")
        
        # 准备数据存储
        collected_data = {code: {'ts_code': code} for code in ts_codes}
        
        # 1. 获取每日指标 (float_share, total_mv)
        self._fill_daily_basic(ts_codes, t1_date, collected_data)
        
        # 2. 获取日线行情 (pre_close, pre_vol, pre_ats)
        self._fill_stock_daily(ts_codes, t1_date, collected_data)
        
        # 3. 获取筹码数据 (winner_rate, cost_concentration)
        self._fill_cyq_perf(ts_codes, t1_date, collected_data)
        
        # 4. 获取融资融券数据 (margin_cap_ratio)
        self._fill_margin_detail(ts_codes, t1_date, collected_data)
        
        # 5. 获取龙虎榜数据 (sum_inst_net, list_count)
        self._fill_top_list_data(ts_codes, t1_date, collected_data)
        
        # 写入数据库
        return self._save_to_t1_data(collected_data)

    def _fill_daily_basic(self, ts_codes, t1_date, collected_data):
        """填充每日指标数据"""
        for code in ts_codes:
            df = daily_basic_cache_manager.get_daily_basic_data(ts_code=code, trade_date=t1_date)
            if df is None or df.empty:
                # 尝试从API获取并存入缓存
                try:
                    df = self.pro.daily_basic(ts_code=code, trade_date=t1_date)
                    if not df.empty:
                        daily_basic_cache_manager.save_daily_basic_data(df)
                except: pass
            
            if df is not None and not df.empty:
                collected_data[code]['float_share'] = float(df['float_share'].iloc[0])
                collected_data[code]['total_mv'] = float(df['total_mv'].iloc[0])

    def _fill_stock_daily(self, ts_codes, t1_date, collected_data):
        """填充日线行情数据"""
        for code in ts_codes:
            df = stock_daily_cache_manager.get_stock_daily_data(ts_code=code, trade_date=t1_date)
            if df is None or df.empty:
                try:
                    df = self.pro.daily(ts_code=code, trade_date=t1_date)
                    if not df.empty:
                        stock_daily_cache_manager.save_stock_daily_data(df)
                except: pass
            
            if df is not None and not df.empty:
                row = df.iloc[0]
                collected_data[code]['pre_close'] = float(row['close'])
                collected_data[code]['pre_vol'] = int(row['vol'])
                # pre_ats = amount / vol (注意单位：amount是千元，vol是手，1手=100股)
                # pre_ats 通常指每手的成交金额或每股的成交金额，这里我们存万元/手
                if row['vol'] > 0:
                    collected_data[code]['pre_ats'] = float(row['amount'] / row['vol']) / 10.0 # 转换为万元

    def _fill_cyq_perf(self, ts_codes, t1_date, collected_data):
        """填充筹码数据"""
        for code in ts_codes:
            df = cyq_perf_cache_manager.get_cyq_perf_data(ts_code=code, trade_date=t1_date)
            if df is None or df.empty:
                try:
                    # 注意：cyq_perf 权限要求较高
                    df = self.pro.cyq_perf(ts_code=code, trade_date=t1_date)
                    if not df.empty:
                        cyq_perf_cache_manager.save_cyq_perf_data(df)
                except: pass
            
            if df is not None and not df.empty:
                row = df.iloc[0]
                collected_data[code]['winner_rate'] = float(row['winner_rate'])
                # 集中度 = (cost_95pct - cost_5pct) / (cost_95pct + cost_5pct)
                c95 = row.get('cost_95pct')
                c5 = row.get('cost_5pct')
                if c95 and c5 and (c95 + c5) > 0:
                    collected_data[code]['cost_concentration'] = float((c95 - c5) / (c95 + c5))

    def _fill_margin_detail(self, ts_codes, t1_date, collected_data):
        """填充融资融券数据"""
        for code in ts_codes:
            df = margin_detail_cache_manager.get_margin_detail_data(ts_code=code, trade_date=t1_date)
            if df is None or df.empty:
                try:
                    df = self.pro.margin_detail(ts_code=code, trade_date=t1_date)
                    if not df.empty:
                        margin_detail_cache_manager.save_margin_detail_data(df)
                except: pass
            
            if df is not None and not df.empty:
                # margin_cap_ratio = 融资余额 / 总市值
                total_mv = collected_data[code].get('total_mv')
                if total_mv and total_mv > 0:
                    # rzye单位是元，total_mv单位是万元
                    collected_data[code]['margin_cap_ratio'] = float(df['rzye'].iloc[0]) / (total_mv * 10000)

    def _fill_top_list_data(self, ts_codes, t1_date, collected_data):
        """填充龙虎榜相关数据 (5日机构净买入, 上榜次数)"""
        # 获取过去5个交易日
        prev_5_dates = self.get_previous_trade_dates(t1_date, 4) + [t1_date]
        
        for code in ts_codes:
            sum_net = 0.0
            l_count = 0
            
            for d in prev_5_dates:
                # 机构净买入
                try:
                    inst_df = cache_manager.get_dataframe('top_inst', trade_date=d, ts_code=code)
                    if inst_df is None or inst_df.empty:
                        inst_df = self.pro.top_inst(trade_date=d, ts_code=code)
                        if not inst_df.empty:
                            cache_manager.set('top_inst', inst_df, trade_date=d, ts_code=code)
                    
                    if inst_df is not None and not inst_df.empty:
                        # net_buy 是万元
                        sum_net += inst_df['net_buy'].sum()
                except: pass
                
                # 上榜次数 (在 top_list 中出现)
                try:
                    list_df = cache_manager.get_dataframe('top_list', trade_date=d, ts_code=code)
                    if list_df is None or list_df.empty:
                        list_df = self.pro.top_list(trade_date=d, ts_code=code)
                        if not list_df.empty:
                            cache_manager.set('top_list', list_df, trade_date=d, ts_code=code)
                    
                    if list_df is not None and not list_df.empty:
                        l_count += 1
                except: pass
                
            collected_data[code]['sum_inst_net'] = sum_net
            collected_data[code]['list_count'] = l_count

    def _save_to_t1_data(self, collected_data: Dict[str, Dict]) -> int:
        """将整理好的数据存入数据库"""
        cursor = self.conn.cursor()
        current_time = time.time()
        count = 0
        
        for code, data in collected_data.items():
            # 只保存有基本信息的记录
            if 'ts_code' not in data: continue
            
            fields = [
                'ts_code', 'float_share', 'total_mv', 'sum_inst_net', 'list_count',
                'winner_rate', 'cost_concentration', 'margin_cap_ratio',
                'pre_close', 'pre_vol', 'pre_ats', 'updated_at'
            ]
            
            values = [
                data.get('ts_code'),
                data.get('float_share'),
                data.get('total_mv'),
                data.get('sum_inst_net', 0.0),
                data.get('list_count', 0),
                data.get('winner_rate'),
                data.get('cost_concentration'),
                data.get('margin_cap_ratio'),
                data.get('pre_close'),
                data.get('pre_vol'),
                data.get('pre_ats'),
                current_time
            ]
            
            placeholders = ','.join(['?'] * len(fields))
            sql = f"INSERT OR REPLACE INTO t1_data ({','.join(fields)}) VALUES ({placeholders})"
            
            try:
                cursor.execute(sql, values)
                count += 1
            except Exception as e:
                print(f"写入 {code} 数据失败: {e}")
                
        self.conn.commit()
        return count

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    # 测试代码
    collector = DataCollector()
    test_codes = ['600519.SH', '000001.SZ', '300750.SZ']
    num = collector.collect_data(test_codes)
    print(f"成功填充 {num} 条数据")
    collector.close()

