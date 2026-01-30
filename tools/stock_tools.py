"""股票相关MCP工具"""
import tushare as ts
import pandas as pd
import json
from typing import TYPE_CHECKING, Optional, List
from datetime import datetime
from config.token_manager import get_tushare_token

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from cache.cache_manager import cache_manager
from cache.stock_daily_cache_manager import stock_daily_cache_manager
from cache.stock_weekly_cache_manager import stock_weekly_cache_manager
from cache.index_daily_cache_manager import index_daily_cache_manager
from cache.stk_surv_cache_manager import stk_surv_cache_manager
from cache.cyq_perf_cache_manager import cyq_perf_cache_manager
from cache.daily_basic_cache_manager import daily_basic_cache_manager
from cache.mapping_cache_manager import mapping_cache_manager
from cache.stock_intraday_cache_manager import stock_intraday_cache_manager
from utils.common import format_date

def register_stock_tools(mcp: "FastMCP"):
    """注册股票相关工具"""
    
    @mcp.tool()
    def get_stock_basic_info(ts_code: str = "", name: str = "") -> str:
        """
        获取股票基本信息
        
        参数:
            ts_code: 股票代码（如：000001.SZ）
            name: 股票名称（如：平安银行）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {'ts_code': ts_code, 'name': name}
            df = cache_manager.get_dataframe('stock_basic', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('stock_basic', **cache_params):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                filters = {}
                if ts_code:
                    filters['ts_code'] = ts_code
                if name:
                    filters['name'] = name
                    
                df = pro.stock_basic(**filters)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('stock_basic', df, **cache_params)
            
            if df.empty:
                return "未找到符合条件的股票"
                
            # 格式化输出
            result = []
            for _, row in df.iterrows():
                # 获取所有可用的列
                available_fields = row.index.tolist()
                
                # 构建基本信息
                info_parts = []
                
                # 必要字段
                if 'ts_code' in available_fields:
                    info_parts.append(f"股票代码: {row['ts_code']}")
                if 'name' in available_fields:
                    info_parts.append(f"股票名称: {row['name']}")
                    
                # 可选字段
                optional_fields = {
                    'area': '所属地区',
                    'industry': '所属行业',
                    'list_date': '上市日期',
                    'market': '市场类型',
                    'exchange': '交易所',
                    'curr_type': '币种',
                    'list_status': '上市状态',
                    'delist_date': '退市日期'
                }
                
                for field, label in optional_fields.items():
                    if field in available_fields and not pd.isna(row[field]):
                        info_parts.append(f"{label}: {row[field]}")
                
                info = "\n".join(info_parts)
                info += "\n------------------------"
                result.append(info)
                
            return "\n".join(result)
            
        except Exception as e:
            return f"查询失败：{str(e)}"

    @mcp.tool()
    def collect_stock_sector_mapping() -> str:
        """
        全量拉取并建立股票与申万二级行业、东财行业、东财概念的映射关系
        
        说明：
        - 这是一个重型工具，会多次请求Tushare和东财接口
        - 结果会持久化到本地数据库，支持后续筛选和分析
        - 执行时间可能较长（几分钟到十几分钟不等）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
            
        try:
            pro = ts.pro_api()
            print("🚀 开始全量映射采集...", file=__import__('sys').stderr)
            
            # 1. 获取所有A股股票基本信息
            print("📥 正在读取股票列表...", file=__import__('sys').stderr)
            df_basic = pro.stock_basic(list_status='L', fields='ts_code,name,industry')
            if df_basic.empty:
                return "未能获取到股票基本信息"
            
            # 初始化汇总字典
            # stock_map = {ts_code: {name, industry, sw_l2_code, sw_l2_name, em_ind_code, em_ind_name, em_concept_codes: [], em_concept_names: []}}
            stock_map = {}
            for _, row in df_basic.iterrows():
                code = row['ts_code']
                stock_map[code] = {
                    'ts_code': code,
                    'name': row['name'],
                    'sw_l2_code': '',
                    'sw_l2_name': '',
                    'em_industry_code': '',
                    'em_industry_name': '',
                    'em_concept_codes': [],
                    'em_concept_names': []
                }

            # 2. 采集申万二级行业映射
            print("正在采集申万二级行业映射...", file=__import__('sys').stderr)
            # 获取所有申万二级行业分类 (SW2021)
            sw_l2_classify = pro.index_classify(level='L2', src='SW2021')
            if not sw_l2_classify.empty:
                for _, ind in sw_l2_classify.iterrows():
                    l2_code = ind['index_code']
                    l2_name = ind['industry_name']
                    # 获取该行业成分股
                    try:
                        members = pro.index_member_all(l2_code=l2_code)
                        if members is not None and not members.empty:
                            for _, member in members.iterrows():
                                m_code = member['ts_code']
                                if m_code in stock_map:
                                    stock_map[m_code]['sw_l2_code'] = l2_code
                                    stock_map[m_code]['sw_l2_name'] = l2_name
                    except:
                        continue

            # 3. 采集东财行业和概念映射
            # 导入现有的采集工具以保持逻辑一致
            from tools.concept_tools import get_dc_board_codes
            
            # 3.1 东财行业
            print("正在采集东财行业板块映射...", file=__import__('sys').stderr)
            ind_boards_str = get_dc_board_codes(board_type='行业板块')
            # get_dc_board_codes 返回的是格式化字符串，我们需要解析它或直接调用接口
            # 简化起见，我们直接调用接口获取代码列表
            try:
                # pro.dc_index 只支持概念，我们需要用 dc_daily 或其他方式获取行业列表
                # 实际上从 pro.dc_index 获取所有代码更稳妥
                all_boards = pro.dc_index() # 获取东财所有指数信息
                if not all_boards.empty:
                    # 行业板块
                    industry_boards = all_boards[all_boards['type'] == '行业']
                    for _, board in industry_boards.iterrows():
                        b_code = board['ts_code']
                        b_name = board['name']
                        try:
                            m = pro.dc_index_member(ts_code=b_code)
                            if m is not None and not m.empty:
                                for _, member in m.iterrows():
                                    m_code = member['con_code']
                                    if m_code in stock_map:
                                        stock_map[m_code]['em_industry_code'] = b_code
                                        stock_map[m_code]['em_industry_name'] = b_name
                        except:
                            continue
                    
                    # 3.2 东财概念
                    print(" 正在采集东财概念板块映射...", file=__import__('sys').stderr)
                    concept_boards = all_boards[all_boards['type'] == '概念']
                    for _, board in concept_boards.iterrows():
                        b_code = board['ts_code']
                        b_name = board['name']
                        try:
                            m = pro.dc_index_member(ts_code=b_code)
                            if m is not None and not m.empty:
                                for _, member in m.iterrows():
                                    m_code = member['con_code']
                                    if m_code in stock_map:
                                        if b_code not in stock_map[m_code]['em_concept_codes']:
                                            stock_map[m_code]['em_concept_codes'].append(b_code)
                                            stock_map[m_code]['em_concept_names'].append(b_name)
                        except:
                            continue
            except Exception as e:
                print(f"采集东财板块数据出错: {str(e)}", file=__import__('sys').stderr)

            # 4. 汇总与入库
            print("正在同步同步到本地数据库...", file=__import__('sys').stderr)
            final_list = list(stock_map.values())
            df_final = pd.DataFrame(final_list)
            
            saved_count = mapping_cache_manager.save_mapping(df_final)
            
            return f"全量映射采集完成！\n- 扫描股票总数: {len(df_basic)}\n- 成功入库/更新记录: {saved_count}\n- 包含申万L2、东财行业及概念板块映射数据"
            
        except Exception as e:
            import traceback
            return f"映射采集失败: {str(e)}\n{traceback.format_exc()}"
    
    @mcp.tool()
    def get_stock_sector_mapping(ts_code: str) -> str:
        """
        获取单只股票的申万二级行业、东财行业及概念映射
        
        参数:
            ts_code: 股票代码（如：600519.SH）
        """
        try:
            mapping = mapping_cache_manager.get_mapping_by_code(ts_code)
            if not mapping:
                return f"未找到股票 {ts_code} 的映射数据。请先运行 collect_stock_sector_mapping 进行同步。"
            
            result = [
                f"股票: {mapping['name']} ({mapping['ts_code']})",
                f"申万二级行业: {mapping['sw_l2_name']} ({mapping['sw_l2_code']})",
                f"东财行业: {mapping['em_industry_name']} ({mapping['em_industry_code']})",
                f"东财概念: {', '.join(mapping['em_concept_names'])}",
                f"更新时间: {datetime.fromtimestamp(mapping['updated_at']).strftime('%Y-%m-%d %H:%M:%S')}"
            ]
            return "\n".join(result)
        except Exception as e:
            return f"查询失败: {str(e)}"

    @mcp.tool()
    def get_stocks_by_sector(sector_code: str, sector_type: str = "em_concept") -> str:
        """
        根据板块代码获取所属的所有股票
        
        参数:
            sector_code: 板块代码（如：BK1184.DC, 801053.SI）
            sector_type: 板块类型 ('sw_l2', 'em_industry', 'em_concept')
        """
        try:
            df = mapping_cache_manager.search_by_sector(sector_type, sector_code)
            if df.empty:
                return f"未找到该板块下的股票。请确保代码正确且已运行 collect_stock_sector_mapping 同步。"
            
            # 格式化输出
            result = [f"### 板块 {sector_code} 下的股票列表 ({len(df)} 只):\n"]
            result.append("| 股票代码 | 股票名称 | 申万二级 | 东财行业 |")
            result.append("| --- | --- | --- | --- |")
            
            for _, row in df.iterrows():
                result.append(f"| {row['ts_code']} | {row['name']} | {row['sw_l2_name']} | {row['em_industry_name']} |")
                
            return "\n".join(result)
        except Exception as e:
            return f"查询失败: {str(e)}"
    
    @mcp.tool()
    def get_stock_intraday_history(ts_code: str, trade_date: str, trade_time: str) -> str:
        """
        获取单只股票在历史某一时刻的快照数据（用于同刻量比计算）
        
        参数:
            ts_code: 股票代码
            trade_date: 历史日期 (YYYYMMDD)
            trade_time: 历史时间 (HH:MM:SS)
        """
        try:
            snapshot = stock_intraday_cache_manager.get_historical_snapshot(ts_code, trade_date, trade_time)
            if not snapshot:
                return f"未找到股票 {ts_code} 在 {trade_date} {trade_time} 之前的快照数据。"
            
            result = [
                f"### 历史时刻快照数据: {ts_code}",
                f"- **匹配日期**: {snapshot['trade_date']}",
                f"- **匹配时刻**: {snapshot['trade_time']}",
                f"- **当时价格**: {snapshot['close']}",
                f"- **累计成交量**: {snapshot['vol']} 手",
                f"- **累计成交额**: {snapshot['amount']} 千元",
                f"- **数据采集时间**: {datetime.fromtimestamp(snapshot['created_at']).strftime('%Y-%m-%d %H:%M:%S')}"
            ]
            return "\n".join(result)
        except Exception as e:
            return f"查询失败: {str(e)}"

    @mcp.tool()
    def scan_realtime_strong_sectors(sector_type: str = "em_concept", top_n: int = 15, sim_date: str = "", sim_time: str = "") -> str:
        """
        实时强势板块扫描：横向分析板块强度（增强版：多维加权 + 动态修正 + 画像标签）
        
        参数:
            sector_type: 板块维度 ('sw_l2', 'em_industry', 'em_concept')
            top_n: 返回排名前N的板块
            sim_date: 模拟日期（YYYYMMDD格式，可选，用于历史回放）
            sim_time: 模拟时间（HH:MM:SS格式，可选，用于历史回放）
        
        说明:
            - 不指定 sim_date/sim_time 时，使用实时 API 抓取当前行情
            - 指定 sim_date 和 sim_time 时，从历史快照数据库读取数据进行模拟
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
            
        try:
            pro = ts.pro_api()
            
            # 判断是否为模拟模式
            is_simulation = bool(sim_date and sim_time)
            
            if is_simulation:
                # 模拟模式：使用历史快照数据
                trade_date = sim_date.strip()
                current_time_str = sim_time.strip()
                print(f"[模拟模式] 📅 日期: {trade_date}, 时间: {current_time_str}", file=__import__('sys').stderr)
                
                # 从历史快照数据库获取数据
                df_rt = stock_intraday_cache_manager.get_all_snapshots_for_time(trade_date, current_time_str)
                if df_rt.empty:
                    return f"未找到 {trade_date} {current_time_str} 的历史快照数据。请确保该时间点有采集数据。"
                
                # 设置索引
                df_rt = df_rt.set_index('ts_code')
                
                # 计算涨跌幅（从 pre_close 计算，如果没有 pre_close 则需要获取）
                # 历史快照可能没有 pre_close，需要从日线数据补充
                if 'pre_close' not in df_rt.columns or df_rt['pre_close'].isna().all():
                    # 尝试从日线缓存获取前一日收盘价
                    try:
                        from cache.stock_daily_cache_manager import stock_daily_cache_manager
                        # 获取前一交易日
                        prev_date_df = stock_daily_cache_manager.get_stock_daily_data(trade_date=trade_date)
                        if prev_date_df is not None and not prev_date_df.empty:
                            prev_close_map = prev_date_df.set_index('ts_code')['pre_close'].to_dict()
                            df_rt['pre_close'] = df_rt.index.map(lambda x: prev_close_map.get(x))
                    except:
                        pass
                
                # 计算涨跌幅
                if 'pct_chg' not in df_rt.columns or df_rt['pct_chg'].isna().all():
                    if 'pre_close' in df_rt.columns:
                        df_rt['pct_chg'] = (df_rt['close'] - df_rt['pre_close']) / df_rt['pre_close'] * 100
                    else:
                        df_rt['pct_chg'] = 0
            else:
                # 实时模式：使用 API 抓取
                now = datetime.now()
                current_time_str = now.strftime("%H:%M:%S")
                trade_date = now.strftime("%Y%m%d")
                
                # 1. 获取全市场实时行情（增强字段）
                print(f"[{current_time_str}] 🚀 正在抓取实时行情...", file=__import__('sys').stderr)
                fields = 'ts_code,open,close,high,low,pre_close,pct_chg,vol,amount,num,bid_volume1,ask_volume1'
                patterns = ['6*.SH', '0*.SZ', '3*.SZ', '4*.BJ', '8*.BJ']
                rt_dfs = []
                for p in patterns:
                    try:
                        df_p = pro.rt_k(ts_code=p, fields=fields)
                        if df_p is not None and not df_p.empty:
                            rt_dfs.append(df_p)
                    except:
                        continue
                
                if not rt_dfs:
                    return "未能获取到实时行情数据，请检查网络或API权限。"
                df_rt = pd.concat(rt_dfs).set_index('ts_code')
                
                # 确保数值列类型正确
                numeric_cols = ['open', 'close', 'high', 'low', 'pre_close', 'pct_chg', 'vol', 'amount', 'num', 'bid_volume1', 'ask_volume1']
                for col in numeric_cols:
                    if col in df_rt.columns:
                        df_rt[col] = pd.to_numeric(df_rt[col], errors='coerce')
                
                # 计算涨跌幅（如果缺失）
                if 'pct_chg' not in df_rt.columns or df_rt['pct_chg'].isna().all():
                    df_rt['pct_chg'] = (df_rt['close'] - df_rt['pre_close']) / df_rt['pre_close'] * 100
            
            # 2. 获取股票-板块映射
            from cache.mapping_cache_manager import mapping_cache_manager
            from cache.sector_strength_cache_manager import sector_strength_cache_manager
            db_conn = mapping_cache_manager.conn
            df_mapping = pd.read_sql_query("SELECT * FROM stock_sector_mapping", db_conn)
            
            if df_mapping.empty:
                return "映射数据库为空，请先运行 collect_stock_sector_mapping。"

            # 3. 准备昨日数据对比日期（用于量比计算）
            cursor = db_conn.cursor()
            if is_simulation:
                # 模拟模式：查找该日期之前的最近交易日
                cursor.execute("SELECT MAX(trade_date) FROM stock_intraday_data WHERE trade_date < ?", (trade_date,))
            else:
                cursor.execute("SELECT MAX(trade_date) FROM stock_intraday_data")
            last_date_row = cursor.fetchone()
            hist_date = last_date_row[0] if last_date_row and last_date_row[0] else None

            # 4. 聚合计算（多维度）
            sector_data = {}
            
            for _, row in df_mapping.iterrows():
                ts_code = row['ts_code']
                if ts_code not in df_rt.index:
                    continue
                
                target_sectors = []
                if sector_type == 'sw_l2':
                    if row['sw_l2_name']: target_sectors.append(row['sw_l2_name'])
                elif sector_type == 'em_industry':
                    if row['em_industry_name']: target_sectors.append(row['em_industry_name'])
                elif sector_type == 'em_concept':
                    try:
                        names = json.loads(row['em_concept_names'])
                        target_sectors.extend(names)
                    except: pass
                
                rt_row = df_rt.loc[ts_code]
                pct = rt_row.get('pct_chg', 0) or 0
                close = rt_row.get('close', 0) or 0
                open_price = rt_row.get('open', 0) or 0
                high = rt_row.get('high', 0) or 0
                low = rt_row.get('low', 0) or 0
                pre_close = rt_row.get('pre_close', 0) or 0
                amount = rt_row.get('amount', 0) or 0
                num = rt_row.get('num', 0) or 0
                bid_vol = rt_row.get('bid_volume1', 0) or 0
                ask_vol = rt_row.get('ask_volume1', 0) or 0
                
                # 日内位阶
                price_range = high - low
                intraday_pos = (close - low) / price_range if price_range > 0 else 0.5
                
                # 真阳线
                is_real_body = 1 if close > open_price else 0
                
                # 量比计算
                vol_now = rt_row.get('vol', 0) or 0
                vol_hist = 0
                if hist_date:
                    hist_snap = stock_intraday_cache_manager.get_historical_snapshot(ts_code, hist_date, current_time_str)
                    if hist_snap:
                        try:
                            t1 = datetime.strptime(current_time_str, "%H:%M:%S")
                            t2 = datetime.strptime(hist_snap['trade_time'], "%H:%M:%S")
                            if abs((t1 - t2).total_seconds()) <= 600:
                                vol_hist = hist_snap.get('vol', 0)
                        except: pass
                
                for s_name in target_sectors:
                    if s_name not in sector_data:
                        sector_data[s_name] = {
                            'count': 0, 'pct_list': [], 'sum_vol_now': 0, 'sum_vol_hist': 0, 
                            'rising': 0, 'sum_intraday_pos': 0, 'real_body_count': 0,
                            'sum_amount': 0, 'sum_num': 0, 'sum_bid': 0, 'sum_ask': 0
                        }
                    d = sector_data[s_name]
                    d['count'] += 1
                    d['pct_list'].append(pct)
                    d['sum_vol_now'] += vol_now
                    d['sum_vol_hist'] += vol_hist
                    d['sum_intraday_pos'] += intraday_pos
                    d['real_body_count'] += is_real_body
                    d['sum_amount'] += amount
                    d['sum_num'] += num
                    d['sum_bid'] += bid_vol
                    d['sum_ask'] += ask_vol
                    if pct > 0: d['rising'] += 1

            # 5. 计算多维度评分
            results = []
            for name, d in sector_data.items():
                if d['count'] < 3: continue
                
                # 广度维度
                avg_pct = sum(d['pct_list']) / d['count']
                rising_ratio = d['rising'] / d['count'] * 100
                median_return = sorted(d['pct_list'])[len(d['pct_list']) // 2]
                
                # 质量维度
                avg_intraday_pos = d['sum_intraday_pos'] / d['count']
                real_body_ratio = d['real_body_count'] / d['count'] * 100
                
                # 量能维度
                volume_ratio = d['sum_vol_now'] / d['sum_vol_hist'] if d['sum_vol_hist'] > 0 else 1.0
                value_per_trade = (d['sum_amount'] * 1000) / d['sum_num'] if d['sum_num'] > 0 else 0
                
                # 盘口维度
                total_order = d['sum_bid'] + d['sum_ask']
                order_imbalance = (d['sum_bid'] - d['sum_ask']) / total_order * 100 if total_order > 0 else 0
                
                # === 新评分公式 (100分制) ===
                # 攻击力 (50%): avg_intraday_pos * 30 + avg_pct 归一化 * 20
                attack_score = avg_intraday_pos * 30 + min(max(avg_pct, -5), 10) / 10 * 20
                
                # 稳定性 (30%): real_body_ratio * 0.15 + rising_ratio * 0.15
                stability_score = real_body_ratio * 0.15 + rising_ratio * 0.15
                
                # 资金面 (20%): value_per_trade 归一化 * 10 + volume_ratio 归一化 * 10
                # value_per_trade: 1万元/笔 以上即为满分
                vpt_score = min(value_per_trade / 10000, 1.0) * 10
                # volume_ratio: 1.5-3 满分, <0.8 或 >5 扣分
                if 1.5 <= volume_ratio <= 3.0:
                    vr_score = 10
                elif volume_ratio < 0.8:
                    vr_score = volume_ratio / 0.8 * 5
                elif volume_ratio > 5:
                    vr_score = max(0, 10 - (volume_ratio - 5) * 2)
                else:
                    vr_score = 7  # 1.0-1.5 or 3.0-5.0
                capital_score = vpt_score + vr_score
                
                base_score = attack_score + stability_score + capital_score
                
                # === 纵向动态修正 ===
                momentum_factor = 1.0
                divergence_penalty = False
                order_flow_surge = False  # 盘口突变标记
                prev_order_imbalance = None  # 上一周期委比
                
                try:
                    hist_df = sector_strength_cache_manager.get_strength_history(name, trade_date, limit=3)
                    if hist_df is not None and len(hist_df) >= 3:
                        scores = hist_df['score'].tolist()
                        if all(scores[i] < scores[i+1] for i in range(len(scores)-1)):
                            momentum_factor = 1.15  # +15% 动能溢价
                        # 背离检测
                        if len(hist_df) >= 2:
                            prev = hist_df.iloc[-2]
                            if avg_pct > prev['avg_pct'] and avg_intraday_pos < prev.get('avg_intraday_pos', avg_intraday_pos):
                                divergence_penalty = True
                            # 盘口突变检测：委比由负转正且斜率陡增
                            prev_order_imbalance = prev.get('order_imbalance', None)
                            if prev_order_imbalance is not None and prev_order_imbalance < 0 and order_imbalance > 0:
                                # 斜率陡增：当前委比 - 上一周期委比 > 30
                                if (order_imbalance - prev_order_imbalance) > 30:
                                    order_flow_surge = True
                except:
                    pass
                
                final_score = base_score * momentum_factor
                if divergence_penalty:
                    final_score *= 0.8  # -20% 背离惩罚
                
                # === 板块画像标签 ===
                tag = "✅ 稳健"
                if final_score > 85 and avg_intraday_pos > 0.9:
                    tag = "🚀 坦克突击型"
                elif order_flow_surge:
                    tag = "🔥 异动抢筹型"  # 盘口突变：委比由负转正且斜率陡增
                elif momentum_factor > 1.0 and base_score < 60:
                    # 极速异动型：Score提升极快但绝对值尚低
                    tag = "⚡ 极速异动型"
                elif avg_pct > 2 and real_body_ratio < 50:
                    tag = "⚠️ 高开低走陷阱"
                elif divergence_penalty:
                    tag = "🔻 虚假繁荣"
                
                results.append({
                    '板块名称': name,
                    '平均涨幅': f"{avg_pct:.2f}%",
                    '日内位阶': f"{avg_intraday_pos:.2f}",
                    '真阳线': f"{real_body_ratio:.0f}%",
                    '量比': f"{volume_ratio:.2f}",
                    '成分数': d['count'],
                    'score': final_score,
                    '标签': tag
                })
            
            if not results:
                return "未扫描到显著强势的板块。"
            
            df_res = pd.DataFrame(results).sort_values('score', ascending=False).head(top_n)
            if df_res.empty: return "未扫描到显著强势的板块。"

            output = [f"### {'📅 模拟回放' if is_simulation else '实时'}强势板块扫描 (维度: {sector_type}, {'日期: ' + trade_date + ', ' if is_simulation else ''}时间: {current_time_str})"]
            output.append("| 板块名称 | 涨幅 | 位阶 | 真阳 | 量比 | 成分 | 评分 | 标签 |")
            output.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
            for _, r in df_res.iterrows():
                output.append(f"| {r['板块名称']} | {r['平均涨幅']} | {r['日内位阶']} | {r['真阳线']} | {r['量比']} | {r['成分数']} | {r['score']:.1f} | {r['标签']} |")
            return "\n".join(output)
        except Exception as e:
            import traceback
            return f"扫描失败: {str(e)}\n{traceback.format_exc()}"


    @mcp.tool()
    def analyze_sector_health(sector_type: str = "em_industry", benchmark_code: str = "000001.SH", top_n: int = 15, sim_date: str = "", sim_time: str = "") -> str:
        """
        板块走势健康度分析，纵向分析当前板块强度（基于时序统计和线性回归，支持模拟回放）
        
        参数:
            sector_type: 板块维度 ('sw_l2', 'em_industry')
            benchmark_code: 基准指数 (默认 000001.SH 上证指数)
            top_n: 返回前N个健康板块
            sim_date: 模拟日期（YYYYMMDD格式，可选，用于历史回放）
            sim_time: 模拟时间（HH:MM:SS格式，可选，用于历史回放，筛选该时间之前的数据）
            
        返回:
            JSON格式结果，包含:
            - health_score: 综合健康分 (0-100)
            - slope_status: 趋势状态 (爆发/稳健/衰减)
            - warning_tags: 背离预警标签列表
            
        说明:
            - 不指定 sim_date/sim_time 时，使用当天最新数据
            - 指定 sim_date 和 sim_time 时，回放历史某一时刻的健康度数据
        """
        token = get_tushare_token()
        if not token: return "请查询Tushare token"
        
        try:
            import numpy as np
            pro = ts.pro_api()
            
            # 判断是否为模拟模式
            is_simulation = bool(sim_date and sim_time)
            
            if is_simulation:
                trade_date = sim_date.strip()
                current_time_str = sim_time.strip()
                print(f"[模拟模式] 📅 日期: {trade_date}, 时间: {current_time_str}", file=__import__('sys').stderr)
            else:
                now = datetime.now()
                trade_date = now.strftime("%Y%m%d")
                current_time_str = now.strftime("%H:%M:%S")
            
            # 1. 获取基准行情
            benchmark_pct = 0
            if is_simulation:
                # 模拟模式：从日线缓存获取基准数据
                try:
                    df_benchmark = index_daily_cache_manager.get_index_daily_data(
                        ts_code=benchmark_code, trade_date=trade_date
                    )
                    if df_benchmark is not None and not df_benchmark.empty:
                        row = df_benchmark.iloc[0]
                        benchmark_pct = row.get('pct_chg', 0) or 0
                except:
                    pass
            else:
                # 实时模式：从API获取
                df_benchmark = pro.rt_k(ts_code=benchmark_code)
                if not df_benchmark.empty:
                    row = df_benchmark.iloc[0]
                    benchmark_pct = (row['close'] - row['pre_close']) / row['pre_close'] * 100 if row['pre_close'] > 0 else 0

            # 2. 从数据库加载最近4-6周期的评分快照 (20-30分钟)
            from cache.sector_strength_cache_manager import sector_strength_cache_manager
            db_conn = sector_strength_cache_manager.conn
            
            # 获取完整字段用于多维度计算
            # 如果指定了 sim_time，只取该时间之前的数据
            time_filter = f"AND trade_time <= '{current_time_str}'" if is_simulation else ""
            query = f"""
            SELECT sector_name, trade_time, 
                   avg_pct, rising_ratio, median_return,
                   intraday_position, real_body_ratio,
                   volume_ratio, value_per_trade, order_imbalance,
                   score 
            FROM sector_strength_data 
            WHERE trade_date = '{trade_date}' AND sector_type = '{sector_type}' {time_filter}
            ORDER BY sector_name, trade_time DESC
            """
            df_hist = pd.read_sql_query(query, db_conn)
            if df_hist.empty: 
                return json.dumps({"error": f"{'模拟日期 ' + trade_date if is_simulation else '今天'}尚未记录板块强度统计数据，请先运行 sector_strength_collector.py。"}, ensure_ascii=False)
            
            health_results = []
            for name, group in df_hist.groupby('sector_name'):
                if len(group) < 4: continue  # 至少需要4个周期
                
                # 取最近6个周期 (约30分钟)，按时间正序排列
                group = group.head(6).iloc[::-1].reset_index(drop=True)
                n_periods = len(group)
                
                # ============ A. 趋势斜率计算 (权重 40%) ============
                y_score = group['score'].values
                x = np.arange(n_periods)
                slope = np.polyfit(x, y_score, 1)[0] if n_periods > 1 else 0
                
                # 归一化至 [0, 100]：slope_score = clip(slope * 10, -50, 50) + 50
                slope_score = np.clip(slope * 10, -50, 50) + 50
                
                # 盘口加权：order_imbalance 连续2期由负转正且斜率增大，给予10%增益
                order_imb_values = group['order_imbalance'].values
                order_imb_bonus = 1.0
                if n_periods >= 3:
                    # 检查最后2期是否由负转正
                    if (order_imb_values[-3] < 0 and 
                        order_imb_values[-2] > order_imb_values[-3] and 
                        order_imb_values[-1] > order_imb_values[-2] and
                        order_imb_values[-1] > 0 and
                        slope > 0):
                        order_imb_bonus = 1.10
                
                slope_score_final = slope_score * order_imb_bonus
                
                # ============ B. 量能连贯性 (权重 30%) ============
                # 计算 value_per_trade 的变异系数 CV = std / mean
                vpt_values = group['value_per_trade'].values
                vpt_values = vpt_values[~np.isnan(vpt_values)]  # 排除NaN
                if len(vpt_values) > 1 and np.mean(vpt_values) > 0:
                    cv = np.std(vpt_values) / np.mean(vpt_values)
                    # CV 放大100倍后归一化，增强区分度 (原CV=0.001~0.01 → 稳定性=0%~90%)
                    vol_stability = max(0, min(1, 1 - cv * 100))
                else:
                    vol_stability = 0.5
                
                vol_stability_score = vol_stability * 100
                
                # ============ C. 平均广度与质量维持 (权重 20%) ============
                # avg_breadth = mean(rising_ratio + real_body_ratio) / 2
                rising_ratio_mean = group['rising_ratio'].mean()
                real_body_ratio_mean = group['real_body_ratio'].mean() if 'real_body_ratio' in group.columns else 50
                avg_breadth = (rising_ratio_mean + real_body_ratio_mean) / 2
                breadth_score = avg_breadth  # 已经是百分比形式
                
                # ============ D. 相对强度 (权重 10%) ============
                # 板块 avg_pct 斜率 - 基准指数涨幅
                y_pct = group['avg_pct'].values
                pct_slope = np.polyfit(x, y_pct, 1)[0] if n_periods > 1 else 0
                # 基准指数当前只有一个点，用当前涨幅代替斜率
                relative_strength = pct_slope - (benchmark_pct / 10)  # 归一化
                rs_score = np.clip(relative_strength * 20 + 50, 0, 100)
                
                # ============ 综合健康分计算 ============
                health_score = (
                    slope_score_final * 0.40 +
                    vol_stability_score * 0.30 +
                    breadth_score * 0.20 +
                    rs_score * 0.10
                )
                
                # ============ 背离检测与分数修正 ============
                warning_tags = []
                
                current = group.iloc[-1]
                avg_pct_current = current['avg_pct']
                median_return_current = current.get('median_return', avg_pct_current)
                intraday_pos_current = current.get('intraday_position', 0.5)
                vpt_current = current.get('value_per_trade', 0)
                
                # 1. 龙头独舞修正：avg_pct - median_return > 3% → -15分
                if avg_pct_current - median_return_current > 3:
                    health_score -= 15
                    warning_tags.append("龙头独舞")
                
                # 2. 量价背离预警：avg_pct 趋势向上但 value_per_trade 趋势向下 → -20分
                if n_periods >= 3:
                    pct_trend_up = y_pct[-1] > y_pct[-3]  # 涨幅趋势向上
                    vpt_trend_down = vpt_values[-1] < vpt_values[0] if len(vpt_values) >= 2 else False
                    if pct_trend_up and vpt_trend_down:
                        health_score -= 20
                        warning_tags.append("量价背离")
                
                # 3. 位阶坍塌预警：avg_pct 维持但 intraday_pos 连续2期从>0.8跌至<0.5 → -25分
                if n_periods >= 3:
                    intraday_pos_values = group['intraday_position'].values
                    intraday_pos_values = intraday_pos_values[~np.isnan(intraday_pos_values)]
                    if len(intraday_pos_values) >= 3:
                        was_high = intraday_pos_values[-3] > 0.8
                        dropped_mid = intraday_pos_values[-2] < 0.8
                        dropped_low = intraday_pos_values[-1] < 0.5
                        pct_stable = abs(y_pct[-1] - y_pct[-3]) < 1.0  # 涨幅变化不大
                        if was_high and dropped_mid and dropped_low and pct_stable:
                            health_score -= 25
                            warning_tags.append("位阶坍塌")
                
                # 确保分数在合理范围
                health_score = max(0, min(100, health_score))
                
                # ============ 趋势状态判定 ============
                if slope > 0.5:
                    slope_status = "爆发"
                elif slope > -0.2:
                    slope_status = "稳健"
                else:
                    slope_status = "衰减"
                
                # ============ 日内走势形态判定 ============
                # 基于开盘至今的score时间序列识别走势形态
                intraday_pattern = "〰️ 区间震荡"  # 默认
                try:
                    # 获取当天（至当前时刻）全部历史数据
                    query_full = f"""
                    SELECT trade_time, score FROM sector_strength_data 
                    WHERE trade_date = '{trade_date}' AND sector_name = '{name}' AND sector_type = '{sector_type}'
                    {time_filter}
                    ORDER BY trade_time ASC
                    """
                    df_full_day = pd.read_sql_query(query_full, db_conn)
                    
                    if len(df_full_day) >= 4:
                        full_scores = df_full_day['score'].values
                        n_full = len(full_scores)
                        half = n_full // 2
                        first_half = full_scores[:half]
                        second_half = full_scores[half:]
                        
                        # 计算前后半段的趋势（前半段=开盘至中点，后半段=中点至当前）
                        first_trend = first_half[-1] - first_half[0]
                        second_trend = second_half[-1] - second_half[0]
                        overall_trend = full_scores[-1] - full_scores[0]
                        
                        # 计算波动幅度和极值位置
                        score_range = np.max(full_scores) - np.min(full_scores)
                        min_idx = np.argmin(full_scores)
                        max_idx = np.argmax(full_scores)
                        
                        # 动态阈值：根据数据周期数调整（周期越多，阈值越高）
                        # 标准化：假设全天约24个周期(每5分钟一个)，按比例调整
                        period_factor = min(1.0, n_full / 12)  # 12周期(1小时)以上按满值算
                        trend_threshold = 2 + 1 * period_factor  # 2~3
                        range_threshold = 3 + 1 * period_factor  # 3~4
                        monotonic_tolerance = 0.5 + 0.5 * period_factor  # 0.5~1.0
                        
                        # 形态判定逻辑（基于开盘至今数据）
                        if overall_trend > trend_threshold and all(full_scores[i] <= full_scores[i+1] + monotonic_tolerance for i in range(n_full-1)):
                            # 开盘至今持续上涨
                            intraday_pattern = "📈 一路向上"
                        elif overall_trend < -trend_threshold and all(full_scores[i] >= full_scores[i+1] - monotonic_tolerance for i in range(n_full-1)):
                            # 开盘至今持续下跌
                            intraday_pattern = "📉 持续走弱"
                        elif min_idx <= half and second_trend > trend_threshold * 0.7 and full_scores[-1] > full_scores[0]:
                            # 前期探底后回升（V形反弹）
                            intraday_pattern = "🔄 触底反弹"
                        elif max_idx <= half and second_trend < -trend_threshold * 0.7 and full_scores[-1] < full_scores[0]:
                            # 前期冲高后回落（倒V形）
                            intraday_pattern = "⤵️ 冲高回落"
                        elif score_range < range_threshold:
                            # 波动幅度小，横盘震荡
                            intraday_pattern = "➡️ 窄幅震荡"
                        else:
                            # 波动较大但无明显趋势
                            intraday_pattern = "〰️ 区间震荡"
                except:
                    pass  # 出错时使用默认值
                
                health_results.append({
                    'sector_name': name,
                    'health_score': round(health_score, 1),
                    'slope_status': slope_status,
                    'intraday_pattern': intraday_pattern,  # 新增字段
                    'warning_tags': warning_tags,
                    # 额外信息
                    'slope': round(slope, 3),
                    'vol_stability': f"{vol_stability*100:.1f}%",
                    'avg_breadth': f"{avg_breadth:.1f}%",
                    'relative_strength': f"{relative_strength:+.2f}",
                    'order_imb_bonus': order_imb_bonus > 1.0
                })
            
            # 排序并取Top N
            health_results.sort(key=lambda x: x['health_score'], reverse=True)
            top_results = health_results[:top_n]
            
            if not top_results:
                return json.dumps({"error": "暂无数据符合健康度分析标准。"}, ensure_ascii=False)
            
            # 格式化为Markdown表格 + JSON数据
            mode_label = f"📅 模拟回放 {trade_date} {current_time_str}" if is_simulation else "实时"
            output = [f"### {mode_label} 板块走势健康度分析 (周期: 4-6期/约30分钟, 基准: {benchmark_code})"]
            output.append("")
            output.append("| 板块 | 健康分 | 趋势 | 走势 | 量能稳定 | 广度 | RS | 警告 |")
            output.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
            
            for r in top_results:
                warning_str = ", ".join(r['warning_tags']) if r['warning_tags'] else "✅"
                bonus_mark = "📈" if r['order_imb_bonus'] else ""
                pattern = r.get('intraday_pattern', '区间震荡')
                output.append(
                    f"| {r['sector_name']} | **{r['health_score']}** | "
                    f"{r['slope_status']}{bonus_mark} | {pattern} | {r['vol_stability']} | "
                    f"{r['avg_breadth']} | {r['relative_strength']} | {warning_str} |"
                )
            
            output.append("")
            output.append("---")
            output.append("```json")
            output.append(json.dumps(top_results, ensure_ascii=False, indent=2))
            output.append("```")
            
            return "\n".join(output)
            
        except Exception as e:
            import traceback
            return json.dumps({
                "error": f"分析过程出错: {str(e)}",
                "traceback": traceback.format_exc()
            }, ensure_ascii=False)

    @mcp.tool()
    def get_market_rt_snapshot(ts_code: str = "") -> str:
        """
        获取沪深京市场实时行情快照数据（支持股票、ETF、指数）
        
        参数:
            ts_code: 支持通配符方式，例如 6*.SH、3*.SZ、600000.SH
        """
        token = get_tushare_token()
        if not token: return "请先配置Tushare token"
        if not ts_code: return "请提供ts_code"
        try:
            pro = ts.pro_api()
            df = pro.rt_k(ts_code=ts_code)
            if df is None or df.empty: return f"未找到数据: {ts_code}"
            
            # 计算涨跌幅
            if 'pre_close' in df.columns and 'close' in df.columns:
                df['pct_chg'] = (df['close'] - df['pre_close']) / df['pre_close'] * 100
                
            df = df.sort_values('vol', ascending=False, na_position='last')
            
            output = [f"### 实时日线快照 ({ts_code})"]
            cols = ['ts_code', 'name', 'pre_close', 'open', 'close', 'high', 'low', 'pct_chg', 'vol', 'amount']
            col_names = ['代码', '名称', '昨收', '开盘', '现价', '最高', '最低', '涨幅', '成交量', '金额']
            output.append("| " + " | ".join(col_names) + " |")
            output.append("| " + " | ".join(["---"] * len(col_names)) + " |")
            
            for _, r in df.iterrows():
                vals = [
                    str(r.get('ts_code','')), str(r.get('name','')), f"{r.get('pre_close',0):.2f}",
                    f"{r.get('open',0):.2f}", f"{r.get('close',0):.2f}", f"{r.get('high',0):.2f}",
                    f"{r.get('low',0):.2f}", f"{r.get('pct_chg',0):+.2f}%", f"{r.get('vol',0)/10000:.0f}万",
                    f"{r.get('amount',0)/100000000:.2f}亿"
                ]
                output.append("| " + " | ".join(vals) + " |")
            return "\n".join(output)
        except Exception as e:
            return f"查询失败: {str(e)}"
    
    @mcp.tool()
    def search_stocks(keyword: str) -> str:
        """
        搜索股票
        
        参数:
            keyword: 关键词（可以是股票代码的一部分或股票名称的一部分）
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        try:
            # 尝试从缓存获取完整的股票列表（即使过期也返回）
            df = cache_manager.get_dataframe('stock_search', keyword='all')
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True  # 未找到数据，需要从API获取
            elif cache_manager.is_expired('stock_search', keyword='all'):
                need_update = True  # 数据过期，需要更新
            
            if need_update:
                # 过期后立即更新（同步）
                pro = ts.pro_api()
                df = pro.stock_basic()
                # 保存完整列表到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('stock_search', df, keyword='all')
            
            # 在代码和名称中搜索关键词
            mask = (df['ts_code'].str.contains(keyword, case=False)) | \
                   (df['name'].str.contains(keyword, case=False))
            results = df[mask]
            
            if results.empty:
                return "未找到符合条件的股票"
                
            # 格式化输出
            output = []
            for _, row in results.iterrows():
                output.append(f"{row['ts_code']} - {row['name']}")
                
            return "\n".join(output)
            
        except Exception as e:
            return f"搜索失败：{str(e)}"
    
    @mcp.tool()
    def get_stock_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取A股日线行情数据
        
        参数:
            ts_code: 股票代码（如：000001.SZ，支持多个股票同时提取，逗号分隔，如：000001.SZ,600000.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与start_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：交易日每天15点～16点之间入库，本接口是未复权行情，停牌期间不提供数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供股票代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                if ts_code:
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有股票在特定日期的数据
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not stock_daily_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有股票在日期范围内的数据
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = stock_daily_cache_manager.get_stock_daily_data(
                        ts_code=ts_code,
                        limit=20,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供股票代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.daily(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = stock_daily_cache_manager.save_stock_daily_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        if ts_code:
                            df = stock_daily_cache_manager.get_stock_daily_data(
                                ts_code=ts_code,
                                limit=20,
                                order_by='DESC'
                            )
            
            if df is None or df.empty:
                if ts_code:
                    stock_info = f"股票 {ts_code}"
                else:
                    stock_info = "股票"
                
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {stock_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_stock_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_stock_weekly(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取A股周线行情数据
        
        参数:
            ts_code: 股票代码（如：000001.SZ，支持多个股票同时提取，逗号分隔，如：000001.SZ,600000.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定周的数据，trade_date为该周的最后交易日）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与start_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定周的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的周线数据
            - trade_date优先级高于start_date/end_date
            - trade_date为该周的最后交易日（通常是周五）
            - 数据说明：周线数据每周更新一次，本接口是未复权行情
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code:
            return "请提供股票代码(ts_code)"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定周
                df = stock_weekly_cache_manager.get_stock_weekly_data(
                    ts_code=ts_code,
                    trade_date=trade_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                df = stock_weekly_cache_manager.get_stock_weekly_data(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                # 检查缓存数据是否完整覆盖请求的日期范围
                if df is None or df.empty:
                    need_fetch_from_api = True
                elif not stock_weekly_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                    # 缓存数据不完整，需要从API获取完整数据
                    need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                df = stock_weekly_cache_manager.get_stock_weekly_data(
                    ts_code=ts_code,
                    limit=20,
                    order_by='DESC'
                )
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.weekly(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = stock_weekly_cache_manager.save_stock_weekly_data(df)
                    # 如果查询的是特定周或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        df = stock_weekly_cache_manager.get_stock_weekly_data(
                            ts_code=ts_code,
                            trade_date=trade_date
                        )
                    elif start_date or end_date:
                        df = stock_weekly_cache_manager.get_stock_weekly_data(
                            ts_code=ts_code,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:
                        # 查询最近数据
                        df = stock_weekly_cache_manager.get_stock_weekly_data(
                            ts_code=ts_code,
                            limit=20,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                stock_info = f"股票 {ts_code}"
                
                if trade_date:
                    date_info = f"周 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {stock_info} 在 {date_info} 的周线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_stock_weekly_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_etf_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取ETF日线行情数据
        
        参数:
            ts_code: ETF基金代码（如：510330.SH沪深300ETF华夏，支持多个ETF同时提取，逗号分隔，如：510330.SH,510300.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与end_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取ETF行情每日收盘后成交数据，历史超过10年
            - 限量：单次最大2000行记录，可以根据ETF代码和日期循环获取历史
        
        常用ETF代码示例：
            - 510330.SH: 沪深300ETF华夏
            - 510300.SH: 沪深300ETF
            - 159919.SZ: 沪深300ETF
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供ETF代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有ETF在特定日期的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not index_daily_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有ETF在日期范围内的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        limit=20,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供ETF代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                # 使用fund_daily接口获取ETF日线行情数据
                df = pro.fund_daily(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = index_daily_cache_manager.save_index_daily_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                limit=20,
                                order_by='DESC'
                            )
            
            if df is None or df.empty:
                if ts_code:
                    etf_info = f"ETF {ts_code}"
                else:
                    etf_info = "ETF"
                
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {etf_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_etf_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_index_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
        """
        获取A股指数日线行情数据
        
        参数:
            ts_code: 指数代码（如：000300.SH沪深300、000001.SH上证指数、399001.SZ深证成指等，支持多个指数同时提取，逗号分隔，如：000300.SH,000001.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20240101，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20240101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20241231，需与end_date配合使用）
        
        注意：
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：交易日每天15点～16点之间入库，本接口是未复权行情
        
        常用指数代码：
            - 000300.SH: 沪深300指数
            - 000001.SH: 上证指数
            - 399001.SZ: 深证成指
            - 399006.SZ: 创业板指
            - 000016.SH: 上证50指数
            - 399005.SZ: 中小板指
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code and not trade_date:
            return "请至少提供指数代码(ts_code)或交易日期(trade_date)之一"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有指数在特定日期的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not index_daily_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有指数在日期范围内的数据
                    df = index_daily_cache_manager.get_index_daily_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = index_daily_cache_manager.get_index_daily_data(
                        ts_code=ts_code,
                        limit=20,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供指数代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                # 使用index_daily接口获取A股指数日线行情数据
                df = pro.index_daily(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = index_daily_cache_manager.save_index_daily_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = index_daily_cache_manager.get_index_daily_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        if ts_code:
                            df = index_daily_cache_manager.get_index_daily_data(
                                ts_code=ts_code,
                                limit=20,
                                order_by='DESC'
                            )
            
            if df is None or df.empty:
                if ts_code:
                    index_info = f"指数 {ts_code}"
                else:
                    index_info = "指数"
                
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {index_info} 在 {date_info} 的日线行情数据，请检查参数是否正确"
            
            # 格式化输出
            return format_index_daily_data(df, ts_code or "")
            
        except Exception as e:
            return f"查询失败：{str(e)}"
    
    @mcp.tool()
    def get_stock_holder_trade(
        ts_code: str = "",
        ann_date: str = "",
        start_date: str = "",
        end_date: str = "",
        trade_type: str = "",
        holder_type: str = ""
    ) -> str:
        """
        获取上市公司股东增减持数据
        
        参数:
            ts_code: 股票代码（如：300766.SZ，留空则查询所有股票）
            ann_date: 公告日期（YYYYMMDD格式，如：20240426，查询指定日期的增减持数据）
            start_date: 公告开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 公告结束日期（YYYYMMDD格式，需与start_date配合使用）
            trade_type: 交易类型（IN增持，DE减持，留空则查询所有类型）
            holder_type: 股东类型（C公司，P个人，G高管，留空则查询所有类型）
        
        返回:
            包含股东增减持数据的格式化字符串
        
        说明:
            - 数据来源于上市公司公告
            - 支持按股票代码、公告日期、交易类型、股东类型筛选
            - 显示增减持数量、占流通比例、平均价格等信息
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not ann_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、公告日期(ann_date)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if ann_date:
                params['ann_date'] = ann_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            if trade_type:
                params['trade_type'] = trade_type
            if holder_type:
                params['holder_type'] = holder_type
            
            # 获取增减持数据
            df = pro.stk_holdertrade(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if trade_type:
                    param_info.append(f"交易类型: {trade_type}")
                if holder_type:
                    param_info.append(f"股东类型: {holder_type}")
                
                return f"未找到符合条件的增减持数据\n查询条件: {', '.join(param_info)}"
            
            # 按公告日期排序（最新的在前）
            if 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            
            # 格式化输出
            result = []
            result.append("📊 上市公司股东增减持数据")
            result.append("=" * 120)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"股票代码: {ts_code}")
            if ann_date:
                query_info.append(f"公告日期: {ann_date}")
            if start_date or end_date:
                date_range = f"{start_date or '开始'} 至 {end_date or '结束'}"
                query_info.append(f"日期范围: {date_range}")
            if trade_type:
                trade_type_name = "增持" if trade_type == "IN" else "减持" if trade_type == "DE" else trade_type
                query_info.append(f"交易类型: {trade_type_name}")
            if holder_type:
                holder_type_name = {"C": "公司", "P": "个人", "G": "高管"}.get(holder_type, holder_type)
                query_info.append(f"股东类型: {holder_type_name}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 显示数据统计
            result.append(f"📈 共找到 {len(df)} 条增减持记录")
            result.append("")
            
            # 按股票代码分组显示（如果查询了多个股票）
            if not ts_code:
                # 如果未指定股票代码，按股票代码分组
                codes = df['ts_code'].unique()
                for code in codes[:10]:  # 最多显示10个股票
                    code_df = df[df['ts_code'] == code].copy()
                    result.append(format_holder_trade_data(code_df, code))
                    result.append("")
                
                if len(codes) > 10:
                    result.append(f"（共 {len(codes)} 个股票，仅显示前 10 个）")
            else:
                # 单个股票，直接显示
                result.append(format_holder_trade_data(df, ts_code))
            
            result.append("")
            result.append("📝 说明：")
            result.append("  - 数据来源于上市公司公告")
            result.append("  - IN: 增持，DE: 减持")
            result.append("  - 股东类型：C公司，P个人，G高管")
            result.append("  - change_ratio: 占流通比例（%）")
            result.append("  - avg_price: 平均交易价格")
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_stock_holder_number(
        ts_code: str = "",
        ann_date: str = "",
        enddate: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取上市公司股东户数数据
        
        参数:
            ts_code: 股票代码（如：300766.SZ，留空则查询所有股票）
            ann_date: 公告日期（YYYYMMDD格式，如：20240426，查询指定公告日期的数据）
            enddate: 截止日期（YYYYMMDD格式，如：20240930，查询指定截止日期的数据）
            start_date: 公告开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 公告结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含股东户数数据的格式化字符串
        
        说明:
            - 数据来源于上市公司定期报告，不定期公布
            - 支持按股票代码、公告日期、截止日期、日期范围筛选
            - 股东户数变化可以反映股票的集中度变化趋势
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not ann_date and not enddate and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、公告日期(ann_date)、截止日期(enddate)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if ann_date:
                params['ann_date'] = ann_date
            if enddate:
                params['enddate'] = enddate
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 获取股东户数数据
            df = pro.stk_holdernumber(**params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if enddate:
                    param_info.append(f"截止日期: {enddate}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的股东户数数据\n查询条件: {', '.join(param_info)}"
            
            # 按公告日期排序（最新的在前）
            if 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            elif 'end_date' in df.columns:
                df = df.sort_values('end_date', ascending=False)
            
            # 格式化输出
            result = []
            result.append("📊 上市公司股东户数数据")
            result.append("=" * 100)
            result.append("")
            
            # 显示查询条件
            query_info = []
            if ts_code:
                query_info.append(f"股票代码: {ts_code}")
            if ann_date:
                query_info.append(f"公告日期: {ann_date}")
            if enddate:
                query_info.append(f"截止日期: {enddate}")
            if start_date or end_date:
                date_range = f"{start_date or '开始'} 至 {end_date or '结束'}"
                query_info.append(f"日期范围: {date_range}")
            
            if query_info:
                result.append("查询条件:")
                for info in query_info:
                    result.append(f"  - {info}")
                result.append("")
            
            # 显示数据统计
            result.append(f"📈 共找到 {len(df)} 条股东户数记录")
            result.append("")
            
            # 按股票代码分组显示（如果查询了多个股票）
            if not ts_code:
                # 如果未指定股票代码，按股票代码分组
                codes = df['ts_code'].unique()
                for code in codes[:10]:  # 最多显示10个股票
                    code_df = df[df['ts_code'] == code].copy()
                    result.append(format_holder_number_data(code_df, code))
                    result.append("")
                
                if len(codes) > 10:
                    result.append(f"（共 {len(codes)} 个股票，仅显示前 10 个）")
            else:
                # 单个股票，直接显示
                result.append(format_holder_number_data(df, ts_code))
            
            result.append("")
            result.append("📝 说明：")
            result.append("  - 数据来源于上市公司定期报告，不定期公布")
            result.append("  - 股东户数增加通常表示持股分散，减少表示持股集中")
            result.append("  - 建议结合股价走势分析股东户数变化趋势")
            
            return "\n".join(result)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_stock_moneyflow_dc(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取东方财富个股资金流向数据
        
        参数:
            ts_code: 股票代码（如：600111.SH，留空则查询所有股票）
            trade_date: 交易日期（YYYYMMDD格式，如：20241011，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含资金流向数据的格式化字符串
        
        说明:
            - 数据来源：东方财富，每日盘后更新，数据开始于20230911
            - 支持按股票代码、交易日期、日期范围筛选
            - 显示主力净流入额、超大单/大单/中单/小单的净流入额和占比
            - 权限要求：5000积分
            - 限量：单次最大获取6000条数据，可根据日期或股票代码循环提取
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if trade_date:
                params['trade_date'] = trade_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('moneyflow_dc', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('moneyflow_dc', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                df = pro.moneyflow_dc(**params)
                
                # 保存到缓存（创建新版本）
                if not df.empty:
                    cache_manager.set('moneyflow_dc', df, **cache_params)
            
            if df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的资金流向数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_moneyflow_dc_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_stock_survey(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取上市公司机构调研记录数据
        
        参数:
            ts_code: 股票代码（如：002223.SZ，留空则查询所有股票）
            trade_date: 调研日期（YYYYMMDD格式，如：20211024，查询指定日期的调研数据）
            start_date: 调研开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 调研结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含机构调研数据的格式化字符串
        
        说明:
            - 数据来源于上市公司披露的机构调研记录
            - 支持按股票代码、调研日期、日期范围筛选
            - 显示机构参与人员、接待地点、接待方式、接待公司等信息
            - 权限要求：5000积分
            - 限量：单次最大获取100条数据，可循环或分页提取
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、调研日期(trade_date)或日期范围(start_date/end_date)"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                if ts_code:
                    df = stk_surv_cache_manager.get_stk_surv_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有股票在特定日期的数据
                    df = stk_surv_cache_manager.get_stk_surv_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = stk_surv_cache_manager.get_stk_surv_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not stk_surv_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有股票在日期范围内的数据
                    df = stk_surv_cache_manager.get_stk_surv_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = stk_surv_cache_manager.get_stk_surv_data(
                        ts_code=ts_code,
                        limit=100,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供股票代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.stk_surv(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = stk_surv_cache_manager.save_stk_surv_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = stk_surv_cache_manager.get_stk_surv_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = stk_surv_cache_manager.get_stk_surv_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = stk_surv_cache_manager.get_stk_surv_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = stk_surv_cache_manager.get_stk_surv_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        df = stk_surv_cache_manager.get_stk_surv_data(
                            ts_code=ts_code,
                            limit=100,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                stock_info = f"股票 {ts_code}" if ts_code else "股票"
                
                if trade_date:
                    date_info = f"调研日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {stock_info} 在 {date_info} 的机构调研数据，请检查参数是否正确"
            
            # 格式化输出
            return format_stock_survey_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_cyq_perf(
        ts_code: str,
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取A股每日筹码平均成本和胜率情况
        
        参数:
            ts_code: 股票代码（必填，如：600000.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20220429，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20220101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20220429，需与start_date配合使用）
        
        返回:
            包含每日筹码及胜率数据的格式化字符串，包括计算的筹码集中度
        
        说明:
            - 数据每天17~18点左右更新，数据从2018年开始
            - 权限要求：5000积分每天20000次，10000积分每天200000次，15000积分每天不限总量
            - 限量：单次最大5000条，可以分页或者循环提取
            - 筹码集中度计算公式：集中度 = (cost_95pct - cost_5pct) / (cost_95pct + cost_5pct)
            - 集中度越小，说明筹码越集中；集中度越大，说明筹码越分散
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code:
            return "请提供股票代码(ts_code)"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip()
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                df = cyq_perf_cache_manager.get_cyq_perf_data(
                    ts_code=ts_code,
                    trade_date=trade_date
                )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                df = cyq_perf_cache_manager.get_cyq_perf_data(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                # 检查缓存数据是否完整覆盖请求的日期范围
                if df is None or df.empty:
                    need_fetch_from_api = True
                elif not cyq_perf_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                    # 缓存数据不完整，需要从API获取完整数据
                    need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                df = cyq_perf_cache_manager.get_cyq_perf_data(
                    ts_code=ts_code,
                    limit=30,
                    order_by='DESC'
                )
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {'ts_code': ts_code}
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.cyq_perf(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = cyq_perf_cache_manager.save_cyq_perf_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        df = cyq_perf_cache_manager.get_cyq_perf_data(
                            ts_code=ts_code,
                            trade_date=trade_date
                        )
                    elif start_date or end_date:
                        df = cyq_perf_cache_manager.get_cyq_perf_data(
                            ts_code=ts_code,
                            start_date=start_date,
                            end_date=end_date
                        )
                    else:
                        # 查询最近数据
                        df = cyq_perf_cache_manager.get_cyq_perf_data(
                            ts_code=ts_code,
                            limit=30,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到股票 {ts_code} 在 {date_info} 的每日筹码及胜率数据，请检查参数是否正确"
            
            # 计算筹码集中度
            # 集中度 = (cost_95pct - cost_5pct) / (cost_95pct + cost_5pct)
            if 'cost_95pct' in df.columns and 'cost_5pct' in df.columns:
                df['concentration'] = (df['cost_95pct'] - df['cost_5pct']) / (df['cost_95pct'] + df['cost_5pct'])
                # 处理除零情况
                df['concentration'] = df['concentration'].replace([float('inf'), float('-inf')], None)
            
            # 格式化输出
            return format_cyq_perf_data(df, ts_code)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_daily_basic(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取每日指标数据
        
        参数:
            ts_code: 股票代码（如：600230.SH，支持多个股票同时提取，逗号分隔，如：600230.SH,600237.SH）
            trade_date: 交易日期（YYYYMMDD格式，如：20180726，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，如：20180101，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，如：20181231，需与start_date配合使用）
        
        注意:
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取每日指标数据，包括估值指标（PE、PB、PS）、换手率、量比、市值等
        
        返回:
            包含每日指标数据的格式化字符串
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
        try:
            # 参数处理：将空字符串转换为 None，便于后续处理
            ts_code = ts_code.strip() if ts_code else None
            trade_date = trade_date.strip() if trade_date else None
            start_date = start_date.strip() if start_date else None
            end_date = end_date.strip() if end_date else None
            
            if trade_date and (start_date or end_date):
                # 如果同时提供了trade_date和日期范围，优先使用trade_date
                start_date = None
                end_date = None
            
            # 从专用缓存表查询数据（永不失效）
            df = None
            need_fetch_from_api = False
            
            if trade_date:
                # 查询特定日期
                if ts_code:
                    df = daily_basic_cache_manager.get_daily_basic_data(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                else:
                    # 查询所有股票在特定日期的数据
                    df = daily_basic_cache_manager.get_daily_basic_data(
                        trade_date=trade_date
                    )
                if df is None or df.empty:
                    need_fetch_from_api = True
            elif start_date or end_date:
                # 查询日期范围（至少需要提供一个日期参数）
                if ts_code:
                    df = daily_basic_cache_manager.get_daily_basic_data(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    # 检查缓存数据是否完整覆盖请求的日期范围
                    if df is None or df.empty:
                        need_fetch_from_api = True
                    elif not daily_basic_cache_manager.is_cache_data_complete(ts_code, start_date, end_date):
                        # 缓存数据不完整，需要从API获取完整数据
                        need_fetch_from_api = True
                else:
                    # 查询所有股票在日期范围内的数据
                    df = daily_basic_cache_manager.get_daily_basic_data(
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is None or df.empty:
                        need_fetch_from_api = True
            else:
                # 查询最近数据（从缓存获取最新数据）
                if ts_code:
                    df = daily_basic_cache_manager.get_daily_basic_data(
                        ts_code=ts_code,
                        limit=30,
                        order_by='DESC'
                    )
                else:
                    return "查询最近数据时，请提供股票代码(ts_code)"
                # 如果缓存中没有数据，需要从API获取
                if df is None or df.empty:
                    need_fetch_from_api = True
            
            # 如果需要从API获取数据
            if need_fetch_from_api:
                pro = ts.pro_api()
                params = {}
                
                if ts_code:
                    params['ts_code'] = ts_code
                
                # 优先使用trade_date，否则使用日期范围
                if trade_date:
                    params['trade_date'] = trade_date
                else:
                    if start_date:
                        params['start_date'] = start_date
                    if end_date:
                        params['end_date'] = end_date
                
                df = pro.daily_basic(**params)
                
                # 保存到专用缓存表（永不失效）
                if not df.empty:
                    saved_count = daily_basic_cache_manager.save_daily_basic_data(df)
                    # 如果查询的是特定日期或范围，重新从缓存读取以确保数据一致性
                    if trade_date:
                        if ts_code:
                            df = daily_basic_cache_manager.get_daily_basic_data(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                        else:
                            df = daily_basic_cache_manager.get_daily_basic_data(
                                trade_date=trade_date
                            )
                    elif start_date or end_date:
                        if ts_code:
                            df = daily_basic_cache_manager.get_daily_basic_data(
                                ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date
                            )
                        else:
                            df = daily_basic_cache_manager.get_daily_basic_data(
                                start_date=start_date,
                                end_date=end_date
                            )
                    else:
                        # 查询最近数据
                        df = daily_basic_cache_manager.get_daily_basic_data(
                            ts_code=ts_code,
                            limit=30,
                            order_by='DESC'
                        )
            
            if df is None or df.empty:
                stock_info = f"股票 {ts_code}" if ts_code else "股票"
                
                if trade_date:
                    date_info = f"日期 {trade_date}"
                elif start_date or end_date:
                    if start_date and end_date:
                        date_info = f"日期范围 {start_date} 至 {end_date}"
                    elif start_date:
                        date_info = f"日期范围从 {start_date} 开始"
                    else:
                        date_info = f"日期范围到 {end_date} 结束"
                else:
                    date_info = "最近数据"
                return f"未找到 {stock_info} 在 {date_info} 的每日指标数据，请检查参数是否正确"
            
            # 格式化输出
            return format_daily_basic_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_top_list(
        trade_date: str = "",
        ts_code: str = ""
    ) -> str:
        """
        获取龙虎榜每日交易明细数据
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20180928，必填）
            ts_code: 股票代码（如：002219.SZ，可选）
        
        返回:
            包含龙虎榜交易明细数据的格式化字符串
        
        说明:
            - 数据来源：Tushare top_list接口
            - 数据历史：2005年至今
            - 显示收盘价、涨跌幅、换手率、总成交额、龙虎榜买入/卖出额、净买入额等数据
            - 显示上榜理由（如：日涨幅偏离值达到7%、日换手率达到20%等）
            - 权限要求：2000积分
            - 限量：单次请求返回最大10000行数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：trade_date是必填参数
        if not trade_date:
            return "请提供交易日期(trade_date)，格式：YYYYMMDD（如：20180928）"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {
                'trade_date': trade_date
            }
            if ts_code:
                params['ts_code'] = ts_code
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'trade_date': trade_date,
                'ts_code': ts_code or ''
            }
            df = cache_manager.get_dataframe('top_list', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('top_list', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.top_list(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('top_list', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：top_list\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认top_list接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = f"交易日期: {trade_date}"
                if ts_code:
                    param_info += f", 股票代码: {ts_code}"
                
                return f"未找到符合条件的龙虎榜数据\n查询条件: {param_info}\n\n提示：\n- 请确认该日期是否为交易日\n- 该日期是否有股票上榜龙虎榜"
            
            # 按股票代码和交易日期排序
            if 'ts_code' in df.columns:
                df = df.sort_values(['ts_code', 'trade_date'], ascending=[True, False])
            elif 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_top_list_data(df, trade_date, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_top_inst(
        trade_date: str = "",
        ts_code: str = ""
    ) -> str:
        """
        获取龙虎榜机构成交明细数据
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20210525，必填）
            ts_code: 股票代码（如：000592.SZ，可选）
        
        返回:
            包含龙虎榜机构成交明细数据的格式化字符串
        
        说明:
            - 数据来源：Tushare top_inst接口
            - 显示营业部名称、买卖类型、买入额、卖出额、净成交额等数据
            - 买卖类型：0=买入金额最大的前5名，1=卖出金额最大的前5名
            - 显示上榜理由（如：涨幅偏离值达7%、连续三个交易日内涨幅偏离值累计达20%等）
            - 权限要求：5000积分
            - 限量：单次请求最大返回10000行数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：trade_date是必填参数
        if not trade_date:
            return "请提供交易日期(trade_date)，格式：YYYYMMDD（如：20210525）"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {
                'trade_date': trade_date
            }
            if ts_code:
                params['ts_code'] = ts_code
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'trade_date': trade_date,
                'ts_code': ts_code or ''
            }
            df = cache_manager.get_dataframe('top_inst', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('top_inst', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.top_inst(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('top_inst', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：top_inst\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到5000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认top_inst接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到5000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = f"交易日期: {trade_date}"
                if ts_code:
                    param_info += f", 股票代码: {ts_code}"
                
                return f"未找到符合条件的龙虎榜机构明细数据\n查询条件: {param_info}\n\n提示：\n- 请确认该日期是否为交易日\n- 该日期是否有机构上榜龙虎榜"
            
            # 按股票代码、买卖类型和交易日期排序
            if 'ts_code' in df.columns:
                df = df.sort_values(['ts_code', 'side', 'trade_date'], ascending=[True, True, False])
            elif 'side' in df.columns:
                df = df.sort_values(['side', 'trade_date'], ascending=[True, False])
            elif 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_top_inst_data(df, trade_date, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    # ==================== A股实时分钟行情 基于本地缓存 ====================
    def _fetch_stock_min_data(
        ts_code: str = "",
        freq: str = "1MIN",
        date_str: str = ""
    ) -> str:
        """
        从本地分时快照缓存获取A股分钟行情数据（基于5分钟快照）。
        参数:
            ts_code: 股票代码，支持逗号分隔的多只股票
            freq: 目标频率，支持 1MIN、5MIN、15MIN、30MIN、60MIN
                  仅在5MIN基础上做重采样
            date_str: 指定日期 (YYYY-MM-DD)，默认当日
        返回:
            格式化的分钟行情字符串
        """
        if not ts_code:
            return "请提供股票代码(ts_code)，如：600000.SH，支持多个股票（逗号分隔）"

        # 解析日期
        if date_str:
            try:
                trade_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
            except Exception:
                return f"日期格式错误: {date_str}，请使用 YYYY-MM-DD"
        else:
            trade_date = datetime.now().strftime("%Y%m%d")

        # 读取本地缓存
        codes = [c.strip() for c in ts_code.split(",")]
        dfs = []
        for code in codes:
            df = pd.read_sql_query(
                "SELECT * FROM stock_intraday_data WHERE ts_code = ? AND trade_date = ?",
                stock_intraday_cache_manager.conn,
                params=(code, trade_date)
            )
            if df.empty:
                continue
            # 合并时间列为 datetime，便于重采样
            df["datetime"] = pd.to_datetime(trade_date + " " + df["trade_time"])
            df.set_index("datetime", inplace=True)
            dfs.append(df)

        if not dfs:
            return f"未找到符合条件的A股分钟行情数据\n查询条件: 股票代码: {ts_code}, 日期: {trade_date}"

        # 合并所有股票数据
        result_df = pd.concat(dfs)

        # 频率重采样（基于5MIN快照）
        freq = freq.upper()
        if freq != "5MIN":
            # 使用前向填充进行重采样
            result_df = result_df.resample(freq).ffill()

        # 按时间倒序
        result_df = result_df.sort_index(ascending=False)

        return format_stock_min_data(result_df, ts_code, freq, date_str)
    
    # ==================== A股实时分钟行情 MCP Tool ====================
    @mcp.tool()
    def get_stock_min(
        ts_code: str = "",
        freq: str = "1MIN",
        date_str: str = ""
    ) -> str:
        """
        获取A股实时分钟行情数据
        
        参数:
            ts_code: 股票代码（必填，如：600000.SH，支持多个股票，逗号分隔，如：600000.SH,000001.SZ）
            freq: 分钟频度（必填，默认1MIN）
                - 1MIN: 1分钟
                - 5MIN: 5分钟
                - 15MIN: 15分钟
                - 30MIN: 30分钟
                - 60MIN: 60分钟
            date_str: 回放日期（可选，格式：YYYY-MM-DD，默认为交易当日，支持回溯一天）
                如果提供此参数，将使用rt_min_daily接口获取当日开盘以来的所有历史分钟数据
        
        返回:
            包含A股实时分钟行情数据的格式化字符串
        
        说明:
            - 数据来源：Tushare rt_min接口（实时）或rt_min_daily接口（历史回放）
            - 支持1min/5min/15min/30min/60min行情
            - 显示开盘、最高、最低、收盘、成交量、成交额等数据
            - 权限要求：正式权限请参阅权限说明
            - 限量：单次最大1000行数据，支持多个股票同时提取
            - 注意：rt_min_daily接口仅支持单个股票提取，不能同时提取多个
        """
        return _fetch_stock_min_data(ts_code=ts_code, freq=freq, date_str=date_str)
    
    @mcp.tool()
    def get_stock_rt_k(
        ts_code: str = ""
    ) -> str:
        """
        获取沪深京实时日线行情数据
        
        参数:
            ts_code: 股票代码（必填，支持通配符方式）
                - 单个股票：600000.SH、000001.SZ、430047.BJ
                - 通配符方式：6*.SH（所有6开头的沪市股票）、301*.SZ（所有301开头的深市股票）、0*.SZ（所有0开头的深市股票）、9*.BJ（所有9开头的北交所股票）
                - 多个股票或通配符：600000.SH,000001.SZ 或 6*.SH,0*.SZ
                - 注意：代码必须带.SH/.SZ/.BJ后缀
        
        返回:
            包含沪深京实时日线行情数据的格式化字符串
        
        说明:
            - 数据来源：Tushare rt_k接口
            - 获取实时日k线行情，支持按股票代码及股票代码通配符一次性提取全部股票实时日k线行情
            - 显示开盘、最高、最低、收盘（最新价）、成交量、成交金额、成交笔数、委托买卖盘等数据
            - 权限要求：本接口是单独开权限的数据，单独申请权限请参考权限列表
            - 限量：单次最大可提取6000条数据，等同于一次提取全市场
            - 注意：不建议一次提取全市场，可分批提取性能更好
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code:
            return "请提供股票代码(ts_code)，支持通配符方式，如：600000.SH、6*.SH、301*.SZ等，代码必须带.SH/.SZ/.BJ后缀"
        
        # 验证代码格式（必须包含.SH/.SZ/.BJ后缀）
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            if not (code.endswith('.SH') or code.endswith('.SZ') or code.endswith('.BJ')):
                return f"股票代码格式错误：{code}\n代码必须带.SH/.SZ/.BJ后缀，如：600000.SH、000001.SZ、430047.BJ"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {
                'ts_code': ts_code
            }
            
            # 实时数据不缓存
            try:
                df = pro.rt_k(**params)
            except Exception as api_error:
                error_msg = str(api_error)
                # 检查是否是接口名错误
                if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                    return f"API接口调用失败：{error_msg}\n\n已使用接口：rt_k\n\n可能的原因：\n1. Tushare token是否有效\n2. 是否已开通沪深京实时日线行情权限\n3. 网络连接是否正常\n4. 股票代码格式是否正确（必须带.SH/.SZ/.BJ后缀）\n\n建议：\n- 请查看Tushare文档确认rt_k接口是否可用\n- 检查是否已开通相应权限"
                else:
                    return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 是否已开通沪深京实时日线行情权限\n3. 网络连接是否正常\n4. 股票代码格式是否正确"
            
            if df is None or df.empty:
                return f"未找到符合条件的沪深京实时日线行情数据\n查询条件: 股票代码: {ts_code}"
            
            # 按成交量排序（降序），显示最活跃的股票
            if 'vol' in df.columns:
                df = df.sort_values('vol', ascending=False, na_position='last')
            
            # 格式化输出
            return format_stock_rt_k_data(df, ts_code)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_share_float(
        ts_code: str = "",
        ann_date: str = "",
        float_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取限售股解禁数据
        
        参数:
            ts_code: 股票代码（如：000998.SZ，可选）
            ann_date: 公告日期（YYYYMMDD格式，如：20181220，可选）
            float_date: 解禁日期（YYYYMMDD格式，可选）
            start_date: 解禁开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 解禁结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含限售股解禁数据的格式化字符串
        
        说明:
            - 数据来源：Tushare share_float接口
            - 支持按股票代码、公告日期、解禁日期、日期范围筛选
            - 显示解禁日期、流通股份、流通股份占总股本比率、股东名称、股份类型等信息
            - 权限要求：2000积分
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not ann_date and not float_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、公告日期(ann_date)、解禁日期(float_date)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if ann_date:
                params['ann_date'] = ann_date
            if float_date:
                params['float_date'] = float_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了ann_date和日期范围，优先使用ann_date
            if ann_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 如果同时提供了float_date和日期范围，优先使用float_date
            if float_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'ann_date': ann_date or '',
                'float_date': float_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('share_float', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('share_float', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.share_float(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('share_float', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：share_float\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常\n\n建议：\n- 请查看Tushare文档确认share_float接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到2000分以上\n3. 网络连接是否正常"
            
            if df is None or df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if float_date:
                    param_info.append(f"解禁日期: {float_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的限售股解禁数据\n查询条件: {', '.join(param_info)}"
            
            # 按解禁日期排序（最新的在前）
            if 'float_date' in df.columns:
                df = df.sort_values('float_date', ascending=False)
            elif 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            
            # 格式化输出
            return format_share_float_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_stock_repurchase(
        ann_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取上市公司股票回购数据
        
        参数:
            ann_date: 公告日期（YYYYMMDD格式，如：20181010，可选）
            start_date: 公告开始日期（YYYYMMDD格式，如：20180101，需与end_date配合使用）
            end_date: 公告结束日期（YYYYMMDD格式，如：20180510，需与start_date配合使用）
        
        返回:
            包含股票回购数据的格式化字符串
        
        说明:
            - 数据来源：Tushare repurchase接口
            - 支持按公告日期、日期范围筛选
            - 显示公告日期、截止日期、进度、过期日期、回购数量、回购金额、回购最高价、回购最低价等信息
            - 权限要求：600积分
            - 注意：如果都不填参数，单次默认返回2000条数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ann_date:
                params['ann_date'] = ann_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了ann_date和日期范围，优先使用ann_date
            if ann_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ann_date': ann_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('repurchase', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('repurchase', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.repurchase(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('repurchase', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：repurchase\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到600分以上\n3. 网络连接是否正常\n\n建议：\n- 请查看Tushare文档确认repurchase接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到600分以上\n3. 网络连接是否正常"
            
            if df is None or df.empty:
                param_info = []
                if ann_date:
                    param_info.append(f"公告日期: {ann_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                if not param_info:
                    param_info.append("无筛选条件（默认返回2000条）")
                
                return f"未找到符合条件的股票回购数据\n查询条件: {', '.join(param_info)}"
            
            # 按公告日期排序（最新的在前）
            if 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            
            # 格式化输出
            return format_repurchase_data(df, ann_date or start_date or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_pledge_detail(
        ts_code: str = ""
    ) -> str:
        """
        获取股票股权质押明细数据
        
        参数:
            ts_code: 股票代码（必填，如：000014.SZ）
        
        返回:
            包含股权质押明细数据的格式化字符串
        
        说明:
            - 数据来源：Tushare pledge_detail接口
            - 显示股票质押明细数据，包括公告日期、股东名称、质押数量、质押开始/结束日期、是否已解押、解押日期、质押方、持股总数、质押总数、质押比例等信息
            - 权限要求：500积分
            - 限量：单次最大可调取1000条数据
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证
        if not ts_code:
            return "请提供股票代码(ts_code)，如：000014.SZ"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {
                'ts_code': ts_code
            }
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code
            }
            df = cache_manager.get_dataframe('pledge_detail', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('pledge_detail', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.pledge_detail(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('pledge_detail', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：pledge_detail\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到500分以上\n3. 网络连接是否正常\n4. 股票代码格式是否正确（如：000014.SZ）\n\n建议：\n- 请查看Tushare文档确认pledge_detail接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到500分以上\n3. 网络连接是否正常\n4. 股票代码格式是否正确"
            
            if df is None or df.empty:
                return f"未找到 {ts_code} 的股权质押明细数据，请检查股票代码是否正确"
            
            # 按公告日期排序（最新的在前）
            if 'ann_date' in df.columns:
                df = df.sort_values('ann_date', ascending=False)
            
            # 格式化输出
            return format_pledge_detail_data(df, ts_code)
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_block_trade(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取大宗交易数据
        
        参数:
            ts_code: 股票代码（如：600436.SH，可选）
            trade_date: 交易日期（YYYYMMDD格式，如：20181227，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        返回:
            包含大宗交易数据的格式化字符串
        
        说明:
            - 数据来源：Tushare block_trade接口
            - 支持按股票代码、交易日期、日期范围筛选
            - 显示交易日期、成交价、成交量、成交金额、买方营业部、卖方营业部等信息
            - 权限要求：请查看Tushare文档确认具体权限要求
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not ts_code and not trade_date and not start_date and not end_date:
            return "请至少提供以下参数之一：股票代码(ts_code)、交易日期(trade_date)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if trade_date:
                params['trade_date'] = trade_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('block_trade', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('block_trade', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.block_trade(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('block_trade', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：block_trade\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到要求\n3. 网络连接是否正常\n4. 股票代码格式是否正确（如：600436.SH）\n\n建议：\n- 请查看Tushare文档确认block_trade接口是否可用\n- 检查Tushare账户积分是否足够"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到要求\n3. 网络连接是否正常\n4. 股票代码格式是否正确"
            
            if df is None or df.empty:
                param_info = []
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的大宗交易数据\n查询条件: {', '.join(param_info)}"
            
            # 按交易日期排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_block_trade_data(df, ts_code or "", trade_date or start_date or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_limit_list(
        trade_date: str = "",
        ts_code: str = "",
        limit_type: str = "",
        exchange: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取A股每日涨跌停、炸板数据情况
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20220615，可选）
            ts_code: 股票代码（如：000001.SZ，可选）
            limit_type: 涨跌停类型（U涨停、D跌停、Z炸板，可选）
            exchange: 交易所（SH上交所、SZ深交所、BJ北交所，可选）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用，可选）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用，可选）
        
        返回:
            包含涨跌停数据的格式化字符串
        
        说明:
            - 数据来源：Tushare limit_list_d接口
            - 数据历史：2020年至今（不提供ST股票的统计）
            - 显示收盘价、涨跌幅、成交额、封单金额、首次/最后封板时间、炸板次数、连板数等信息
            - 权限要求：5000积分（每分钟200次，每天总量1万次），8000积分以上（每分钟500次，每天总量不限制）
            - 限量：单次最大可获取2500条数据，可通过日期或股票循环提取
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not trade_date and not ts_code and not start_date and not end_date:
            return "请至少提供以下参数之一：交易日期(trade_date)、股票代码(ts_code)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        # 验证limit_type参数
        if limit_type and limit_type.upper() not in ['U', 'D', 'Z']:
            return "limit_type参数值错误，可选值：U（涨停）、D（跌停）、Z（炸板）"
        
        # 验证exchange参数
        if exchange and exchange.upper() not in ['SH', 'SZ', 'BJ']:
            return "exchange参数值错误，可选值：SH（上交所）、SZ（深交所）、BJ（北交所）"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if trade_date:
                params['trade_date'] = trade_date
            if ts_code:
                params['ts_code'] = ts_code
            if limit_type:
                params['limit_type'] = limit_type.upper()
            if exchange:
                params['exchange'] = exchange.upper()
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'trade_date': trade_date or '',
                'ts_code': ts_code or '',
                'limit_type': limit_type.upper() if limit_type else '',
                'exchange': exchange.upper() if exchange else '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('limit_list_d', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('limit_list_d', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.limit_list_d(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('limit_list_d', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：limit_list_d\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到5000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认limit_list_d接口是否可用\n- 检查Tushare账户积分是否足够（需要5000积分）"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到5000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = []
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if limit_type:
                    limit_type_map = {'U': '涨停', 'D': '跌停', 'Z': '炸板'}
                    param_info.append(f"类型: {limit_type_map.get(limit_type.upper(), limit_type)}")
                if exchange:
                    exchange_map = {'SH': '上交所', 'SZ': '深交所', 'BJ': '北交所'}
                    param_info.append(f"交易所: {exchange_map.get(exchange.upper(), exchange)}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的涨跌停数据\n查询条件: {', '.join(param_info)}\n\n提示：\n- 请确认该日期是否为交易日\n- 该日期是否有股票涨跌停或炸板\n- 注意：本接口不提供ST股票的统计"
            
            # 按交易日期和股票代码排序（最新的在前）
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            if 'ts_code' in df.columns:
                df = df.sort_values(['trade_date', 'ts_code'], ascending=[False, True])
            
            # 格式化输出
            return format_limit_list_data(df, trade_date or start_date or "", ts_code or "", limit_type or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_limit_cpt_list(
        trade_date: str = "",
        ts_code: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取每天涨停股票最多最强的概念板块
        
        参数:
            trade_date: 交易日期（YYYYMMDD格式，如：20241127，可选）
            ts_code: 板块代码（可选）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用，可选）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用，可选）
        
        返回:
            包含最强板块统计数据的格式化字符串
        
        说明:
            - 数据来源：Tushare limit_cpt_list接口
            - 功能：获取每天涨停股票最多最强的概念板块，可以分析强势板块的轮动，判断资金动向
            - 显示板块代码、板块名称、交易日期、上榜天数、连板高度、连板家数、涨停家数、涨跌幅、板块热点排名等信息
            - 权限要求：8000积分以上每分钟500次，每天总量不限制
            - 限量：单次最大2000行数据，可根据股票代码或日期循环提取全部
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not trade_date and not ts_code and not start_date and not end_date:
            return "请至少提供以下参数之一：交易日期(trade_date)、板块代码(ts_code)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if trade_date:
                params['trade_date'] = trade_date
            if ts_code:
                params['ts_code'] = ts_code
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'trade_date': trade_date or '',
                'ts_code': ts_code or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('limit_cpt_list', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('limit_cpt_list', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.limit_cpt_list(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('limit_cpt_list', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：limit_cpt_list\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户积分是否达到8000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n\n建议：\n- 请查看Tushare文档确认limit_cpt_list接口是否可用\n- 检查Tushare账户积分是否足够（需要8000积分以上）"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户积分是否达到8000分以上\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = []
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if ts_code:
                    param_info.append(f"板块代码: {ts_code}")
                if start_date or end_date:
                    param_info.append(f"日期范围: {start_date or '开始'} 至 {end_date or '结束'}")
                
                return f"未找到符合条件的最强板块统计数据\n查询条件: {', '.join(param_info)}\n\n提示：\n- 请确认该日期是否为交易日\n- 该日期是否有涨停股票和概念板块数据"
            
            # 按板块热点排名排序（升序，排名越小越靠前）
            if 'rank' in df.columns:
                # 将rank转换为数字进行排序
                df['rank_num'] = df['rank'].astype(str).str.extract(r'(\d+)').astype(float)
                df = df.sort_values('rank_num', ascending=True, na_position='last')
                df = df.drop('rank_num', axis=1)
            elif 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=False)
            
            # 格式化输出
            return format_limit_cpt_list_data(df, trade_date or start_date or "", ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"
    
    @mcp.tool()
    def get_stock_auction(
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = ""
    ) -> str:
        """
        获取当日个股和ETF的集合竞价成交情况
        
        参数:
            ts_code: 股票代码（如：000001.SZ，可选）
            trade_date: 交易日期（YYYYMMDD格式，如：20250218，查询指定日期的数据）
            start_date: 开始日期（YYYYMMDD格式，需与end_date配合使用）
            end_date: 结束日期（YYYYMMDD格式，需与start_date配合使用）
        
        注意:
            - 如果提供了trade_date，将查询该特定日期的数据
            - 如果提供了start_date和end_date，将查询该日期范围内的数据
            - trade_date优先级高于start_date/end_date
            - 数据说明：获取当日个股和ETF的集合竞价成交情况，每天9点25~29分之间可以获取当日的集合竞价成交数据
            - 权限要求：本接口是单独开权限的数据，已经开通了股票分钟权限的用户可自动获得本接口权限
            - 限量：单次最大返回8000行数据，可根据日期或代码循环获取历史
        
        返回:
            包含集合竞价成交数据的格式化字符串
        """
        token = get_tushare_token()
        if not token:
            return "请先配置Tushare token"
        
        # 参数验证：至少需要提供一个查询条件
        if not trade_date and not ts_code and not start_date and not end_date:
            return "请至少提供以下参数之一：交易日期(trade_date)、股票代码(ts_code)或日期范围(start_date/end_date)"
        
        # 如果提供了日期范围，必须同时提供start_date和end_date
        if (start_date and not end_date) or (end_date and not start_date):
            return "如果使用日期范围查询，请同时提供start_date和end_date"
        
        try:
            pro = ts.pro_api()
            
            # 构建查询参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if trade_date:
                params['trade_date'] = trade_date
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            
            # 如果同时提供了trade_date和日期范围，优先使用trade_date
            if trade_date and (start_date or end_date):
                params.pop('start_date', None)
                params.pop('end_date', None)
            
            # 尝试从缓存获取（即使过期也返回）
            cache_params = {
                'ts_code': ts_code or '',
                'trade_date': trade_date or '',
                'start_date': start_date or '',
                'end_date': end_date or ''
            }
            df = cache_manager.get_dataframe('stk_auction', **cache_params)
            
            # 检查是否需要更新（过期后立即更新）
            need_update = False
            if df is None:
                need_update = True
            elif cache_manager.is_expired('stk_auction', **cache_params):
                need_update = True
            
            if need_update:
                # 过期后立即更新（同步）
                try:
                    df = pro.stk_auction(**params)
                    
                    # 保存到缓存（创建新版本）
                    if not df.empty:
                        cache_manager.set('stk_auction', df, **cache_params)
                except Exception as api_error:
                    error_msg = str(api_error)
                    # 检查是否是接口名错误或权限问题
                    if '接口名' in error_msg or 'api_name' in error_msg.lower() or '请指定正确的接口名' in error_msg:
                        return f"API接口调用失败：{error_msg}\n\n已使用接口：stk_auction\n\n可能的原因：\n1. Tushare token是否有效\n2. 账户是否开通了股票分钟权限（已开通的用户可自动获得本接口权限）\n3. 网络连接是否正常\n4. 查询日期是否为交易日\n5. 查询时间是否在9点25~29分之间（当日数据）\n\n建议：\n- 请查看Tushare文档确认stk_auction接口是否可用\n- 检查是否已开通股票分钟权限"
                    else:
                        return f"API调用失败：{error_msg}\n请检查：\n1. Tushare token是否有效\n2. 账户是否开通了股票分钟权限\n3. 网络连接是否正常\n4. 查询日期是否为交易日"
            
            if df is None or df.empty:
                param_info = []
                if trade_date:
                    param_info.append(f"交易日期: {trade_date}")
                if ts_code:
                    param_info.append(f"股票代码: {ts_code}")
                if start_date and end_date:
                    param_info.append(f"日期范围: {start_date} 至 {end_date}")
                
                param_str = "、".join(param_info) if param_info else "指定条件"
                return f"未找到符合{param_str}的集合竞价数据\n\n提示：\n- 当日数据需要在9点25~29分之间查询\n- 请确认查询日期是否为交易日\n- 请确认股票代码是否正确"
            
            # 格式化输出
            return format_stock_auction_data(df, ts_code or "")
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            return f"查询失败：{str(e)}\n详细信息：{error_detail}"


def format_holder_number_data(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化股东户数数据
    
    参数:
        df: 股东户数数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的股东户数数据"
    
    result = []
    result.append(f"📊 {ts_code} 股东户数变化")
    result.append("-" * 100)
    result.append("")
    
    # 表头
    result.append(f"{'公告日期':<12} {'截止日期':<12} {'股东户数':<12} {'变化':<12} {'变化率':<12}")
    result.append("-" * 100)
    
    # 按公告日期排序（最新的在前）
    if 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    elif 'end_date' in df.columns:
        df = df.sort_values('end_date', ascending=False)
    
    # 计算变化
    df = df.copy()
    if 'holder_num' in df.columns:
        df['holder_num'] = pd.to_numeric(df['holder_num'], errors='coerce')
        # 计算变化（与上一条记录比较）
        df['change'] = df['holder_num'].diff().fillna(0)
        df['change_pct'] = (df['change'] / df['holder_num'].shift(1) * 100).fillna(0)
    
    for _, row in df.iterrows():
        # 公告日期
        ann_date = format_date(str(row['ann_date'])) if 'ann_date' in row and pd.notna(row['ann_date']) else "-"
        
        # 截止日期
        end_date = format_date(str(row['end_date'])) if 'end_date' in row and pd.notna(row['end_date']) else "-"
        
        # 股东户数
        holder_num = f"{int(row['holder_num']):,}" if 'holder_num' in row and pd.notna(row['holder_num']) else "-"
        
        # 变化
        change = "-"
        if 'change' in row and pd.notna(row['change']):
            change_val = row['change']
            if change_val > 0:
                change = f"+{int(change_val):,}"
            elif change_val < 0:
                change = f"{int(change_val):,}"
            else:
                change = "0"
        
        # 变化率
        change_pct = "-"
        if 'change_pct' in row and pd.notna(row['change_pct']):
            change_pct_val = row['change_pct']
            if change_pct_val > 0:
                change_pct = f"+{change_pct_val:.2f}%"
            elif change_pct_val < 0:
                change_pct = f"{change_pct_val:.2f}%"
            else:
                change_pct = "0.00%"
        
        result.append(f"{ann_date:<12} {end_date:<12} {holder_num:<12} {change:<12} {change_pct:<12}")
    
    # 统计信息
    result.append("")
    result.append("📊 统计信息：")
    
    if 'holder_num' in df.columns and len(df) > 1:
        # 最新股东户数
        latest_num = df['holder_num'].iloc[0]
        oldest_num = df['holder_num'].iloc[-1]
        result.append(f"  - 最新股东户数: {int(latest_num):,} 户")
        result.append(f"  - 最早股东户数: {int(oldest_num):,} 户")
        
        # 总变化
        total_change = latest_num - oldest_num
        if total_change > 0:
            result.append(f"  - 总变化: +{int(total_change):,} 户（持股分散）")
        elif total_change < 0:
            result.append(f"  - 总变化: {int(total_change):,} 户（持股集中）")
        else:
            result.append(f"  - 总变化: 0 户")
        
        # 变化率
        if oldest_num > 0:
            total_change_pct = (total_change / oldest_num) * 100
            result.append(f"  - 变化率: {total_change_pct:+.2f}%")
    
    # 数据点数量
    result.append(f"  - 数据点数量: {len(df)} 个")
    
    return "\n".join(result)

def format_holder_trade_data(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化股东增减持数据
    
    参数:
        df: 增减持数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的增减持数据"
    
    result = []
    result.append(f"📊 {ts_code} 股东增减持记录")
    result.append("-" * 120)
    result.append("")
    
    # 表头
    result.append(f"{'公告日期':<12} {'股东名称':<25} {'类型':<8} {'变动数量(股)':<18} {'占流通比例(%)':<15} {'平均价格':<12} {'变动后持股(股)':<18}")
    result.append("-" * 120)
    
    # 按公告日期排序（最新的在前）
    if 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    
    for _, row in df.iterrows():
        # 公告日期
        ann_date = format_date(str(row['ann_date'])) if 'ann_date' in row and pd.notna(row['ann_date']) else "-"
        
        # 股东名称
        holder_name = str(row['holder_name'])[:23] if 'holder_name' in row and pd.notna(row['holder_name']) else "-"
        
        # 交易类型
        in_de = row.get('in_de', '-')
        if in_de == 'IN':
            trade_type = "增持"
        elif in_de == 'DE':
            trade_type = "减持"
        else:
            trade_type = str(in_de)
        
        # 变动数量
        change_vol = f"{int(row['change_vol']):,}" if 'change_vol' in row and pd.notna(row['change_vol']) else "-"
        
        # 占流通比例
        change_ratio = f"{row['change_ratio']:.2f}%" if 'change_ratio' in row and pd.notna(row['change_ratio']) else "-"
        
        # 平均价格
        avg_price = f"{row['avg_price']:.2f}" if 'avg_price' in row and pd.notna(row['avg_price']) else "-"
        
        # 变动后持股
        after_share = f"{int(row['after_share']):,}" if 'after_share' in row and pd.notna(row['after_share']) else "-"
        
        result.append(f"{ann_date:<12} {holder_name:<25} {trade_type:<8} {change_vol:<18} {change_ratio:<15} {avg_price:<12} {after_share:<18}")
    
    # 统计信息
    result.append("")
    result.append("📊 统计信息：")
    
    # 增持/减持统计
    if 'in_de' in df.columns:
        increase_count = len(df[df['in_de'] == 'IN'])
        decrease_count = len(df[df['in_de'] == 'DE'])
        result.append(f"  - 增持记录: {increase_count} 条")
        result.append(f"  - 减持记录: {decrease_count} 条")
    
    # 股东类型统计
    if 'holder_type' in df.columns:
        holder_type_map = {"C": "公司", "P": "个人", "G": "高管"}
        for htype, count in df['holder_type'].value_counts().items():
            type_name = holder_type_map.get(htype, htype)
            result.append(f"  - {type_name}股东: {count} 条")
    
    # 总变动数量
    if 'change_vol' in df.columns:
        total_change = df['change_vol'].sum()
        if total_change > 0:
            result.append(f"  - 净增持: {int(total_change):,} 股")
        elif total_change < 0:
            result.append(f"  - 净减持: {int(abs(total_change)):,} 股")
        else:
            result.append(f"  - 净变动: 0 股")
    
    return "\n".join(result)

def format_stock_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化股票日线行情数据输出
    
    参数:
        df: 日线行情数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_daily(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 80)
                result.append(f"{'股票代码':<15} {'收盘价':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 80)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_stock_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的日线行情数据
    
    参数:
        df: 单个股票的日线行情数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 日线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 80)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<10} {high:<10} {low:<10} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 80)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"开盘价: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘价: -")
        result.append(f"最高价: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高价: -")
        result.append(f"最低价: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低价: -")
        result.append(f"收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘价: -")
        result.append(f"昨收价: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收价: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌额: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_stock_weekly_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化股票周线行情数据输出
    
    参数:
        df: 周线行情数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的周线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_weekly(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10周
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易周（最后交易日）: {format_date(date)}")
                result.append("=" * 100)
                result.append(f"{'股票代码':<15} {'收盘价':<10} {'涨跌额':<10} {'涨跌幅':<10} {'波动范围':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 100)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    # 计算波动范围（最高价 - 最低价）
                    if pd.notna(row.get('high')) and pd.notna(row.get('low')):
                        swing_range = f"{row['high'] - row['low']:.2f}"
                    else:
                        swing_range = "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<10} {change:<10} {pct_chg:<10} {swing_range:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_stock_weekly(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的周线行情数据
    
    参数:
        df: 单个股票的周线行情数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的周线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 周线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20周）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 周数据：")
    result.append("")
    result.append(f"{'交易周':<12} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'涨跌额':<10} {'涨跌幅':<10} {'波动范围':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        # 计算波动范围（最高价 - 最低价）
        if pd.notna(row['high']) and pd.notna(row['low']):
            swing_range = f"{row['high'] - row['low']:.2f}"
        else:
            swing_range = "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<10} {high:<10} {low:<10} {close:<10} {change:<10} {pct_chg:<10} {swing_range:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新周数据摘要：")
        result.append("-" * 80)
        result.append(f"交易周（最后交易日）: {format_date(latest['trade_date'])}")
        result.append(f"周开盘价: {latest['open']:.2f}" if pd.notna(latest['open']) else "周开盘价: -")
        result.append(f"周最高价: {latest['high']:.2f}" if pd.notna(latest['high']) else "周最高价: -")
        result.append(f"周最低价: {latest['low']:.2f}" if pd.notna(latest['low']) else "周最低价: -")
        result.append(f"周收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "周收盘价: -")
        result.append(f"上周收盘价: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "上周收盘价: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌额: {latest['change']:+.2f} (收盘价 - 上周收盘价)")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        # 添加波动范围
        if pd.notna(latest.get('high')) and pd.notna(latest.get('low')):
            swing_range = latest['high'] - latest['low']
            result.append(f"波动范围: {swing_range:.2f} (最高价 - 最低价)")
        if pd.notna(latest.get('vol')):
            result.append(f"周成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"周成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_index_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化A股指数日线行情数据输出
    
    参数:
        df: 指数日线行情数据DataFrame
        ts_code: 指数代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的指数日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个指数或多个指数
    if ts_code:
        # 按指数代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            index_df = df[df['ts_code'] == code]
            if not index_df.empty:
                result.append(format_single_index_daily(index_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有指数
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 80)
                result.append(f"{'指数代码':<15} {'收盘点位':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 80)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<12} {change:<12} {pct_chg:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_index_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个指数的日线行情数据
    
    参数:
        df: 单个指数的日线行情数据DataFrame
        ts_code: 指数代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 日线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'涨跌点位':<12} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<12} {high:<12} {low:<12} {close:<12} {change:<12} {pct_chg:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 80)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"开盘点位: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘点位: -")
        result.append(f"最高点位: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高点位: -")
        result.append(f"最低点位: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低点位: -")
        result.append(f"收盘点位: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘点位: -")
        result.append(f"昨收点位: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收点位: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌点位: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_etf_daily_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化ETF日线行情数据输出
    
    参数:
        df: ETF日线行情数据DataFrame
        ts_code: ETF代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的ETF日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个ETF或多个ETF
    if ts_code:
        # 按ETF代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            etf_df = df[df['ts_code'] == code]
            if not etf_df.empty:
                result.append(format_single_etf_daily(etf_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有ETF
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 80)
                result.append(f"{'ETF代码':<15} {'收盘价':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
                result.append("-" * 80)
                for _, row in date_df.iterrows():
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
                    pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
                    vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
                    amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
                    result.append(f"{row['ts_code']:<15} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
                result.append("")
    
    return "\n".join(result)

def format_single_etf_daily(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个ETF的日线行情数据
    
    参数:
        df: 单个ETF的日线行情数据DataFrame
        ts_code: ETF代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的日线行情数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} ETF日线行情")
    result.append("=" * 80)
    result.append("")
    
    # 显示最近的数据（最多20条）
    display_count = min(20, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'涨跌额':<10} {'涨跌幅':<10} {'成交量(手)':<15} {'成交额(千元)':<15}")
    result.append("-" * 100)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        open_price = f"{row['open']:.2f}" if pd.notna(row['open']) else "-"
        high = f"{row['high']:.2f}" if pd.notna(row['high']) else "-"
        low = f"{row['low']:.2f}" if pd.notna(row['low']) else "-"
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        change = f"{row['change']:+.2f}" if pd.notna(row['change']) else "-"
        pct_chg = f"{row['pct_chg']:+.2f}%" if pd.notna(row['pct_chg']) else "-"
        vol = f"{row['vol']:.0f}" if pd.notna(row['vol']) else "-"
        amount = f"{row['amount']:.0f}" if pd.notna(row['amount']) else "-"
        
        result.append(f"{trade_date:<12} {open_price:<10} {high:<10} {low:<10} {close:<10} {change:<10} {pct_chg:<10} {vol:<15} {amount:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 80)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"开盘价: {latest['open']:.2f}" if pd.notna(latest['open']) else "开盘价: -")
        result.append(f"最高价: {latest['high']:.2f}" if pd.notna(latest['high']) else "最高价: -")
        result.append(f"最低价: {latest['low']:.2f}" if pd.notna(latest['low']) else "最低价: -")
        result.append(f"收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘价: -")
        result.append(f"昨收价: {latest['pre_close']:.2f}" if pd.notna(latest.get('pre_close')) else "昨收价: -")
        if pd.notna(latest.get('change')):
            result.append(f"涨跌额: {latest['change']:+.2f}")
        if pd.notna(latest.get('pct_chg')):
            result.append(f"涨跌幅: {latest['pct_chg']:+.2f}%")
        if pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest['vol']:.0f} 手")
        if pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest['amount']:.0f} 千元")
    
    return "\n".join(result)

def format_stock_survey_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化机构调研数据输出
    
    参数:
        df: 机构调研数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的机构调研数据"
    
    # 按调研日期排序（最新的在前）
    df = df.sort_values('surv_date', ascending=False)
    
    result = []
    result.append("📊 上市公司机构调研记录")
    result.append("=" * 140)
    result.append("")
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_survey(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['surv_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个调研日期
            date_df = df[df['surv_date'] == date]
            if not date_df.empty:
                result.append(f"📅 调研日期: {format_date(date)}")
                result.append("=" * 140)
                # 按股票代码分组
                stocks = date_df['ts_code'].unique()
                for stock in stocks[:10]:  # 最多显示10只股票
                    stock_df = date_df[date_df['ts_code'] == stock]
                    stock_name = stock_df.iloc[0]['name'] if pd.notna(stock_df.iloc[0].get('name')) else stock
                    result.append(f"股票: {stock} ({stock_name}) - 共 {len(stock_df)} 条调研记录")
                    result.append("-" * 140)
                    result.append(f"{'机构参与人员':<20} {'接待公司':<30} {'接待方式':<15} {'接待地点':<20} {'上市公司接待人员':<20}")
                    result.append("-" * 140)
                    for _, row in stock_df.head(5).iterrows():  # 每只股票最多显示5条
                        fund_visitors = str(row['fund_visitors'])[:18] if pd.notna(row.get('fund_visitors')) else "-"
                        rece_org = str(row['rece_org'])[:28] if pd.notna(row.get('rece_org')) else "-"
                        rece_mode = str(row['rece_mode'])[:13] if pd.notna(row.get('rece_mode')) else "-"
                        rece_place = str(row['rece_place'])[:18] if pd.notna(row.get('rece_place')) else "-"
                        comp_rece = str(row['comp_rece'])[:18] if pd.notna(row.get('comp_rece')) else "-"
                        result.append(f"{fund_visitors:<20} {rece_org:<30} {rece_mode:<15} {rece_place:<20} {comp_rece:<20}")
                    if len(stock_df) > 5:
                        result.append(f"（共 {len(stock_df)} 条记录，仅显示前 5 条）")
                    result.append("")
                if len(stocks) > 10:
                    result.append(f"（共 {len(stocks)} 只股票，仅显示前 10 只）")
                result.append("")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源于上市公司披露的机构调研记录")
    result.append("  - 机构调研可以反映市场对公司的关注度")
    result.append("  - 调研频率和参与机构数量可以作为投资参考指标")
    
    return "\n".join(result)


def format_single_stock_survey(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的机构调研数据
    
    参数:
        df: 单个股票的机构调研数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的机构调研数据"
    
    # 按调研日期排序（最新的在前）
    df = df.sort_values('surv_date', ascending=False)
    
    stock_name = df.iloc[0]['name'] if pd.notna(df.iloc[0].get('name')) else ts_code
    result = []
    result.append(f"📈 {ts_code} ({stock_name}) 机构调研记录")
    result.append("=" * 140)
    result.append("")
    
    # 统计信息
    unique_dates = df['surv_date'].nunique()
    total_records = len(df)
    result.append(f"📊 统计信息：")
    result.append(f"  - 调研日期数: {unique_dates} 个")
    result.append(f"  - 调研记录数: {total_records} 条")
    
    # 统计参与机构
    if 'rece_org' in df.columns:
        unique_orgs = df['rece_org'].nunique()
        result.append(f"  - 参与机构数: {unique_orgs} 家")
    
    # 统计参与人员
    if 'fund_visitors' in df.columns:
        unique_visitors = df['fund_visitors'].nunique()
        result.append(f"  - 参与人员数: {unique_visitors} 人")
    
    result.append("")
    
    # 按调研日期分组显示
    dates = df['surv_date'].unique()
    display_dates = sorted(dates, reverse=True)[:10]  # 最多显示最近10个调研日期
    
    for date in display_dates:
        date_df = df[df['surv_date'] == date]
        if not date_df.empty:
            result.append(f"📅 调研日期: {format_date(date)} (共 {len(date_df)} 条记录)")
            result.append("-" * 140)
            result.append(f"{'机构参与人员':<20} {'接待公司':<30} {'接待方式':<15} {'接待地点':<20} {'上市公司接待人员':<20}")
            result.append("-" * 140)
            
            for _, row in date_df.iterrows():
                fund_visitors = str(row['fund_visitors'])[:18] if pd.notna(row.get('fund_visitors')) else "-"
                rece_org = str(row['rece_org'])[:28] if pd.notna(row.get('rece_org')) else "-"
                rece_mode = str(row['rece_mode'])[:13] if pd.notna(row.get('rece_mode')) else "-"
                rece_place = str(row['rece_place'])[:18] if pd.notna(row.get('rece_place')) else "-"
                comp_rece = str(row['comp_rece'])[:18] if pd.notna(row.get('comp_rece')) else "-"
                result.append(f"{fund_visitors:<20} {rece_org:<30} {rece_mode:<15} {rece_place:<20} {comp_rece:<20}")
            
            result.append("")
    
    if len(dates) > 10:
        result.append(f"（共 {len(dates)} 个调研日期，仅显示最近 10 个）")
    
    return "\n".join(result)


def format_cyq_perf_data(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化每日筹码及胜率数据输出
    
    参数:
        df: 每日筹码及胜率数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的每日筹码及胜率数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📊 {ts_code} 每日筹码及胜率数据")
    result.append("=" * 140)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'5分位成本':<12} {'50分位成本':<12} {'95分位成本':<12} {'加权平均成本':<14} {'胜率(%)':<10} {'筹码集中度':<12}")
    result.append("-" * 140)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        cost_5pct = f"{row['cost_5pct']:.2f}" if pd.notna(row['cost_5pct']) else "-"
        cost_50pct = f"{row['cost_50pct']:.2f}" if pd.notna(row['cost_50pct']) else "-"
        cost_95pct = f"{row['cost_95pct']:.2f}" if pd.notna(row['cost_95pct']) else "-"
        weight_avg = f"{row['weight_avg']:.2f}" if pd.notna(row['weight_avg']) else "-"
        winner_rate = f"{row['winner_rate']:.2f}" if pd.notna(row['winner_rate']) else "-"
        
        # 筹码集中度
        concentration = "-"
        if 'concentration' in row and pd.notna(row['concentration']):
            concentration = f"{row['concentration']:.4f}"
        
        result.append(f"{trade_date:<12} {cost_5pct:<12} {cost_50pct:<12} {cost_95pct:<12} {weight_avg:<14} {winner_rate:<10} {concentration:<12}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 140)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"历史最低价: {latest['his_low']:.2f}" if pd.notna(latest['his_low']) else "历史最低价: -")
        result.append(f"历史最高价: {latest['his_high']:.2f}" if pd.notna(latest['his_high']) else "历史最高价: -")
        result.append(f"5分位成本: {latest['cost_5pct']:.2f}" if pd.notna(latest['cost_5pct']) else "5分位成本: -")
        result.append(f"15分位成本: {latest['cost_15pct']:.2f}" if pd.notna(latest.get('cost_15pct')) else "15分位成本: -")
        result.append(f"50分位成本: {latest['cost_50pct']:.2f}" if pd.notna(latest['cost_50pct']) else "50分位成本: -")
        result.append(f"85分位成本: {latest['cost_85pct']:.2f}" if pd.notna(latest.get('cost_85pct')) else "85分位成本: -")
        result.append(f"95分位成本: {latest['cost_95pct']:.2f}" if pd.notna(latest['cost_95pct']) else "95分位成本: -")
        result.append(f"加权平均成本: {latest['weight_avg']:.2f}" if pd.notna(latest['weight_avg']) else "加权平均成本: -")
        result.append(f"胜率: {latest['winner_rate']:.2f}%" if pd.notna(latest['winner_rate']) else "胜率: -")
        
        # 筹码集中度
        if 'concentration' in latest and pd.notna(latest['concentration']):
            concentration = latest['concentration']
            result.append(f"筹码集中度: {concentration:.4f}")
            # 解释集中度含义
            if concentration < 0.1:
                result.append("  → 筹码高度集中（集中度 < 0.1）")
            elif concentration < 0.2:
                result.append("  → 筹码较为集中（集中度 0.1-0.2）")
            elif concentration < 0.3:
                result.append("  → 筹码中等集中（集中度 0.2-0.3）")
            else:
                result.append("  → 筹码较为分散（集中度 > 0.3）")
        
        # 计算成本区间
        if pd.notna(latest.get('cost_5pct')) and pd.notna(latest.get('cost_95pct')):
            cost_range = latest['cost_95pct'] - latest['cost_5pct']
            result.append(f"成本区间: {cost_range:.2f} (95分位 - 5分位)")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据每天17~18点左右更新，数据从2018年开始")
    result.append("  - 筹码集中度计算公式：集中度 = (cost_95pct - cost_5pct) / (cost_95pct + cost_5pct)")
    result.append("  - 集中度越小，说明筹码越集中；集中度越大，说明筹码越分散")
    result.append("  - 胜率：当前价格高于持仓成本的比例")
    
    return "\n".join(result)


def format_moneyflow_dc_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化资金流向数据输出
    
    参数:
        df: 资金流向数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的资金流向数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_moneyflow_dc(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 如果有多个交易日期，按日期分组显示
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
            dates = sorted(df['trade_date'].unique(), reverse=True)
            for date in dates[:10]:  # 最多显示最近10个交易日
                date_df = df[df['trade_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 交易日期: {format_date(date)}")
                    result.append("=" * 160)
                    result.append(f"{'股票代码':<15} {'股票名称':<15} {'涨跌幅':<10} {'最新价':<10} {'主力净流入(万)':<18} {'主力净流入占比':<16} {'超大单净流入(万)':<18} {'超大单占比':<14} {'大单净流入(万)':<16} {'大单占比':<12} {'中单净流入(万)':<16} {'中单占比':<12} {'小单净流入(万)':<16} {'小单占比':<12}")
                    result.append("-" * 160)
                    
                    # 按主力净流入额排序（降序）
                    if 'net_amount' in date_df.columns:
                        date_df = date_df.sort_values('net_amount', ascending=False)
                    
                    for _, row in date_df.iterrows():
                        code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
                        name = str(row['name'])[:12] if 'name' in row and pd.notna(row['name']) else "-"
                        pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
                        close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
                        net_amount = f"{row['net_amount']:.2f}" if 'net_amount' in row and pd.notna(row['net_amount']) else "-"
                        net_amount_rate = f"{row['net_amount_rate']:+.2f}%" if 'net_amount_rate' in row and pd.notna(row['net_amount_rate']) else "-"
                        buy_elg_amount = f"{row['buy_elg_amount']:.2f}" if 'buy_elg_amount' in row and pd.notna(row['buy_elg_amount']) else "-"
                        buy_elg_amount_rate = f"{row['buy_elg_amount_rate']:+.2f}%" if 'buy_elg_amount_rate' in row and pd.notna(row['buy_elg_amount_rate']) else "-"
                        buy_lg_amount = f"{row['buy_lg_amount']:.2f}" if 'buy_lg_amount' in row and pd.notna(row['buy_lg_amount']) else "-"
                        buy_lg_amount_rate = f"{row['buy_lg_amount_rate']:+.2f}%" if 'buy_lg_amount_rate' in row and pd.notna(row['buy_lg_amount_rate']) else "-"
                        buy_md_amount = f"{row['buy_md_amount']:.2f}" if 'buy_md_amount' in row and pd.notna(row['buy_md_amount']) else "-"
                        buy_md_amount_rate = f"{row['buy_md_amount_rate']:+.2f}%" if 'buy_md_amount_rate' in row and pd.notna(row['buy_md_amount_rate']) else "-"
                        buy_sm_amount = f"{row['buy_sm_amount']:.2f}" if 'buy_sm_amount' in row and pd.notna(row['buy_sm_amount']) else "-"
                        buy_sm_amount_rate = f"{row['buy_sm_amount_rate']:+.2f}%" if 'buy_sm_amount_rate' in row and pd.notna(row['buy_sm_amount_rate']) else "-"
                        
                        result.append(f"{code:<15} {name:<15} {pct_change:<10} {close:<10} {net_amount:<18} {net_amount_rate:<16} {buy_elg_amount:<18} {buy_elg_amount_rate:<14} {buy_lg_amount:<16} {buy_lg_amount_rate:<12} {buy_md_amount:<16} {buy_md_amount_rate:<12} {buy_sm_amount:<16} {buy_sm_amount_rate:<12}")
                    result.append("")
        else:
            # 单个日期或单个股票，使用详细格式
            if ts_code and len(df['ts_code'].unique()) == 1:
                result.append(format_single_moneyflow_dc(df, df['ts_code'].iloc[0]))
            else:
                # 显示所有股票
                result.append("📊 资金流向数据")
                result.append("=" * 160)
                result.append(f"{'股票代码':<15} {'股票名称':<15} {'涨跌幅':<10} {'最新价':<10} {'主力净流入(万)':<18} {'主力净流入占比':<16} {'超大单净流入(万)':<18} {'超大单占比':<14} {'大单净流入(万)':<16} {'大单占比':<12} {'中单净流入(万)':<16} {'中单占比':<12} {'小单净流入(万)':<16} {'小单占比':<12}")
                result.append("-" * 160)
                
                for _, row in df.iterrows():
                    code = str(row['ts_code']) if 'ts_code' in row and pd.notna(row['ts_code']) else "-"
                    name = str(row['name'])[:12] if 'name' in row and pd.notna(row['name']) else "-"
                    pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
                    close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
                    net_amount = f"{row['net_amount']:.2f}" if 'net_amount' in row and pd.notna(row['net_amount']) else "-"
                    net_amount_rate = f"{row['net_amount_rate']:+.2f}%" if 'net_amount_rate' in row and pd.notna(row['net_amount_rate']) else "-"
                    buy_elg_amount = f"{row['buy_elg_amount']:.2f}" if 'buy_elg_amount' in row and pd.notna(row['buy_elg_amount']) else "-"
                    buy_elg_amount_rate = f"{row['buy_elg_amount_rate']:+.2f}%" if 'buy_elg_amount_rate' in row and pd.notna(row['buy_elg_amount_rate']) else "-"
                    buy_lg_amount = f"{row['buy_lg_amount']:.2f}" if 'buy_lg_amount' in row and pd.notna(row['buy_lg_amount']) else "-"
                    buy_lg_amount_rate = f"{row['buy_lg_amount_rate']:+.2f}%" if 'buy_lg_amount_rate' in row and pd.notna(row['buy_lg_amount_rate']) else "-"
                    buy_md_amount = f"{row['buy_md_amount']:.2f}" if 'buy_md_amount' in row and pd.notna(row['buy_md_amount']) else "-"
                    buy_md_amount_rate = f"{row['buy_md_amount_rate']:+.2f}%" if 'buy_md_amount_rate' in row and pd.notna(row['buy_md_amount_rate']) else "-"
                    buy_sm_amount = f"{row['buy_sm_amount']:.2f}" if 'buy_sm_amount' in row and pd.notna(row['buy_sm_amount']) else "-"
                    buy_sm_amount_rate = f"{row['buy_sm_amount_rate']:+.2f}%" if 'buy_sm_amount_rate' in row and pd.notna(row['buy_sm_amount_rate']) else "-"
                    
                    result.append(f"{code:<15} {name:<15} {pct_change:<10} {close:<10} {net_amount:<18} {net_amount_rate:<16} {buy_elg_amount:<18} {buy_elg_amount_rate:<14} {buy_lg_amount:<16} {buy_lg_amount_rate:<12} {buy_md_amount:<16} {buy_md_amount_rate:<12} {buy_sm_amount:<16} {buy_sm_amount_rate:<12}")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：东方财富，每日盘后更新，数据开始于20230911")
    result.append("  - 主力净流入 = 超大单净流入 + 大单净流入")
    result.append("  - 正数表示净流入，负数表示净流出")
    result.append("  - 权限要求：5000积分")
    result.append("  - 限量：单次最大获取6000条数据")
    
    return "\n".join(result)


def format_single_moneyflow_dc(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的资金流向数据
    
    参数:
        df: 单个股票的资金流向数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的资金流向数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    stock_name = str(df.iloc[0]['name']) if 'name' in df.columns and pd.notna(df.iloc[0]['name']) else ts_code
    result.append(f"💰 {ts_code} {stock_name} 资金流向数据")
    result.append("=" * 160)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'涨跌幅':<10} {'最新价':<10} {'主力净流入(万)':<18} {'主力净流入占比':<16} {'超大单净流入(万)':<18} {'超大单占比':<14} {'大单净流入(万)':<16} {'大单占比':<12} {'中单净流入(万)':<16} {'中单占比':<12} {'小单净流入(万)':<16} {'小单占比':<12}")
    result.append("-" * 160)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(str(row['trade_date']))
        pct_change = f"{row['pct_change']:+.2f}%" if 'pct_change' in row and pd.notna(row['pct_change']) else "-"
        close = f"{row['close']:.2f}" if 'close' in row and pd.notna(row['close']) else "-"
        net_amount = f"{row['net_amount']:.2f}" if 'net_amount' in row and pd.notna(row['net_amount']) else "-"
        net_amount_rate = f"{row['net_amount_rate']:+.2f}%" if 'net_amount_rate' in row and pd.notna(row['net_amount_rate']) else "-"
        buy_elg_amount = f"{row['buy_elg_amount']:.2f}" if 'buy_elg_amount' in row and pd.notna(row['buy_elg_amount']) else "-"
        buy_elg_amount_rate = f"{row['buy_elg_amount_rate']:+.2f}%" if 'buy_elg_amount_rate' in row and pd.notna(row['buy_elg_amount_rate']) else "-"
        buy_lg_amount = f"{row['buy_lg_amount']:.2f}" if 'buy_lg_amount' in row and pd.notna(row['buy_lg_amount']) else "-"
        buy_lg_amount_rate = f"{row['buy_lg_amount_rate']:+.2f}%" if 'buy_lg_amount_rate' in row and pd.notna(row['buy_lg_amount_rate']) else "-"
        buy_md_amount = f"{row['buy_md_amount']:.2f}" if 'buy_md_amount' in row and pd.notna(row['buy_md_amount']) else "-"
        buy_md_amount_rate = f"{row['buy_md_amount_rate']:+.2f}%" if 'buy_md_amount_rate' in row and pd.notna(row['buy_md_amount_rate']) else "-"
        buy_sm_amount = f"{row['buy_sm_amount']:.2f}" if 'buy_sm_amount' in row and pd.notna(row['buy_sm_amount']) else "-"
        buy_sm_amount_rate = f"{row['buy_sm_amount_rate']:+.2f}%" if 'buy_sm_amount_rate' in row and pd.notna(row['buy_sm_amount_rate']) else "-"
        
        result.append(f"{trade_date:<12} {pct_change:<10} {close:<10} {net_amount:<18} {net_amount_rate:<16} {buy_elg_amount:<18} {buy_elg_amount_rate:<14} {buy_lg_amount:<16} {buy_lg_amount_rate:<12} {buy_md_amount:<16} {buy_md_amount_rate:<12} {buy_sm_amount:<16} {buy_sm_amount_rate:<12}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 160)
        trade_date_str = str(latest.get('trade_date', '-'))
        result.append(f"交易日期: {format_date(trade_date_str)}")
        result.append(f"股票名称: {latest.get('name', '-')}")
        result.append(f"涨跌幅: {latest.get('pct_change', 0):+.2f}%" if pd.notna(latest.get('pct_change')) else "涨跌幅: -")
        result.append(f"最新价: {latest.get('close', 0):.2f}" if pd.notna(latest.get('close')) else "最新价: -")
        result.append("")
        result.append("资金流向：")
        result.append(f"  主力净流入: {latest.get('net_amount', 0):.2f} 万元 ({latest.get('net_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('net_amount')) else "  主力净流入: -")
        result.append(f"  超大单净流入: {latest.get('buy_elg_amount', 0):.2f} 万元 ({latest.get('buy_elg_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_elg_amount')) else "  超大单净流入: -")
        result.append(f"  大单净流入: {latest.get('buy_lg_amount', 0):.2f} 万元 ({latest.get('buy_lg_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_lg_amount')) else "  大单净流入: -")
        result.append(f"  中单净流入: {latest.get('buy_md_amount', 0):.2f} 万元 ({latest.get('buy_md_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_md_amount')) else "  中单净流入: -")
        result.append(f"  小单净流入: {latest.get('buy_sm_amount', 0):.2f} 万元 ({latest.get('buy_sm_amount_rate', 0):+.2f}%)" if pd.notna(latest.get('buy_sm_amount')) else "  小单净流入: -")
    
    return "\n".join(result)


def format_daily_basic_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化每日指标数据输出
    
    参数:
        df: 每日指标数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的每日指标数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append("📊 每日指标数据")
    result.append("=" * 160)
    result.append("")
    
    # 如果查询的是单个股票或多个股票
    if ts_code:
        # 按股票代码分组显示
        codes = [code.strip() for code in ts_code.split(',')]
        for code in codes:
            stock_df = df[df['ts_code'] == code]
            if not stock_df.empty:
                result.append(format_single_stock_daily_basic(stock_df, code))
                result.append("")  # 添加空行分隔
    else:
        # 按日期查询，显示所有股票
        # 按日期分组
        dates = df['trade_date'].unique()
        for date in sorted(dates, reverse=True)[:10]:  # 最多显示最近10个交易日
            date_df = df[df['trade_date'] == date]
            if not date_df.empty:
                result.append(f"📅 交易日期: {format_date(date)}")
                result.append("=" * 160)
                result.append(f"{'股票代码':<15} {'收盘价':<10} {'换手率(%)':<12} {'量比':<10} {'PE':<10} {'PB':<10} {'PS':<10} {'总市值(万)':<15} {'流通市值(万)':<15}")
                result.append("-" * 160)
                
                for _, row in date_df.head(20).iterrows():  # 最多显示20只股票
                    code = row['ts_code']
                    close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
                    turnover_rate = f"{row['turnover_rate']:.2f}" if pd.notna(row['turnover_rate']) else "-"
                    volume_ratio = f"{row['volume_ratio']:.2f}" if pd.notna(row['volume_ratio']) else "-"
                    pe = f"{row['pe']:.2f}" if pd.notna(row['pe']) else "-"
                    pb = f"{row['pb']:.2f}" if pd.notna(row['pb']) else "-"
                    ps = f"{row['ps']:.2f}" if pd.notna(row['ps']) else "-"
                    total_mv = format_large_number(row['total_mv']) if pd.notna(row['total_mv']) else "-"
                    circ_mv = format_large_number(row['circ_mv']) if pd.notna(row['circ_mv']) else "-"
                    
                    result.append(f"{code:<15} {close:<10} {turnover_rate:<12} {volume_ratio:<10} {pe:<10} {pb:<10} {ps:<10} {total_mv:<15} {circ_mv:<15}")
                
                if len(date_df) > 20:
                    result.append(f"（共 {len(date_df)} 只股票，仅显示前 20 只）")
                result.append("")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - PE：市盈率（总市值/净利润），亏损的PE为空")
    result.append("  - PB：市净率（总市值/净资产）")
    result.append("  - PS：市销率")
    result.append("  - 换手率：反映股票流动性，换手率越高，流动性越好")
    result.append("  - 量比：当日成交量与前5日平均成交量的比值，反映成交活跃度")
    
    return "\n".join(result)


def format_single_stock_daily_basic(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的每日指标数据
    
    参数:
        df: 单个股票的每日指标数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的每日指标数据"
    
    # 按日期排序（最新的在前）
    df = df.sort_values('trade_date', ascending=False)
    
    result = []
    result.append(f"📈 {ts_code} 每日指标数据")
    result.append("=" * 160)
    result.append("")
    
    # 显示最近的数据（最多30条）
    display_count = min(30, len(df))
    result.append(f"最近 {display_count} 个交易日数据：")
    result.append("")
    result.append(f"{'日期':<12} {'收盘价':<10} {'换手率(%)':<12} {'换手率(自由)':<15} {'量比':<10} {'PE':<10} {'PE(TTM)':<12} {'PB':<10} {'PS':<10} {'PS(TTM)':<12} {'股息率(%)':<12} {'总市值(万)':<15} {'流通市值(万)':<15}")
    result.append("-" * 160)
    
    for _, row in df.head(display_count).iterrows():
        trade_date = format_date(row['trade_date'])
        close = f"{row['close']:.2f}" if pd.notna(row['close']) else "-"
        turnover_rate = f"{row['turnover_rate']:.2f}" if pd.notna(row['turnover_rate']) else "-"
        turnover_rate_f = f"{row['turnover_rate_f']:.2f}" if pd.notna(row['turnover_rate_f']) else "-"
        volume_ratio = f"{row['volume_ratio']:.2f}" if pd.notna(row['volume_ratio']) else "-"
        pe = f"{row['pe']:.2f}" if pd.notna(row['pe']) else "-"
        pe_ttm = f"{row['pe_ttm']:.2f}" if pd.notna(row['pe_ttm']) else "-"
        pb = f"{row['pb']:.2f}" if pd.notna(row['pb']) else "-"
        ps = f"{row['ps']:.2f}" if pd.notna(row['ps']) else "-"
        ps_ttm = f"{row['ps_ttm']:.2f}" if pd.notna(row['ps_ttm']) else "-"
        dv_ratio = f"{row['dv_ratio']:.2f}" if pd.notna(row['dv_ratio']) else "-"
        total_mv = format_large_number(row['total_mv']) if pd.notna(row['total_mv']) else "-"
        circ_mv = format_large_number(row['circ_mv']) if pd.notna(row['circ_mv']) else "-"
        
        result.append(f"{trade_date:<12} {close:<10} {turnover_rate:<12} {turnover_rate_f:<15} {volume_ratio:<10} {pe:<10} {pe_ttm:<12} {pb:<10} {ps:<10} {ps_ttm:<12} {dv_ratio:<12} {total_mv:<15} {circ_mv:<15}")
    
    # 如果有更多数据，显示统计信息
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 160)
        result.append(f"交易日期: {format_date(latest['trade_date'])}")
        result.append(f"收盘价: {latest['close']:.2f}" if pd.notna(latest['close']) else "收盘价: -")
        result.append("")
        result.append("估值指标：")
        result.append(f"  市盈率(PE): {latest['pe']:.2f}" if pd.notna(latest['pe']) else "  市盈率(PE): -")
        result.append(f"  市盈率(TTM): {latest['pe_ttm']:.2f}" if pd.notna(latest['pe_ttm']) else "  市盈率(TTM): -")
        result.append(f"  市净率(PB): {latest['pb']:.2f}" if pd.notna(latest['pb']) else "  市净率(PB): -")
        result.append(f"  市销率(PS): {latest['ps']:.2f}" if pd.notna(latest['ps']) else "  市销率(PS): -")
        result.append(f"  市销率(TTM): {latest['ps_ttm']:.2f}" if pd.notna(latest['ps_ttm']) else "  市销率(TTM): -")
        result.append(f"  股息率: {latest['dv_ratio']:.2f}%" if pd.notna(latest['dv_ratio']) else "  股息率: -")
        result.append("")
        result.append("交易指标：")
        result.append(f"  换手率: {latest['turnover_rate']:.2f}%" if pd.notna(latest['turnover_rate']) else "  换手率: -")
        result.append(f"  换手率(自由流通): {latest['turnover_rate_f']:.2f}%" if pd.notna(latest['turnover_rate_f']) else "  换手率(自由流通): -")
        result.append(f"  量比: {latest['volume_ratio']:.2f}" if pd.notna(latest['volume_ratio']) else "  量比: -")
        result.append("")
        result.append("股本与市值：")
        result.append(f"  总股本: {format_large_number(latest['total_share'])} 万股" if pd.notna(latest['total_share']) else "  总股本: -")
        result.append(f"  流通股本: {format_large_number(latest['float_share'])} 万股" if pd.notna(latest['float_share']) else "  流通股本: -")
        result.append(f"  自由流通股本: {format_large_number(latest['free_share'])} 万股" if pd.notna(latest['free_share']) else "  自由流通股本: -")
        result.append(f"  总市值: {format_large_number(latest['total_mv'])} 万元" if pd.notna(latest['total_mv']) else "  总市值: -")
        result.append(f"  流通市值: {format_large_number(latest['circ_mv'])} 万元" if pd.notna(latest['circ_mv']) else "  流通市值: -")
    
    return "\n".join(result)


def format_large_number(num: float) -> str:
    """
    格式化大数字（添加千分位分隔符）
    
    参数:
        num: 数字
    
    返回:
        格式化后的字符串
    """
    if pd.isna(num):
        return "-"
    
    # 转换为整数（去掉小数部分）
    num_int = int(num)
    
    # 添加千分位分隔符
    return f"{num_int:,}"


def format_top_list_data(df: pd.DataFrame, trade_date: str, ts_code: str = "") -> str:
    """
    格式化龙虎榜每日交易明细数据输出
    
    参数:
        df: 龙虎榜数据DataFrame
        trade_date: 交易日期
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的龙虎榜数据"
    
    result = []
    result.append("🐉 龙虎榜每日交易明细")
    result.append("=" * 180)
    result.append("")
    result.append(f"📅 交易日期: {format_date(trade_date)}")
    if ts_code:
        result.append(f"📊 股票代码: {ts_code}")
    result.append("")
    
    # 如果查询的是单个股票
    if ts_code and 'ts_code' in df.columns:
        stock_df = df[df['ts_code'] == ts_code]
        if not stock_df.empty:
            result.append(format_single_stock_top_list(stock_df, ts_code))
            return "\n".join(result)
    
    # 按股票代码分组显示
    if 'ts_code' in df.columns:
        # 按净买入额排序（降序）
        if 'net_amount' in df.columns:
            df = df.sort_values('net_amount', ascending=False, na_position='last')
        
        result.append(f"共找到 {len(df)} 条龙虎榜记录，涉及 {len(df['ts_code'].unique())} 只股票")
        result.append("")
        result.append(f"{'股票代码':<15} {'股票名称':<15} {'收盘价':<10} {'涨跌幅':<10} {'换手率':<10} {'总成交额(元)':<18} {'龙虎榜买入(元)':<18} {'龙虎榜卖出(元)':<18} {'净买入(元)':<18} {'净买占比':<12} {'上榜理由':<30}")
        result.append("-" * 180)
        
        display_count = min(50, len(df))
        for _, row in df.head(display_count).iterrows():
            code = str(row.get('ts_code', '-'))[:13]
            name = str(row.get('name', '-'))[:13]
            close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
            pct_change = f"{row.get('pct_change', 0):+.2f}%" if pd.notna(row.get('pct_change')) else "-"
            turnover_rate = f"{row.get('turnover_rate', 0):.2f}%" if pd.notna(row.get('turnover_rate')) else "-"
            amount = format_large_number(row.get('amount', 0)) if pd.notna(row.get('amount')) else "-"
            l_buy = format_large_number(row.get('l_buy', 0)) if pd.notna(row.get('l_buy')) else "-"
            l_sell = format_large_number(row.get('l_sell', 0)) if pd.notna(row.get('l_sell')) else "-"
            net_amount = format_large_number(row.get('net_amount', 0)) if pd.notna(row.get('net_amount')) else "-"
            net_rate = f"{row.get('net_rate', 0):+.2f}%" if pd.notna(row.get('net_rate')) else "-"
            reason = str(row.get('reason', '-'))[:28]
            
            result.append(f"{code:<15} {name:<15} {close:<10} {pct_change:<10} {turnover_rate:<10} {amount:<18} {l_buy:<18} {l_sell:<18} {net_amount:<18} {net_rate:<12} {reason:<30}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    else:
        # 如果没有ts_code字段，直接显示所有记录
        result.append(f"共找到 {len(df)} 条龙虎榜记录")
        result.append("")
        result.append(f"{'收盘价':<10} {'涨跌幅':<10} {'换手率':<10} {'总成交额(元)':<18} {'龙虎榜买入(元)':<18} {'龙虎榜卖出(元)':<18} {'净买入(元)':<18} {'净买占比':<12} {'上榜理由':<30}")
        result.append("-" * 180)
        
        display_count = min(50, len(df))
        for _, row in df.head(display_count).iterrows():
            close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
            pct_change = f"{row.get('pct_change', 0):+.2f}%" if pd.notna(row.get('pct_change')) else "-"
            turnover_rate = f"{row.get('turnover_rate', 0):.2f}%" if pd.notna(row.get('turnover_rate')) else "-"
            amount = format_large_number(row.get('amount', 0)) if pd.notna(row.get('amount')) else "-"
            l_buy = format_large_number(row.get('l_buy', 0)) if pd.notna(row.get('l_buy')) else "-"
            l_sell = format_large_number(row.get('l_sell', 0)) if pd.notna(row.get('l_sell')) else "-"
            net_amount = format_large_number(row.get('net_amount', 0)) if pd.notna(row.get('net_amount')) else "-"
            net_rate = f"{row.get('net_rate', 0):+.2f}%" if pd.notna(row.get('net_rate')) else "-"
            reason = str(row.get('reason', '-'))[:28]
            
            result.append(f"{close:<10} {pct_change:<10} {turnover_rate:<10} {amount:<18} {l_buy:<18} {l_sell:<18} {net_amount:<18} {net_rate:<12} {reason:<30}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        result.append(f"上榜股票数量: {len(df['ts_code'].unique()) if 'ts_code' in df.columns else len(df)}")
        
        if 'net_amount' in df.columns:
            total_net = df['net_amount'].sum()
            result.append(f"总净买入额: {format_large_number(total_net)} 元")
        
        if 'amount' in df.columns:
            total_amount = df['amount'].sum()
            result.append(f"总成交额: {format_large_number(total_amount)} 元")
        
        if 'l_amount' in df.columns:
            total_l_amount = df['l_amount'].sum()
            result.append(f"龙虎榜总成交额: {format_large_number(total_l_amount)} 元")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare top_list接口")
    result.append("  - 数据历史：2005年至今")
    result.append("  - 龙虎榜净买入额 = 龙虎榜买入额 - 龙虎榜卖出额")
    result.append("  - 净买占比 = 龙虎榜净买入额 / 总成交额")
    result.append("  - 上榜理由包括：日涨幅/跌幅偏离值达到7%、日换手率达到20%、连续三个交易日内涨幅偏离值累计达到20%等")
    result.append("  - 权限要求：2000积分")
    result.append("  - 限量：单次请求返回最大10000行数据")
    
    return "\n".join(result)


def format_single_stock_top_list(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的龙虎榜数据
    
    参数:
        df: 单个股票的龙虎榜数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的龙虎榜数据"
    
    result = []
    result.append(f"🐉 {ts_code} 龙虎榜数据")
    result.append("=" * 180)
    result.append("")
    
    # 显示所有记录
    result.append(f"共找到 {len(df)} 条记录")
    result.append("")
    result.append(f"{'交易日期':<12} {'股票名称':<15} {'收盘价':<10} {'涨跌幅':<10} {'换手率':<10} {'总成交额(元)':<18} {'龙虎榜买入(元)':<18} {'龙虎榜卖出(元)':<18} {'净买入(元)':<18} {'净买占比':<12} {'成交占比':<12} {'上榜理由':<30}")
    result.append("-" * 180)
    
    for _, row in df.iterrows():
        trade_date = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
        name = str(row.get('name', '-'))[:13]
        close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
        pct_change = f"{row.get('pct_change', 0):+.2f}%" if pd.notna(row.get('pct_change')) else "-"
        turnover_rate = f"{row.get('turnover_rate', 0):.2f}%" if pd.notna(row.get('turnover_rate')) else "-"
        amount = format_large_number(row.get('amount', 0)) if pd.notna(row.get('amount')) else "-"
        l_buy = format_large_number(row.get('l_buy', 0)) if pd.notna(row.get('l_buy')) else "-"
        l_sell = format_large_number(row.get('l_sell', 0)) if pd.notna(row.get('l_sell')) else "-"
        net_amount = format_large_number(row.get('net_amount', 0)) if pd.notna(row.get('net_amount')) else "-"
        net_rate = f"{row.get('net_rate', 0):+.2f}%" if pd.notna(row.get('net_rate')) else "-"
        amount_rate = f"{row.get('amount_rate', 0):.2f}%" if pd.notna(row.get('amount_rate')) else "-"
        reason = str(row.get('reason', '-'))[:28]
        
        result.append(f"{trade_date:<12} {name:<15} {close:<10} {pct_change:<10} {turnover_rate:<10} {amount:<18} {l_buy:<18} {l_sell:<18} {net_amount:<18} {net_rate:<12} {amount_rate:<12} {reason:<30}")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 180)
        result.append(f"交易日期: {format_date(str(latest.get('trade_date', '-')))}")
        result.append(f"股票代码: {ts_code}")
        result.append(f"股票名称: {latest.get('name', '-')}")
        result.append(f"收盘价: {latest.get('close', 0):.2f}" if pd.notna(latest.get('close')) else "收盘价: -")
        result.append(f"涨跌幅: {latest.get('pct_change', 0):+.2f}%" if pd.notna(latest.get('pct_change')) else "涨跌幅: -")
        result.append(f"换手率: {latest.get('turnover_rate', 0):.2f}%" if pd.notna(latest.get('turnover_rate')) else "换手率: -")
        result.append(f"总成交额: {format_large_number(latest.get('amount', 0))} 元" if pd.notna(latest.get('amount')) else "总成交额: -")
        result.append(f"龙虎榜买入额: {format_large_number(latest.get('l_buy', 0))} 元" if pd.notna(latest.get('l_buy')) else "龙虎榜买入额: -")
        result.append(f"龙虎榜卖出额: {format_large_number(latest.get('l_sell', 0))} 元" if pd.notna(latest.get('l_sell')) else "龙虎榜卖出额: -")
        result.append(f"龙虎榜净买入额: {format_large_number(latest.get('net_amount', 0))} 元" if pd.notna(latest.get('net_amount')) else "龙虎榜净买入额: -")
        result.append(f"净买占比: {latest.get('net_rate', 0):+.2f}%" if pd.notna(latest.get('net_rate')) else "净买占比: -")
        result.append(f"成交占比: {latest.get('amount_rate', 0):.2f}%" if pd.notna(latest.get('amount_rate')) else "成交占比: -")
        result.append(f"上榜理由: {latest.get('reason', '-')}")
    
    return "\n".join(result)


def format_top_inst_data(df: pd.DataFrame, trade_date: str, ts_code: str = "") -> str:
    """
    格式化龙虎榜机构成交明细数据输出
    
    参数:
        df: 龙虎榜机构明细数据DataFrame
        trade_date: 交易日期
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的龙虎榜机构明细数据"
    
    result = []
    result.append("🏢 龙虎榜机构成交明细")
    result.append("=" * 180)
    result.append("")
    result.append(f"📅 交易日期: {format_date(trade_date)}")
    if ts_code:
        result.append(f"📊 股票代码: {ts_code}")
    result.append("")
    
    # 如果查询的是单个股票
    if ts_code and 'ts_code' in df.columns:
        stock_df = df[df['ts_code'] == ts_code]
        if not stock_df.empty:
            result.append(format_single_stock_top_inst(stock_df, ts_code))
            return "\n".join(result)
    
    # 按股票代码和买卖类型分组显示
    if 'ts_code' in df.columns:
        # 按股票代码分组
        codes = sorted(df['ts_code'].unique())
        result.append(f"共找到 {len(df)} 条机构明细记录，涉及 {len(codes)} 只股票")
        result.append("")
        
        for code in codes[:20]:  # 最多显示前20只股票
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                # 获取股票名称（如果有）
                stock_name = ""
                if 'name' in code_df.columns and not code_df['name'].isna().all():
                    stock_name = code_df['name'].iloc[0]
                
                result.append(f"📈 {code} {stock_name} ({len(code_df)} 条记录)")
                result.append("-" * 180)
                
                # 分别显示买入和卖出
                buy_df = code_df[code_df['side'] == 0] if 'side' in code_df.columns else pd.DataFrame()
                sell_df = code_df[code_df['side'] == 1] if 'side' in code_df.columns else pd.DataFrame()
                
                if not buy_df.empty:
                    result.append("💰 买入金额最大的前5名：")
                    result.append(f"{'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
                    result.append("-" * 180)
                    
                    # 按买入额排序（降序）
                    if 'buy' in buy_df.columns:
                        buy_df = buy_df.sort_values('buy', ascending=False, na_position='last')
                    
                    for _, row in buy_df.head(10).iterrows():  # 最多显示10条
                        exalter = str(row.get('exalter', '-'))[:28]
                        buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
                        buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
                        sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
                        sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
                        net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
                        reason = str(row.get('reason', '-'))[:28]
                        
                        result.append(f"{exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
                    result.append("")
                
                if not sell_df.empty:
                    result.append("💸 卖出金额最大的前5名：")
                    result.append(f"{'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
                    result.append("-" * 180)
                    
                    # 按卖出额排序（降序）
                    if 'sell' in sell_df.columns:
                        sell_df = sell_df.sort_values('sell', ascending=False, na_position='last')
                    
                    for _, row in sell_df.head(10).iterrows():  # 最多显示10条
                        exalter = str(row.get('exalter', '-'))[:28]
                        buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
                        buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
                        sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
                        sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
                        net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
                        reason = str(row.get('reason', '-'))[:28]
                        
                        result.append(f"{exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
                    result.append("")
                
                result.append("")
        
        if len(codes) > 20:
            result.append(f"  ... 还有 {len(codes) - 20} 只股票未显示")
    else:
        # 如果没有ts_code字段，按买卖类型分组显示
        if 'side' in df.columns:
            result.append(f"共找到 {len(df)} 条机构明细记录")
            result.append("")
            
            # 买入记录
            buy_df = df[df['side'] == 0]
            if not buy_df.empty:
                result.append("💰 买入金额最大的前5名：")
                result.append(f"{'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
                result.append("-" * 180)
                
                # 按买入额排序（降序）
                if 'buy' in buy_df.columns:
                    buy_df = buy_df.sort_values('buy', ascending=False, na_position='last')
                
                for _, row in buy_df.head(50).iterrows():
                    exalter = str(row.get('exalter', '-'))[:28]
                    buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
                    buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
                    sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
                    sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
                    net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
                    reason = str(row.get('reason', '-'))[:28]
                    
                    result.append(f"{exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
                result.append("")
            
            # 卖出记录
            sell_df = df[df['side'] == 1]
            if not sell_df.empty:
                result.append("💸 卖出金额最大的前5名：")
                result.append(f"{'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
                result.append("-" * 180)
                
                # 按卖出额排序（降序）
                if 'sell' in sell_df.columns:
                    sell_df = sell_df.sort_values('sell', ascending=False, na_position='last')
                
                for _, row in sell_df.head(50).iterrows():
                    exalter = str(row.get('exalter', '-'))[:28]
                    buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
                    buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
                    sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
                    sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
                    net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
                    reason = str(row.get('reason', '-'))[:28]
                    
                    result.append(f"{exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
                result.append("")
        else:
            # 没有side字段，直接显示所有记录
            result.append(f"共找到 {len(df)} 条机构明细记录")
            result.append("")
            result.append(f"{'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
            result.append("-" * 180)
            
            display_count = min(100, len(df))
            for _, row in df.head(display_count).iterrows():
                exalter = str(row.get('exalter', '-'))[:28]
                buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
                buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
                sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
                sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
                net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
                reason = str(row.get('reason', '-'))[:28]
                
                result.append(f"{exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
            
            if len(df) > display_count:
                result.append("")
                result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'ts_code' in df.columns:
            result.append(f"上榜股票数量: {len(df['ts_code'].unique())}")
        
        if 'exalter' in df.columns:
            result.append(f"涉及营业部数量: {len(df['exalter'].unique())}")
        
        if 'buy' in df.columns:
            total_buy = df['buy'].sum()
            result.append(f"总买入额: {format_large_number(total_buy)} 元")
        
        if 'sell' in df.columns:
            total_sell = df['sell'].sum()
            result.append(f"总卖出额: {format_large_number(total_sell)} 元")
        
        if 'net_buy' in df.columns:
            total_net = df['net_buy'].sum()
            result.append(f"总净成交额: {format_large_number(total_net)} 元")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare top_inst接口")
    result.append("  - 买卖类型：0=买入金额最大的前5名，1=卖出金额最大的前5名")
    result.append("  - 净成交额 = 买入额 - 卖出额")
    result.append("  - 买入占比 = 买入额 / 总成交额")
    result.append("  - 卖出占比 = 卖出额 / 总成交额")
    result.append("  - 上榜理由包括：涨幅偏离值达7%、连续三个交易日内涨幅偏离值累计达20%等")
    result.append("  - 权限要求：5000积分")
    result.append("  - 限量：单次请求最大返回10000行数据")
    
    return "\n".join(result)


def format_single_stock_top_inst(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化单个股票的龙虎榜机构明细数据
    
    参数:
        df: 单个股票的龙虎榜机构明细数据DataFrame
        ts_code: 股票代码
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的龙虎榜机构明细数据"
    
    result = []
    result.append(f"🏢 {ts_code} 龙虎榜机构明细")
    result.append("=" * 180)
    result.append("")
    
    # 获取股票名称（如果有）
    stock_name = ""
    if 'name' in df.columns and not df['name'].isna().all():
        stock_name = df['name'].iloc[0]
        result.append(f"股票名称: {stock_name}")
    
    result.append(f"共找到 {len(df)} 条记录")
    result.append("")
    
    # 分别显示买入和卖出
    buy_df = df[df['side'] == 0] if 'side' in df.columns else pd.DataFrame()
    sell_df = df[df['side'] == 1] if 'side' in df.columns else pd.DataFrame()
    
    if not buy_df.empty:
        result.append("💰 买入金额最大的前5名：")
        result.append(f"{'交易日期':<12} {'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
        result.append("-" * 180)
        
        # 按买入额排序（降序）
        if 'buy' in buy_df.columns:
            buy_df = buy_df.sort_values('buy', ascending=False, na_position='last')
        
        for _, row in buy_df.iterrows():
            trade_date = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
            exalter = str(row.get('exalter', '-'))[:28]
            buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
            buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
            sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
            sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
            net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
            reason = str(row.get('reason', '-'))[:28]
            
            result.append(f"{trade_date:<12} {exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
        result.append("")
    
    if not sell_df.empty:
        result.append("💸 卖出金额最大的前5名：")
        result.append(f"{'交易日期':<12} {'营业部名称':<30} {'买入额(元)':<18} {'买入占比':<12} {'卖出额(元)':<18} {'卖出占比':<12} {'净成交额(元)':<18} {'上榜理由':<30}")
        result.append("-" * 180)
        
        # 按卖出额排序（降序）
        if 'sell' in sell_df.columns:
            sell_df = sell_df.sort_values('sell', ascending=False, na_position='last')
        
        for _, row in sell_df.iterrows():
            trade_date = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
            exalter = str(row.get('exalter', '-'))[:28]
            buy = format_large_number(row.get('buy', 0)) if pd.notna(row.get('buy')) else "-"
            buy_rate = f"{row.get('buy_rate', 0):.2f}%" if pd.notna(row.get('buy_rate')) else "-"
            sell = format_large_number(row.get('sell', 0)) if pd.notna(row.get('sell')) else "-"
            sell_rate = f"{row.get('sell_rate', 0):.2f}%" if pd.notna(row.get('sell_rate')) else "-"
            net_buy = format_large_number(row.get('net_buy', 0)) if pd.notna(row.get('net_buy')) else "-"
            reason = str(row.get('reason', '-'))[:28]
            
            result.append(f"{trade_date:<12} {exalter:<30} {buy:<18} {buy_rate:<12} {sell:<18} {sell_rate:<12} {net_buy:<18} {reason:<30}")
        result.append("")
    
    # 显示统计信息
    if not df.empty:
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'exalter' in df.columns:
            result.append(f"涉及营业部数量: {len(df['exalter'].unique())}")
        
        if 'buy' in df.columns:
            total_buy = df['buy'].sum()
            result.append(f"总买入额: {format_large_number(total_buy)} 元")
        
        if 'sell' in df.columns:
            total_sell = df['sell'].sum()
            result.append(f"总卖出额: {format_large_number(total_sell)} 元")
        
        if 'net_buy' in df.columns:
            total_net = df['net_buy'].sum()
            result.append(f"总净成交额: {format_large_number(total_net)} 元")
    
    return "\n".join(result)


def format_stock_min_data(df: pd.DataFrame, ts_code: str = "", freq: str = "1MIN", date_str: str = "") -> str:
    """
    格式化A股实时分钟行情数据输出
    
    参数:
        df: A股分钟行情数据DataFrame
        ts_code: 股票代码（用于显示）
        freq: 分钟频度（用于显示）
        date_str: 回放日期（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的A股分钟行情数据"
    
    result = []
    result.append("📈 A股实时分钟行情数据")
    result.append("=" * 180)
    result.append("")
    
    # 按时间排序（最新的在前）
    if 'time' in df.columns:
        df = df.sort_values('time', ascending=False)
    
    # 如果有多个股票，按股票代码分组显示
    if 'ts_code' in df.columns and len(df['ts_code'].unique()) > 1:
        codes = sorted(df['ts_code'].unique())
        result.append(f"共找到 {len(df)} 条记录，涉及 {len(codes)} 只股票")
        result.append(f"分钟频度: {freq}")
        if date_str:
            result.append(f"回放日期: {date_str}")
        result.append("")
        
        # 按股票代码分组显示
        for code in codes:
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                result.append(f"📊 {code} 分钟行情数据")
                result.append("-" * 180)
                result.append(f"{'交易时间':<20} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'成交量(股)':<18} {'成交额(元)':<18}")
                result.append("-" * 180)
                
                # 按时间排序（最新的在前）
                if 'time' in code_df.columns:
                    code_df = code_df.sort_values('time', ascending=False)
                
                display_count = min(100, len(code_df))
                for _, row in code_df.head(display_count).iterrows():
                    time_str = str(row.get('time', '-'))[:18]
                    open_price = f"{row.get('open', 0):.2f}" if pd.notna(row.get('open')) else "-"
                    high = f"{row.get('high', 0):.2f}" if pd.notna(row.get('high')) else "-"
                    low = f"{row.get('low', 0):.2f}" if pd.notna(row.get('low')) else "-"
                    close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
                    vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                    amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                    
                    result.append(f"{time_str:<20} {open_price:<12} {high:<12} {low:<12} {close:<12} {vol:<18} {amount:<18}")
                
                if len(code_df) > display_count:
                    result.append(f"  ... 还有 {len(code_df) - display_count} 条记录未显示")
                result.append("")
        
        if len(codes) > 10:
            result.append(f"  ... 还有 {len(codes) - 10} 只股票未显示")
    else:
        # 单个股票或没有ts_code字段，直接显示所有记录
        if 'ts_code' in df.columns and not df.empty:
            code = df.iloc[0].get('ts_code', ts_code or '-')
            result.append(f"📊 {code} 分钟行情数据")
        else:
            result.append(f"共找到 {len(df)} 条记录")
        result.append("")
        result.append(f"分钟频度: {freq}")
        if date_str:
            result.append(f"回放日期: {date_str}")
        result.append("")
        result.append(f"{'交易时间':<20} {'开盘':<12} {'最高':<12} {'最低':<12} {'收盘':<12} {'成交量(股)':<18} {'成交额(元)':<18}")
        result.append("-" * 180)
        
        display_count = min(200, len(df))
        for _, row in df.head(display_count).iterrows():
            time_str = str(row.get('time', '-'))[:18]
            open_price = f"{row.get('open', 0):.2f}" if pd.notna(row.get('open')) else "-"
            high = f"{row.get('high', 0):.2f}" if pd.notna(row.get('high')) else "-"
            low = f"{row.get('low', 0):.2f}" if pd.notna(row.get('low')) else "-"
            close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
            vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
            amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
            
            result.append(f"{time_str:<20} {open_price:<12} {high:<12} {low:<12} {close:<12} {vol:<18} {amount:<18}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示最近 {display_count} 条）")
    
    # 显示最新数据摘要
    if not df.empty:
        latest = df.iloc[0]
        result.append("")
        result.append("📊 最新数据摘要：")
        result.append("-" * 180)
        if 'ts_code' in latest:
            result.append(f"股票代码: {latest.get('ts_code', '-')}")
        if 'time' in latest:
            result.append(f"交易时间: {latest.get('time', '-')}")
        if 'open' in latest and pd.notna(latest.get('open')):
            result.append(f"开盘: {latest.get('open', 0):.2f}")
        if 'high' in latest and pd.notna(latest.get('high')):
            result.append(f"最高: {latest.get('high', 0):.2f}")
        if 'low' in latest and pd.notna(latest.get('low')):
            result.append(f"最低: {latest.get('low', 0):.2f}")
        if 'close' in latest and pd.notna(latest.get('close')):
            result.append(f"收盘: {latest.get('close', 0):.2f}")
        if 'vol' in latest and pd.notna(latest.get('vol')):
            result.append(f"成交量: {latest.get('vol', 0):,.0f} 股")
        if 'amount' in latest and pd.notna(latest.get('amount')):
            result.append(f"成交额: {latest.get('amount', 0):,.2f} 元")
    
    result.append("")
    result.append("📝 说明：")
    if date_str:
        result.append("  - 数据来源：Tushare rt_min_daily接口（历史回放）")
        result.append("  - 提供当日开盘以来的所有历史分钟数据")
    else:
        result.append("  - 数据来源：Tushare rt_min接口（实时）")
        result.append("  - 获取全A股票实时分钟数据")
    result.append("  - 支持1min/5min/15min/30min/60min行情")
    result.append("  - 权限要求：正式权限请参阅权限说明")
    result.append("  - 限量：单次最大1000行数据，支持多个股票同时提取")
    result.append("  - 注意：rt_min_daily接口仅支持单个股票提取，不能同时提取多个")
    
    return "\n".join(result)


def format_stock_rt_k_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化沪深京实时日线行情数据输出
    
    参数:
        df: 实时日线行情数据DataFrame
        ts_code: 股票代码或通配符（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的沪深京实时日线行情数据"
    
    result = []
    result.append("📈 沪深京实时日线行情数据")
    result.append("=" * 180)
    result.append("")
    
    # 按成交量排序（降序），显示最活跃的股票
    if 'vol' in df.columns:
        df = df.sort_values('vol', ascending=False, na_position='last')
    
    result.append(f"共找到 {len(df)} 条记录")
    if ts_code:
        result.append(f"查询条件: {ts_code}")
    result.append("")
    
    # 显示主要行情数据
    result.append(f"{'股票代码':<12} {'股票名称':<20} {'昨收':<10} {'开盘':<10} {'最高':<10} {'最低':<10} {'最新价':<10} {'成交量(股)':<18} {'成交额(元)':<18} {'成交笔数':<12}")
    result.append("-" * 180)
    
    display_count = min(200, len(df))
    for _, row in df.head(display_count).iterrows():
        code = str(row.get('ts_code', '-'))[:10]
        name = str(row.get('name', '-'))[:18]
        pre_close = f"{row.get('pre_close', 0):.2f}" if pd.notna(row.get('pre_close')) else "-"
        open_price = f"{row.get('open', 0):.2f}" if pd.notna(row.get('open')) else "-"
        high = f"{row.get('high', 0):.2f}" if pd.notna(row.get('high')) else "-"
        low = f"{row.get('low', 0):.2f}" if pd.notna(row.get('low')) else "-"
        close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
        vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
        amount = f"{row.get('amount', 0):,.0f}" if pd.notna(row.get('amount')) else "-"
        num = f"{row.get('num', 0):,.0f}" if pd.notna(row.get('num')) else "-"
        
        result.append(f"{code:<12} {name:<20} {pre_close:<10} {open_price:<10} {high:<10} {low:<10} {close:<10} {vol:<18} {amount:<18} {num:<12}")
    
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条，按成交量降序排列）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'vol' in df.columns:
            total_vol = df['vol'].sum()
            result.append(f"总成交量: {total_vol:,.0f} 股")
        
        if 'amount' in df.columns:
            total_amount = df['amount'].sum()
            result.append(f"总成交额: {total_amount:,.0f} 元")
        
        if 'num' in df.columns:
            total_num = df['num'].sum()
            result.append(f"总成交笔数: {total_num:,.0f} 笔")
        
        # 计算涨跌情况
        if 'close' in df.columns and 'pre_close' in df.columns:
            up_count = 0
            down_count = 0
            flat_count = 0
            for _, row in df.iterrows():
                if pd.notna(row.get('close')) and pd.notna(row.get('pre_close')):
                    if row.get('close') > row.get('pre_close'):
                        up_count += 1
                    elif row.get('close') < row.get('pre_close'):
                        down_count += 1
                    else:
                        flat_count += 1
            
            if up_count + down_count + flat_count > 0:
                result.append(f"上涨: {up_count} 只，下跌: {down_count} 只，平盘: {flat_count} 只")
    
    # 显示最新数据摘要（前5只最活跃的股票）
    if not df.empty and len(df) > 0:
        result.append("")
        result.append("📊 最活跃股票详情（前5只）：")
        result.append("-" * 180)
        
        for idx, (_, row) in enumerate(df.head(5).iterrows()):
            code = str(row.get('ts_code', '-'))
            name = str(row.get('name', '-'))
            result.append(f"\n{idx + 1}. {code} ({name})")
            
            if pd.notna(row.get('pre_close')):
                result.append(f"   昨收: {row.get('pre_close', 0):.2f}")
            if pd.notna(row.get('open')):
                result.append(f"   开盘: {row.get('open', 0):.2f}")
            if pd.notna(row.get('high')):
                result.append(f"   最高: {row.get('high', 0):.2f}")
            if pd.notna(row.get('low')):
                result.append(f"   最低: {row.get('low', 0):.2f}")
            if pd.notna(row.get('close')):
                result.append(f"   最新价: {row.get('close', 0):.2f}")
                if pd.notna(row.get('pre_close')):
                    change = row.get('close', 0) - row.get('pre_close', 0)
                    change_pct = (change / row.get('pre_close', 1)) * 100 if row.get('pre_close', 0) != 0 else 0
                    result.append(f"   涨跌: {change:+.2f} ({change_pct:+.2f}%)")
            if pd.notna(row.get('vol')):
                result.append(f"   成交量: {row.get('vol', 0):,.0f} 股")
            if pd.notna(row.get('amount')):
                result.append(f"   成交额: {row.get('amount', 0):,.0f} 元")
            if pd.notna(row.get('num')):
                result.append(f"   成交笔数: {row.get('num', 0):,.0f} 笔")
            
            # 显示委托买卖盘信息
            if pd.notna(row.get('ask_price1')) and pd.notna(row.get('ask_volume1')):
                result.append(f"   卖一: {row.get('ask_price1', 0):.2f} ({row.get('ask_volume1', 0):,.0f} 股)")
            if pd.notna(row.get('bid_price1')) and pd.notna(row.get('bid_volume1')):
                result.append(f"   买一: {row.get('bid_price1', 0):.2f} ({row.get('bid_volume1', 0):,.0f} 股)")
            
            if pd.notna(row.get('trade_time')):
                result.append(f"   交易时间: {row.get('trade_time', '-')}")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare rt_k接口")
    result.append("  - 获取实时日k线行情，支持按股票代码及股票代码通配符一次性提取")
    result.append("  - 支持通配符方式：6*.SH（所有6开头的沪市股票）、301*.SZ（所有301开头的深市股票）等")
    result.append("  - 权限要求：本接口是单独开权限的数据，单独申请权限请参考权限列表")
    result.append("  - 限量：单次最大可提取6000条数据，等同于一次提取全市场")
    result.append("  - 注意：不建议一次提取全市场，可分批提取性能更好")
    
    return "\n".join(result)


def format_share_float_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化限售股解禁数据输出
    
    参数:
        df: 限售股解禁数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的限售股解禁数据"
    
    result = []
    result.append("🔓 限售股解禁数据")
    result.append("=" * 180)
    result.append("")
    
    # 按解禁日期排序（最新的在前）
    if 'float_date' in df.columns:
        df = df.sort_values('float_date', ascending=False)
    elif 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    
    # 如果有多个股票，按股票代码分组显示
    if 'ts_code' in df.columns and len(df['ts_code'].unique()) > 1:
        codes = sorted(df['ts_code'].unique())
        result.append(f"共找到 {len(df)} 条记录，涉及 {len(codes)} 只股票")
        result.append("")
        
        # 按股票代码分组显示
        for code in codes:
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                result.append(f"📊 {code} 限售股解禁数据")
                result.append("-" * 180)
                
                # 按解禁日期分组
                if 'float_date' in code_df.columns and len(code_df['float_date'].unique()) > 1:
                    dates = sorted(code_df['float_date'].unique(), reverse=True)
                    for date in dates[:10]:  # 最多显示最近10个解禁日期
                        date_df = code_df[code_df['float_date'] == date]
                        if not date_df.empty:
                            result.append(f"📅 解禁日期: {format_date(str(date))}")
                            result.append(f"{'公告日期':<12} {'股东名称':<25} {'流通股份(股)':<20} {'占总股本比率(%)':<18} {'股份类型':<20}")
                            result.append("-" * 180)
                            
                            for _, row in date_df.iterrows():
                                ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
                                holder_name = str(row.get('holder_name', '-'))[:23]
                                float_share = f"{row.get('float_share', 0):,.0f}" if pd.notna(row.get('float_share')) else "-"
                                float_ratio = f"{row.get('float_ratio', 0):.4f}%" if pd.notna(row.get('float_ratio')) else "-"
                                share_type = str(row.get('share_type', '-'))[:18]
                                
                                result.append(f"{ann_date:<12} {holder_name:<25} {float_share:<20} {float_ratio:<18} {share_type:<20}")
                            
                            result.append("")
                else:
                    # 单个解禁日期，直接显示所有记录
                    result.append(f"{'公告日期':<12} {'解禁日期':<12} {'股东名称':<25} {'流通股份(股)':<20} {'占总股本比率(%)':<18} {'股份类型':<20}")
                    result.append("-" * 180)
                    
                    for _, row in code_df.iterrows():
                        ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
                        float_date = format_date(str(row.get('float_date', '-'))) if pd.notna(row.get('float_date')) else "-"
                        holder_name = str(row.get('holder_name', '-'))[:23]
                        float_share = f"{row.get('float_share', 0):,.0f}" if pd.notna(row.get('float_share')) else "-"
                        float_ratio = f"{row.get('float_ratio', 0):.4f}%" if pd.notna(row.get('float_ratio')) else "-"
                        share_type = str(row.get('share_type', '-'))[:18]
                        
                        result.append(f"{ann_date:<12} {float_date:<12} {holder_name:<25} {float_share:<20} {float_ratio:<18} {share_type:<20}")
                    
                    result.append("")
        
        if len(codes) > 20:
            result.append(f"  ... 还有 {len(codes) - 20} 只股票未显示")
    else:
        # 单个股票或没有ts_code字段，直接显示所有记录
        if 'ts_code' in df.columns and not df.empty:
            code = df.iloc[0].get('ts_code', ts_code or '-')
            result.append(f"📊 {code} 限售股解禁数据")
        else:
            result.append(f"共找到 {len(df)} 条记录")
        result.append("")
        
        # 按解禁日期分组
        if 'float_date' in df.columns and len(df['float_date'].unique()) > 1:
            dates = sorted(df['float_date'].unique(), reverse=True)
            for date in dates[:20]:  # 最多显示最近20个解禁日期
                date_df = df[df['float_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 解禁日期: {format_date(str(date))}")
                    result.append(f"{'公告日期':<12} {'股东名称':<25} {'流通股份(股)':<20} {'占总股本比率(%)':<18} {'股份类型':<20}")
                    result.append("-" * 180)
                    
                    for _, row in date_df.iterrows():
                        ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
                        holder_name = str(row.get('holder_name', '-'))[:23]
                        float_share = f"{row.get('float_share', 0):,.0f}" if pd.notna(row.get('float_share')) else "-"
                        float_ratio = f"{row.get('float_ratio', 0):.4f}%" if pd.notna(row.get('float_ratio')) else "-"
                        share_type = str(row.get('share_type', '-'))[:18]
                        
                        result.append(f"{ann_date:<12} {holder_name:<25} {float_share:<20} {float_ratio:<18} {share_type:<20}")
                    
                    result.append("")
        else:
            # 单个解禁日期，直接显示所有记录
            result.append(f"{'公告日期':<12} {'解禁日期':<12} {'股东名称':<25} {'流通股份(股)':<20} {'占总股本比率(%)':<18} {'股份类型':<20}")
            result.append("-" * 180)
            
            display_count = min(200, len(df))
            for _, row in df.head(display_count).iterrows():
                ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
                float_date = format_date(str(row.get('float_date', '-'))) if pd.notna(row.get('float_date')) else "-"
                holder_name = str(row.get('holder_name', '-'))[:23]
                float_share = f"{row.get('float_share', 0):,.0f}" if pd.notna(row.get('float_share')) else "-"
                float_ratio = f"{row.get('float_ratio', 0):.4f}%" if pd.notna(row.get('float_ratio')) else "-"
                share_type = str(row.get('share_type', '-'))[:18]
                
                result.append(f"{ann_date:<12} {float_date:<12} {holder_name:<25} {float_share:<20} {float_ratio:<18} {share_type:<20}")
            
            if len(df) > display_count:
                result.append("")
                result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'ts_code' in df.columns:
            result.append(f"涉及股票数量: {len(df['ts_code'].unique())} 只")
        
        if 'float_date' in df.columns:
            result.append(f"涉及解禁日期: {len(df['float_date'].unique())} 个")
        
        if 'holder_name' in df.columns:
            result.append(f"涉及股东数量: {len(df['holder_name'].unique())} 个")
        
        if 'float_share' in df.columns:
            total_float_share = df['float_share'].sum()
            result.append(f"总解禁股份: {total_float_share:,.0f} 股")
        
        if 'float_ratio' in df.columns:
            avg_float_ratio = df['float_ratio'].mean()
            result.append(f"平均占总股本比率: {avg_float_ratio:.4f}%")
        
        # 按股份类型统计
        if 'share_type' in df.columns:
            share_types = df['share_type'].value_counts()
            result.append("")
            result.append("股份类型分布：")
            for share_type, count in share_types.items():
                result.append(f"  {share_type}: {count} 条")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare share_float接口")
    result.append("  - 显示限售股解禁数据，包括解禁日期、流通股份、股东名称、股份类型等信息")
    result.append("  - 权限要求：2000积分")
    
    return "\n".join(result)


def format_repurchase_data(df: pd.DataFrame, date_filter: str = "") -> str:
    """
    格式化股票回购数据输出
    
    参数:
        df: 股票回购数据DataFrame
        date_filter: 日期筛选条件（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的股票回购数据"
    
    result = []
    result.append("💰 股票回购数据")
    result.append("=" * 180)
    result.append("")
    
    # 按公告日期排序（最新的在前）
    if 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    
    result.append(f"共找到 {len(df)} 条记录")
    if date_filter:
        result.append(f"查询条件: {date_filter}")
    result.append("")
    
    # 如果有多个股票，按股票代码分组显示
    if 'ts_code' in df.columns and len(df['ts_code'].unique()) > 1:
        codes = sorted(df['ts_code'].unique())
        result.append(f"涉及 {len(codes)} 只股票")
        result.append("")
        
        # 按股票代码分组显示
        for code in codes[:50]:  # 最多显示前50只股票
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                result.append(f"📊 {code} 股票回购数据")
                result.append("-" * 180)
                result.append(f"{'公告日期':<12} {'截止日期':<12} {'进度':<15} {'过期日期':<12} {'回购数量(股)':<18} {'回购金额(元)':<18} {'最高价':<10} {'最低价':<10}")
                result.append("-" * 180)
                
                # 按公告日期排序（最新的在前）
                if 'ann_date' in code_df.columns:
                    code_df = code_df.sort_values('ann_date', ascending=False)
                
                for _, row in code_df.iterrows():
                    ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
                    end_date = format_date(str(row.get('end_date', '-'))) if pd.notna(row.get('end_date')) else "-"
                    proc = str(row.get('proc', '-'))[:13]
                    exp_date = format_date(str(row.get('exp_date', '-'))) if pd.notna(row.get('exp_date')) else "-"
                    vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
                    amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                    high_limit = f"{row.get('high_limit', 0):.2f}" if pd.notna(row.get('high_limit')) else "-"
                    low_limit = f"{row.get('low_limit', 0):.2f}" if pd.notna(row.get('low_limit')) else "-"
                    
                    result.append(f"{ann_date:<12} {end_date:<12} {proc:<15} {exp_date:<12} {vol:<18} {amount:<18} {high_limit:<10} {low_limit:<10}")
                
                result.append("")
        
        if len(codes) > 50:
            result.append(f"  ... 还有 {len(codes) - 50} 只股票未显示")
    else:
        # 单个股票或没有ts_code字段，直接显示所有记录
        if 'ts_code' in df.columns and not df.empty:
            code = df.iloc[0].get('ts_code', '-')
            result.append(f"📊 {code} 股票回购数据")
        else:
            result.append(f"共找到 {len(df)} 条记录")
        result.append("")
        result.append(f"{'股票代码':<12} {'公告日期':<12} {'截止日期':<12} {'进度':<15} {'过期日期':<12} {'回购数量(股)':<18} {'回购金额(元)':<18} {'最高价':<10} {'最低价':<10}")
        result.append("-" * 180)
        
        display_count = min(200, len(df))
        for _, row in df.head(display_count).iterrows():
            code = str(row.get('ts_code', '-'))[:10]
            ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
            end_date = format_date(str(row.get('end_date', '-'))) if pd.notna(row.get('end_date')) else "-"
            proc = str(row.get('proc', '-'))[:13]
            exp_date = format_date(str(row.get('exp_date', '-'))) if pd.notna(row.get('exp_date')) else "-"
            vol = f"{row.get('vol', 0):,.0f}" if pd.notna(row.get('vol')) else "-"
            amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
            high_limit = f"{row.get('high_limit', 0):.2f}" if pd.notna(row.get('high_limit')) else "-"
            low_limit = f"{row.get('low_limit', 0):.2f}" if pd.notna(row.get('low_limit')) else "-"
            
            result.append(f"{code:<12} {ann_date:<12} {end_date:<12} {proc:<15} {exp_date:<12} {vol:<18} {amount:<18} {high_limit:<10} {low_limit:<10}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'ts_code' in df.columns:
            result.append(f"涉及股票数量: {len(df['ts_code'].unique())} 只")
        
        if 'ann_date' in df.columns:
            result.append(f"涉及公告日期: {len(df['ann_date'].unique())} 个")
        
        # 按进度统计
        if 'proc' in df.columns:
            proc_counts = df['proc'].value_counts()
            result.append("")
            result.append("回购进度分布：")
            for proc, count in proc_counts.items():
                result.append(f"  {proc}: {count} 条")
        
        # 计算总回购数量和金额
        if 'vol' in df.columns:
            total_vol = df['vol'].sum()
            if total_vol > 0:
                result.append("")
                result.append(f"总回购数量: {total_vol:,.0f} 股")
        
        if 'amount' in df.columns:
            total_amount = df['amount'].sum()
            if total_amount > 0:
                result.append(f"总回购金额: {total_amount:,.2f} 元")
        
        # 计算平均回购价格
        if 'vol' in df.columns and 'amount' in df.columns:
            valid_data = df[(df['vol'] > 0) & (df['amount'] > 0)]
            if not valid_data.empty:
                avg_price = (valid_data['amount'].sum() / valid_data['vol'].sum())
                result.append(f"平均回购价格: {avg_price:.2f} 元/股")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare repurchase接口")
    result.append("  - 显示上市公司回购股票数据，包括公告日期、截止日期、进度、过期日期、回购数量、回购金额、回购价格区间等信息")
    result.append("  - 权限要求：600积分")
    result.append("  - 注意：如果都不填参数，单次默认返回2000条数据")
    
    return "\n".join(result)


def format_pledge_detail_data(df: pd.DataFrame, ts_code: str) -> str:
    """
    格式化股权质押明细数据输出
    
    参数:
        df: 股权质押明细数据DataFrame
        ts_code: 股票代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return f"未找到 {ts_code} 的股权质押明细数据"
    
    result = []
    result.append(f"🔒 {ts_code} 股权质押明细数据")
    result.append("=" * 180)
    result.append("")
    
    # 按公告日期排序（最新的在前）
    if 'ann_date' in df.columns:
        df = df.sort_values('ann_date', ascending=False)
    
    result.append(f"共找到 {len(df)} 条质押记录")
    result.append("")
    
    # 按股东名称分组显示
    if 'holder_name' in df.columns and len(df['holder_name'].unique()) > 1:
        holders = sorted(df['holder_name'].unique())
        result.append(f"涉及 {len(holders)} 个股东")
        result.append("")
        
        # 按股东名称分组显示
        for holder in holders[:30]:  # 最多显示前30个股东
            holder_df = df[df['holder_name'] == holder]
            if not holder_df.empty:
                result.append(f"👤 {holder}")
                result.append("-" * 180)
                result.append(f"{'公告日期':<12} {'质押开始日期':<12} {'质押结束日期':<12} {'质押数量(万股)':<18} {'是否已解押':<12} {'解押日期':<12} {'质押方':<25} {'持股总数(万股)':<18} {'质押总数(万股)':<18} {'本次质押占总股本比例(%)':<20} {'持股总数占总股本比例(%)':<20} {'是否回购':<10}")
                result.append("-" * 180)
                
                # 按公告日期排序（最新的在前）
                if 'ann_date' in holder_df.columns:
                    holder_df = holder_df.sort_values('ann_date', ascending=False)
                
                for _, row in holder_df.iterrows():
                    ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
                    start_date = format_date(str(row.get('start_date', '-'))) if pd.notna(row.get('start_date')) else "-"
                    end_date = format_date(str(row.get('end_date', '-'))) if pd.notna(row.get('end_date')) else "-"
                    pledge_amount = f"{row.get('pledge_amount', 0):,.2f}" if pd.notna(row.get('pledge_amount')) else "-"
                    is_release = str(row.get('is_release', '-'))[:10]
                    release_date = format_date(str(row.get('release_date', '-'))) if pd.notna(row.get('release_date')) else "-"
                    pledgor = str(row.get('pledgor', '-'))[:23]
                    holding_amount = f"{row.get('holding_amount', 0):,.2f}" if pd.notna(row.get('holding_amount')) else "-"
                    pledged_amount = f"{row.get('pledged_amount', 0):,.2f}" if pd.notna(row.get('pledged_amount')) else "-"
                    p_total_ratio = f"{row.get('p_total_ratio', 0):.4f}%" if pd.notna(row.get('p_total_ratio')) else "-"
                    h_total_ratio = f"{row.get('h_total_ratio', 0):.4f}%" if pd.notna(row.get('h_total_ratio')) else "-"
                    is_buyback = "是" if pd.notna(row.get('is_buyback')) and str(row.get('is_buyback')) == '1' else "否"
                    
                    result.append(f"{ann_date:<12} {start_date:<12} {end_date:<12} {pledge_amount:<18} {is_release:<12} {release_date:<12} {pledgor:<25} {holding_amount:<18} {pledged_amount:<18} {p_total_ratio:<20} {h_total_ratio:<20} {is_buyback:<10}")
                
                result.append("")
        
        if len(holders) > 30:
            result.append(f"  ... 还有 {len(holders) - 30} 个股东未显示")
    else:
        # 单个股东或没有holder_name字段，直接显示所有记录
        if 'holder_name' in df.columns and not df.empty:
            holder = df.iloc[0].get('holder_name', '-')
            result.append(f"👤 {holder} 的质押记录")
        else:
            result.append(f"共找到 {len(df)} 条质押记录")
        result.append("")
        result.append(f"{'公告日期':<12} {'股东名称':<25} {'质押开始日期':<12} {'质押结束日期':<12} {'质押数量(万股)':<18} {'是否已解押':<12} {'解押日期':<12} {'质押方':<25} {'持股总数(万股)':<18} {'质押总数(万股)':<18} {'本次质押占总股本比例(%)':<20} {'持股总数占总股本比例(%)':<20} {'是否回购':<10}")
        result.append("-" * 180)
        
        display_count = min(200, len(df))
        for _, row in df.head(display_count).iterrows():
            ann_date = format_date(str(row.get('ann_date', '-'))) if pd.notna(row.get('ann_date')) else "-"
            holder_name = str(row.get('holder_name', '-'))[:23]
            start_date = format_date(str(row.get('start_date', '-'))) if pd.notna(row.get('start_date')) else "-"
            end_date = format_date(str(row.get('end_date', '-'))) if pd.notna(row.get('end_date')) else "-"
            pledge_amount = f"{row.get('pledge_amount', 0):,.2f}" if pd.notna(row.get('pledge_amount')) else "-"
            is_release = str(row.get('is_release', '-'))[:10]
            release_date = format_date(str(row.get('release_date', '-'))) if pd.notna(row.get('release_date')) else "-"
            pledgor = str(row.get('pledgor', '-'))[:23]
            holding_amount = f"{row.get('holding_amount', 0):,.2f}" if pd.notna(row.get('holding_amount')) else "-"
            pledged_amount = f"{row.get('pledged_amount', 0):,.2f}" if pd.notna(row.get('pledged_amount')) else "-"
            p_total_ratio = f"{row.get('p_total_ratio', 0):.4f}%" if pd.notna(row.get('p_total_ratio')) else "-"
            h_total_ratio = f"{row.get('h_total_ratio', 0):.4f}%" if pd.notna(row.get('h_total_ratio')) else "-"
            is_buyback = "是" if pd.notna(row.get('is_buyback')) and str(row.get('is_buyback')) == '1' else "否"
            
            result.append(f"{ann_date:<12} {holder_name:<25} {start_date:<12} {end_date:<12} {pledge_amount:<18} {is_release:<12} {release_date:<12} {pledgor:<25} {holding_amount:<18} {pledged_amount:<18} {p_total_ratio:<20} {h_total_ratio:<20} {is_buyback:<10}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'holder_name' in df.columns:
            result.append(f"涉及股东数量: {len(df['holder_name'].unique())} 个")
        
        if 'ann_date' in df.columns:
            result.append(f"涉及公告日期: {len(df['ann_date'].unique())} 个")
        
        # 计算总质押数量
        if 'pledge_amount' in df.columns:
            total_pledge = df['pledge_amount'].sum()
            result.append(f"总质押数量: {total_pledge:,.2f} 万股")
        
        # 计算总持股数量
        if 'holding_amount' in df.columns:
            total_holding = df['holding_amount'].sum()
            if total_holding > 0:
                result.append(f"总持股数量: {total_holding:,.2f} 万股")
        
        # 计算总质押数量（pledged_amount）
        if 'pledged_amount' in df.columns:
            total_pledged = df['pledged_amount'].sum()
            if total_pledged > 0:
                result.append(f"累计质押总数: {total_pledged:,.2f} 万股")
        
        # 统计解押情况
        if 'is_release' in df.columns:
            released_count = len(df[df['is_release'] == '是' if 'is_release' in df.columns else False])
            not_released_count = len(df) - released_count
            result.append("")
            result.append(f"已解押: {released_count} 条，未解押: {not_released_count} 条")
        
        # 统计回购情况
        if 'is_buyback' in df.columns:
            buyback_count = len(df[df['is_buyback'] == '1' if 'is_buyback' in df.columns else False])
            if buyback_count > 0:
                result.append(f"涉及回购: {buyback_count} 条")
        
        # 计算平均质押比例
        if 'p_total_ratio' in df.columns:
            avg_ratio = df['p_total_ratio'].mean()
            result.append(f"平均本次质押占总股本比例: {avg_ratio:.4f}%")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare pledge_detail接口")
    result.append("  - 显示股票质押明细数据，包括公告日期、股东名称、质押数量、质押开始/结束日期、是否已解押、解押日期、质押方、持股总数、质押总数、质押比例等信息")
    result.append("  - 权限要求：500积分")
    result.append("  - 限量：单次最大可调取1000条数据")
    
    return "\n".join(result)


def format_block_trade_data(df: pd.DataFrame, ts_code: str = "", date_filter: str = "") -> str:
    """
    格式化大宗交易数据输出
    
    参数:
        df: 大宗交易数据DataFrame
        ts_code: 股票代码（用于显示）
        date_filter: 日期筛选条件（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的大宗交易数据"
    
    result = []
    result.append("💼 大宗交易数据")
    result.append("=" * 180)
    result.append("")
    
    # 按交易日期排序（最新的在前）
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date', ascending=False)
    
    # 如果有多个股票，按股票代码分组显示
    if 'ts_code' in df.columns and len(df['ts_code'].unique()) > 1:
        codes = sorted(df['ts_code'].unique())
        result.append(f"共找到 {len(df)} 条记录，涉及 {len(codes)} 只股票")
        if date_filter:
            result.append(f"查询条件: {date_filter}")
        result.append("")
        
        # 按股票代码分组显示
        for code in codes[:50]:  # 最多显示前50只股票
            code_df = df[df['ts_code'] == code]
            if not code_df.empty:
                result.append(f"📊 {code} 大宗交易数据")
                result.append("-" * 180)
                
                # 如果有多个日期，按日期分组
                if 'trade_date' in code_df.columns and len(code_df['trade_date'].unique()) > 1:
                    dates = sorted(code_df['trade_date'].unique(), reverse=True)
                    for date in dates[:10]:  # 最多显示最近10个交易日
                        date_df = code_df[code_df['trade_date'] == date]
                        if not date_df.empty:
                            result.append(f"📅 交易日期: {format_date(str(date))}")
                            result.append(f"{'成交价(元)':<12} {'成交量(万股)':<18} {'成交金额(万元)':<18} {'买方营业部':<40} {'卖方营业部':<40}")
                            result.append("-" * 180)
                            
                            # 按成交金额排序（降序）
                            if 'amount' in date_df.columns:
                                date_df = date_df.sort_values('amount', ascending=False, na_position='last')
                            
                            for _, row in date_df.iterrows():
                                price = f"{row.get('price', 0):.2f}" if pd.notna(row.get('price')) else "-"
                                vol = f"{row.get('vol', 0):,.2f}" if pd.notna(row.get('vol')) else "-"
                                amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                                buyer = str(row.get('buyer', '-'))[:38]
                                seller = str(row.get('seller', '-'))[:38]
                                
                                result.append(f"{price:<12} {vol:<18} {amount:<18} {buyer:<40} {seller:<40}")
                            
                            result.append("")
                else:
                    # 单个日期，直接显示所有记录
                    result.append(f"{'交易日期':<12} {'成交价(元)':<12} {'成交量(万股)':<18} {'成交金额(万元)':<18} {'买方营业部':<40} {'卖方营业部':<40}")
                    result.append("-" * 180)
                    
                    # 按成交金额排序（降序）
                    if 'amount' in code_df.columns:
                        code_df = code_df.sort_values('amount', ascending=False, na_position='last')
                    
                    for _, row in code_df.iterrows():
                        trade_date = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
                        price = f"{row.get('price', 0):.2f}" if pd.notna(row.get('price')) else "-"
                        vol = f"{row.get('vol', 0):,.2f}" if pd.notna(row.get('vol')) else "-"
                        amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                        buyer = str(row.get('buyer', '-'))[:38]
                        seller = str(row.get('seller', '-'))[:38]
                        
                        result.append(f"{trade_date:<12} {price:<12} {vol:<18} {amount:<18} {buyer:<40} {seller:<40}")
                    
                    result.append("")
        
        if len(codes) > 50:
            result.append(f"  ... 还有 {len(codes) - 50} 只股票未显示")
    else:
        # 单个股票或没有ts_code字段，直接显示所有记录
        if 'ts_code' in df.columns and not df.empty:
            code = df.iloc[0].get('ts_code', ts_code or '-')
            result.append(f"📊 {code} 大宗交易数据")
        else:
            result.append(f"共找到 {len(df)} 条记录")
        result.append("")
        if date_filter:
            result.append(f"查询条件: {date_filter}")
        result.append("")
        
        # 如果有多个日期，按日期分组
        if 'trade_date' in df.columns and len(df['trade_date'].unique()) > 1:
            dates = sorted(df['trade_date'].unique(), reverse=True)
            for date in dates[:20]:  # 最多显示最近20个交易日
                date_df = df[df['trade_date'] == date]
                if not date_df.empty:
                    result.append(f"📅 交易日期: {format_date(str(date))}")
                    result.append(f"{'成交价(元)':<12} {'成交量(万股)':<18} {'成交金额(万元)':<18} {'买方营业部':<40} {'卖方营业部':<40}")
                    result.append("-" * 180)
                    
                    # 按成交金额排序（降序）
                    if 'amount' in date_df.columns:
                        date_df = date_df.sort_values('amount', ascending=False, na_position='last')
                    
                    display_count = min(100, len(date_df))
                    for _, row in date_df.head(display_count).iterrows():
                        price = f"{row.get('price', 0):.2f}" if pd.notna(row.get('price')) else "-"
                        vol = f"{row.get('vol', 0):,.2f}" if pd.notna(row.get('vol')) else "-"
                        amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                        buyer = str(row.get('buyer', '-'))[:38]
                        seller = str(row.get('seller', '-'))[:38]
                        
                        result.append(f"{price:<12} {vol:<18} {amount:<18} {buyer:<40} {seller:<40}")
                    
                    if len(date_df) > display_count:
                        result.append(f"  ... 还有 {len(date_df) - display_count} 条记录未显示")
                    result.append("")
        else:
            # 单个日期，直接显示所有记录
            result.append(f"{'交易日期':<12} {'成交价(元)':<12} {'成交量(万股)':<18} {'成交金额(万元)':<18} {'买方营业部':<40} {'卖方营业部':<40}")
            result.append("-" * 180)
            
            # 按成交金额排序（降序）
            if 'amount' in df.columns:
                df = df.sort_values('amount', ascending=False, na_position='last')
            
            display_count = min(200, len(df))
            for _, row in df.head(display_count).iterrows():
                trade_date = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
                price = f"{row.get('price', 0):.2f}" if pd.notna(row.get('price')) else "-"
                vol = f"{row.get('vol', 0):,.2f}" if pd.notna(row.get('vol')) else "-"
                amount = f"{row.get('amount', 0):,.2f}" if pd.notna(row.get('amount')) else "-"
                buyer = str(row.get('buyer', '-'))[:38]
                seller = str(row.get('seller', '-'))[:38]
                
                result.append(f"{trade_date:<12} {price:<12} {vol:<18} {amount:<18} {buyer:<40} {seller:<40}")
            
            if len(df) > display_count:
                result.append("")
                result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条，按成交金额降序排列）")
    
    # 显示统计信息
    if not df.empty:
        result.append("")
        result.append("📊 统计信息：")
        result.append("-" * 180)
        
        if 'ts_code' in df.columns:
            result.append(f"涉及股票数量: {len(df['ts_code'].unique())} 只")
        
        if 'trade_date' in df.columns:
            result.append(f"涉及交易日期: {len(df['trade_date'].unique())} 个")
        
        # 计算总成交量和成交金额
        if 'vol' in df.columns:
            total_vol = df['vol'].sum()
            result.append(f"总成交量: {total_vol:,.2f} 万股")
        
        if 'amount' in df.columns:
            total_amount = df['amount'].sum()
            result.append(f"总成交金额: {total_amount:,.2f} 万元")
        
        # 计算平均成交价
        if 'price' in df.columns:
            avg_price = df['price'].mean()
            result.append(f"平均成交价: {avg_price:.2f} 元")
        
        # 统计买方营业部
        if 'buyer' in df.columns:
            buyer_count = len(df['buyer'].unique())
            result.append(f"涉及买方营业部: {buyer_count} 个")
        
        # 统计卖方营业部
        if 'seller' in df.columns:
            seller_count = len(df['seller'].unique())
            result.append(f"涉及卖方营业部: {seller_count} 个")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare block_trade接口")
    result.append("  - 显示大宗交易数据，包括交易日期、成交价、成交量、成交金额、买方营业部、卖方营业部等信息")
    result.append("  - 权限要求：请查看Tushare文档确认具体权限要求")
    
    return "\n".join(result)


def format_limit_list_data(df: pd.DataFrame, trade_date: str = "", ts_code: str = "", limit_type: str = "") -> str:
    """
    格式化涨跌停列表数据输出
    
    参数:
        df: 涨跌停数据DataFrame
        trade_date: 交易日期（用于显示）
        ts_code: 股票代码（用于显示）
        limit_type: 涨跌停类型（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的涨跌停数据"
    
    result = []
    result.append("📈 涨跌停列表数据")
    result.append("=" * 200)
    result.append("")
    
    # 显示查询条件
    if trade_date:
        result.append(f"📅 交易日期: {format_date(trade_date)}")
    if ts_code:
        result.append(f"📊 股票代码: {ts_code}")
    if limit_type:
        limit_type_map = {'U': '涨停', 'D': '跌停', 'Z': '炸板'}
        result.append(f"🔖 类型: {limit_type_map.get(limit_type.upper(), limit_type)}")
    result.append("")
    
    # 统计信息
    if 'limit' in df.columns:
        limit_stats = df['limit'].value_counts()
        result.append("📊 统计信息：")
        result.append("-" * 200)
        limit_type_map = {'U': '涨停', 'D': '跌停', 'Z': '炸板'}
        for limit_val, count in limit_stats.items():
            type_name = limit_type_map.get(str(limit_val), str(limit_val))
            result.append(f"  - {type_name}: {count} 只")
        result.append("")
    
    # 如果查询的是单个股票
    if ts_code and 'ts_code' in df.columns:
        stock_df = df[df['ts_code'] == ts_code]
        if not stock_df.empty:
            result.append(f"共找到 {len(stock_df)} 条记录")
            result.append("")
            result.append(f"{'交易日期':<12} {'股票代码':<15} {'股票名称':<15} {'行业':<15} {'收盘价':<10} {'涨跌幅':<10} {'成交额(元)':<18} {'封单金额(元)':<18} {'首次封板':<12} {'最后封板':<12} {'炸板次数':<10} {'连板数':<8} {'涨停统计':<15}")
            result.append("-" * 200)
            
            for _, row in stock_df.iterrows():
                trade_date_str = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
                code = str(row.get('ts_code', '-'))[:13]
                name = str(row.get('name', '-'))[:13]
                industry = str(row.get('industry', '-'))[:13]
                close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
                pct_chg = f"{row.get('pct_chg', 0):+.2f}%" if pd.notna(row.get('pct_chg')) else "-"
                amount = format_large_number(row.get('amount', 0)) if pd.notna(row.get('amount')) else "-"
                fd_amount = format_large_number(row.get('fd_amount', 0)) if pd.notna(row.get('fd_amount')) else "-"
                first_time = str(row.get('first_time', '-'))[:10] if pd.notna(row.get('first_time')) else "-"
                last_time = str(row.get('last_time', '-'))[:10] if pd.notna(row.get('last_time')) else "-"
                open_times = str(int(row.get('open_times', 0))) if pd.notna(row.get('open_times')) else "-"
                limit_times = str(int(row.get('limit_times', 0))) if pd.notna(row.get('limit_times')) else "-"
                up_stat = str(row.get('up_stat', '-'))[:13] if pd.notna(row.get('up_stat')) else "-"
                
                result.append(f"{trade_date_str:<12} {code:<15} {name:<15} {industry:<15} {close:<10} {pct_chg:<10} {amount:<18} {fd_amount:<18} {first_time:<12} {last_time:<12} {open_times:<10} {limit_times:<8} {up_stat:<15}")
            
            return "\n".join(result)
    
    # 按类型分组显示
    if 'limit' in df.columns:
        # 按连板数排序（降序），然后按封单金额排序（降序）
        if 'limit_times' in df.columns:
            df = df.sort_values(['limit_times', 'fd_amount'], ascending=[False, False], na_position='last')
        elif 'fd_amount' in df.columns:
            df = df.sort_values('fd_amount', ascending=False, na_position='last')
        
        result.append(f"共找到 {len(df)} 条涨跌停记录，涉及 {len(df['ts_code'].unique()) if 'ts_code' in df.columns else len(df)} 只股票")
        result.append("")
        result.append(f"{'交易日期':<12} {'股票代码':<15} {'股票名称':<15} {'行业':<15} {'类型':<8} {'收盘价':<10} {'涨跌幅':<10} {'成交额(元)':<18} {'封单金额(元)':<18} {'首次封板':<12} {'最后封板':<12} {'炸板次数':<10} {'连板数':<8} {'涨停统计':<15}")
        result.append("-" * 200)
        
        display_count = min(100, len(df))
        for _, row in df.head(display_count).iterrows():
            trade_date_str = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
            code = str(row.get('ts_code', '-'))[:13]
            name = str(row.get('name', '-'))[:13]
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
            
            result.append(f"{trade_date_str:<12} {code:<15} {name:<15} {industry:<15} {limit_type_name:<8} {close:<10} {pct_chg:<10} {amount:<18} {fd_amount:<18} {first_time:<12} {last_time:<12} {open_times:<10} {limit_times:<8} {up_stat:<15}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    else:
        # 如果没有limit字段，直接显示所有记录
        result.append(f"共找到 {len(df)} 条涨跌停记录")
        result.append("")
        result.append(f"{'交易日期':<12} {'股票代码':<15} {'股票名称':<15} {'行业':<15} {'收盘价':<10} {'涨跌幅':<10} {'成交额(元)':<18} {'封单金额(元)':<18} {'首次封板':<12} {'最后封板':<12} {'炸板次数':<10} {'连板数':<8} {'涨停统计':<15}")
        result.append("-" * 200)
        
        display_count = min(100, len(df))
        for _, row in df.head(display_count).iterrows():
            trade_date_str = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
            code = str(row.get('ts_code', '-'))[:13]
            name = str(row.get('name', '-'))[:13]
            industry = str(row.get('industry', '-'))[:13]
            close = f"{row.get('close', 0):.2f}" if pd.notna(row.get('close')) else "-"
            pct_chg = f"{row.get('pct_chg', 0):+.2f}%" if pd.notna(row.get('pct_chg')) else "-"
            amount = format_large_number(row.get('amount', 0)) if pd.notna(row.get('amount')) else "-"
            fd_amount = format_large_number(row.get('fd_amount', 0)) if pd.notna(row.get('fd_amount')) else "-"
            first_time = str(row.get('first_time', '-'))[:10] if pd.notna(row.get('first_time')) else "-"
            last_time = str(row.get('last_time', '-'))[:10] if pd.notna(row.get('last_time')) else "-"
            open_times = str(int(row.get('open_times', 0))) if pd.notna(row.get('open_times')) else "-"
            limit_times = str(int(row.get('limit_times', 0))) if pd.notna(row.get('limit_times')) else "-"
            up_stat = str(row.get('up_stat', '-'))[:13] if pd.notna(row.get('up_stat')) else "-"
            
            result.append(f"{trade_date_str:<12} {code:<15} {name:<15} {industry:<15} {close:<10} {pct_chg:<10} {amount:<18} {fd_amount:<18} {first_time:<12} {last_time:<12} {open_times:<10} {limit_times:<8} {up_stat:<15}")
        
        if len(df) > display_count:
            result.append("")
            result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示详细统计信息
    if not df.empty:
        result.append("")
        result.append("📊 详细统计：")
        result.append("-" * 200)
        
        if 'ts_code' in df.columns:
            result.append(f"涉及股票数量: {len(df['ts_code'].unique())} 只")
        
        if 'trade_date' in df.columns:
            result.append(f"涉及交易日期: {len(df['trade_date'].unique())} 个")
        
        # 计算总成交额
        if 'amount' in df.columns:
            total_amount = df['amount'].sum()
            result.append(f"总成交额: {format_large_number(total_amount)} 元")
        
        # 计算总封单金额
        if 'fd_amount' in df.columns:
            total_fd_amount = df['fd_amount'].sum()
            result.append(f"总封单金额: {format_large_number(total_fd_amount)} 元")
        
        # 统计连板情况
        if 'limit_times' in df.columns:
            max_limit_times = df['limit_times'].max()
            if pd.notna(max_limit_times):
                result.append(f"最高连板数: {int(max_limit_times)} 板")
        
        # 统计炸板情况
        if 'open_times' in df.columns:
            total_open_times = df['open_times'].sum()
            result.append(f"总炸板次数: {int(total_open_times)} 次")
            avg_open_times = df['open_times'].mean()
            if pd.notna(avg_open_times):
                result.append(f"平均炸板次数: {avg_open_times:.2f} 次")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare limit_list_d接口")
    result.append("  - 数据历史：2020年至今（不提供ST股票的统计）")
    result.append("  - 类型说明：U=涨停，D=跌停，Z=炸板")
    result.append("  - 封单金额：以涨停价买入挂单的资金总量（跌停无此数据）")
    result.append("  - 首次封板时间：股票首次达到涨停价的时间（跌停无此数据）")
    result.append("  - 炸板次数：涨停后开板的次数（跌停为开板次数）")
    result.append("  - 连板数：个股连续封板数量")
    result.append("  - 涨停统计：格式为N/T，表示T天内有N次涨停")
    result.append("  - 权限要求：5000积分（每分钟200次，每天总量1万次），8000积分以上（每分钟500次，每天总量不限制）")
    result.append("  - 限量：单次最大可获取2500条数据，可通过日期或股票循环提取")
    
    return "\n".join(result)


def format_limit_cpt_list_data(df: pd.DataFrame, trade_date: str = "", ts_code: str = "") -> str:
    """
    格式化最强板块统计数据输出
    
    参数:
        df: 最强板块统计数据DataFrame
        trade_date: 交易日期（用于显示）
        ts_code: 板块代码（用于显示）
    
    返回:
        格式化后的字符串
    """
    if df.empty:
        return "未找到符合条件的最强板块统计数据"
    
    result = []
    result.append("🏆 最强板块统计")
    result.append("=" * 200)
    result.append("")
    
    # 显示查询条件
    if trade_date:
        result.append(f"📅 交易日期: {format_date(trade_date)}")
    if ts_code:
        result.append(f"📊 板块代码: {ts_code}")
    result.append("")
    
    # 如果查询的是单个板块
    if ts_code and 'ts_code' in df.columns:
        cpt_df = df[df['ts_code'] == ts_code]
        if not cpt_df.empty:
            result.append(f"共找到 {len(cpt_df)} 条记录")
            result.append("")
            result.append(f"{'交易日期':<12} {'板块代码':<20} {'板块名称':<20} {'上榜天数':<10} {'连板高度':<15} {'连板家数':<10} {'涨停家数':<10} {'涨跌幅(%)':<12} {'板块热点排名':<15}")
            result.append("-" * 200)
            
            for _, row in cpt_df.iterrows():
                trade_date_str = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
                code = str(row.get('ts_code', '-'))[:18]
                name = str(row.get('name', '-'))[:18]
                days = str(int(row.get('days', 0))) if pd.notna(row.get('days')) else "-"
                up_stat = str(row.get('up_stat', '-'))[:13] if pd.notna(row.get('up_stat')) else "-"
                cons_nums = str(int(row.get('cons_nums', 0))) if pd.notna(row.get('cons_nums')) else "-"
                up_nums = str(row.get('up_nums', '-'))[:8] if pd.notna(row.get('up_nums')) else "-"
                pct_chg = f"{row.get('pct_chg', 0):+.2f}%" if pd.notna(row.get('pct_chg')) else "-"
                rank = str(row.get('rank', '-'))[:13] if pd.notna(row.get('rank')) else "-"
                
                result.append(f"{trade_date_str:<12} {code:<20} {name:<20} {days:<10} {up_stat:<15} {cons_nums:<10} {up_nums:<10} {pct_chg:<12} {rank:<15}")
            
            return "\n".join(result)
    
    # 按板块热点排名排序显示
    result.append(f"共找到 {len(df)} 条最强板块记录，涉及 {len(df['ts_code'].unique()) if 'ts_code' in df.columns else len(df)} 个板块")
    result.append("")
    result.append(f"{'排名':<8} {'板块代码':<20} {'板块名称':<20} {'交易日期':<12} {'上榜天数':<10} {'连板高度':<15} {'连板家数':<10} {'涨停家数':<10} {'涨跌幅(%)':<12}")
    result.append("-" * 200)
    
    display_count = min(100, len(df))
    for idx, (_, row) in enumerate(df.head(display_count).iterrows(), 1):
        rank = str(row.get('rank', idx))[:6] if pd.notna(row.get('rank')) else str(idx)
        code = str(row.get('ts_code', '-'))[:18]
        name = str(row.get('name', '-'))[:18]
        trade_date_str = format_date(str(row.get('trade_date', '-'))) if pd.notna(row.get('trade_date')) else "-"
        days = str(int(row.get('days', 0))) if pd.notna(row.get('days')) else "-"
        up_stat = str(row.get('up_stat', '-'))[:13] if pd.notna(row.get('up_stat')) else "-"
        cons_nums = str(int(row.get('cons_nums', 0))) if pd.notna(row.get('cons_nums')) else "-"
        up_nums = str(row.get('up_nums', '-'))[:8] if pd.notna(row.get('up_nums')) else "-"
        pct_chg = f"{row.get('pct_chg', 0):+.2f}%" if pd.notna(row.get('pct_chg')) else "-"
        
        result.append(f"{rank:<8} {code:<20} {name:<20} {trade_date_str:<12} {days:<10} {up_stat:<15} {cons_nums:<10} {up_nums:<10} {pct_chg:<12}")
    
    if len(df) > display_count:
        result.append("")
        result.append(f"（共 {len(df)} 条数据，仅显示前 {display_count} 条）")
    
    # 显示详细统计信息
    if not df.empty:
        result.append("")
        result.append("📊 详细统计：")
        result.append("-" * 200)
        
        if 'ts_code' in df.columns:
            result.append(f"涉及板块数量: {len(df['ts_code'].unique())} 个")
        
        if 'trade_date' in df.columns:
            result.append(f"涉及交易日期: {len(df['trade_date'].unique())} 个")
        
        # 统计涨停家数
        if 'up_nums' in df.columns:
            # up_nums可能是字符串，需要转换
            try:
                up_nums_list = []
                for val in df['up_nums']:
                    if pd.notna(val):
                        # 尝试提取数字
                        import re
                        nums = re.findall(r'\d+', str(val))
                        if nums:
                            up_nums_list.append(int(nums[0]))
                if up_nums_list:
                    total_up_nums = sum(up_nums_list)
                    result.append(f"总涨停家数: {total_up_nums} 家")
                    avg_up_nums = total_up_nums / len(up_nums_list)
                    result.append(f"平均涨停家数: {avg_up_nums:.2f} 家")
            except:
                pass
        
        # 统计连板家数
        if 'cons_nums' in df.columns:
            total_cons_nums = df['cons_nums'].sum()
            result.append(f"总连板家数: {int(total_cons_nums)} 家")
            avg_cons_nums = df['cons_nums'].mean()
            if pd.notna(avg_cons_nums):
                result.append(f"平均连板家数: {avg_cons_nums:.2f} 家")
        
        # 统计涨跌幅
        if 'pct_chg' in df.columns:
            avg_pct_chg = df['pct_chg'].mean()
            if pd.notna(avg_pct_chg):
                result.append(f"平均涨跌幅: {avg_pct_chg:+.2f}%")
            max_pct_chg = df['pct_chg'].max()
            if pd.notna(max_pct_chg):
                result.append(f"最高涨跌幅: {max_pct_chg:+.2f}%")
        
        # 统计上榜天数
        if 'days' in df.columns:
            max_days = df['days'].max()
            if pd.notna(max_days):
                result.append(f"最长上榜天数: {int(max_days)} 天")
            avg_days = df['days'].mean()
            if pd.notna(avg_days):
                result.append(f"平均上榜天数: {avg_days:.2f} 天")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare limit_cpt_list接口")
    result.append("  - 功能：获取每天涨停股票最多最强的概念板块，可以分析强势板块的轮动，判断资金动向")
    result.append("  - 上榜天数：该板块连续上榜的天数")
    result.append("  - 连板高度：板块内股票的连板情况（如：9天7板表示9个交易日内有7个涨停板）")
    result.append("  - 连板家数：板块内连续涨停的股票数量")
    result.append("  - 涨停家数：板块内当日涨停的股票数量")
    result.append("  - 板块热点排名：根据涨停家数、连板高度等指标综合排名，排名越小越强")
    result.append("  - 权限要求：8000积分以上每分钟500次，每天总量不限制")
    result.append("  - 限量：单次最大2000行数据，可根据股票代码或日期循环提取全部")
    
    return "\n".join(result)


def format_stock_auction_data(df: pd.DataFrame, ts_code: str = "") -> str:
    """
    格式化集合竞价数据
    
    参数:
        df: 集合竞价数据DataFrame
        ts_code: 股票代码（可选，用于单股票查询时的标题）
    """
    if df.empty:
        return "未找到集合竞价数据"
    
    result = []
    
    # 标题
    if ts_code:
        result.append(f"📊 股票 {ts_code} 集合竞价成交情况")
    else:
        result.append("📊 集合竞价成交情况")
    result.append("=" * 80)
    result.append("")
    
    # 数据统计
    result.append("📈 数据统计:")
    result.append(f"  - 记录数量: {len(df)} 条")
    
    # 日期范围
    if 'trade_date' in df.columns:
        dates = df['trade_date'].unique()
        if len(dates) == 1:
            result.append(f"  - 交易日期: {dates[0]}")
        else:
            result.append(f"  - 日期范围: {min(dates)} 至 {max(dates)}")
    
    result.append("")
    
    # 数据表格
    result.append("📋 详细数据:")
    result.append("")
    
    # 构建表头
    headers = []
    if 'ts_code' in df.columns:
        headers.append(('股票代码', 12))
    if 'trade_date' in df.columns:
        headers.append(('交易日期', 12))
    if 'vol' in df.columns:
        headers.append(('成交量(股)', 15))
    if 'price' in df.columns:
        headers.append(('成交均价(元)', 15))
    if 'amount' in df.columns:
        headers.append(('成交金额(元)', 18))
    if 'pre_close' in df.columns:
        headers.append(('昨收价(元)', 15))
    if 'turnover_rate' in df.columns:
        headers.append(('换手率(%)', 12))
    if 'volume_ratio' in df.columns:
        headers.append(('量比', 10))
    if 'float_share' in df.columns:
        headers.append(('流通股本(万股)', 15))
    
    # 打印表头
    if headers:
        header_line = " | ".join([f"{h[0]:<{h[1]}}" for h in headers])
        result.append(header_line)
        result.append("-" * len(header_line))
    
    # 打印数据行
    for idx, row in df.iterrows():
        row_data = []
        for header, width in headers:
            field = header.split('(')[0].replace(' ', '_').lower()
            # 字段名映射
            field_map = {
                '股票代码': 'ts_code',
                '交易日期': 'trade_date',
                '成交量(股)': 'vol',
                '成交均价(元)': 'price',
                '成交金额(元)': 'amount',
                '昨收价(元)': 'pre_close',
                '换手率(%)': 'turnover_rate',
                '量比': 'volume_ratio',
                '流通股本(万股)': 'float_share'
            }
            field_name = field_map.get(header, field)
            
            if field_name in row.index:
                value = row[field_name]
                if pd.isna(value):
                    row_data.append(f"{'-':<{width}}")
                elif field_name in ['vol', 'amount', 'float_share']:
                    if field_name == 'vol':
                        # 成交量，整数显示
                        row_data.append(f"{int(value):<{width},}")
                    elif field_name == 'amount':
                        # 成交金额，保留2位小数
                        row_data.append(f"{float(value):<{width},.2f}")
                    else:
                        # 流通股本，保留2位小数
                        row_data.append(f"{float(value):<{width},.2f}")
                elif field_name in ['price', 'pre_close']:
                    # 价格，保留2位小数
                    row_data.append(f"{float(value):<{width},.2f}")
                elif field_name in ['turnover_rate', 'volume_ratio']:
                    # 百分比和比率，保留4位小数
                    row_data.append(f"{float(value):<{width},.4f}")
                else:
                    row_data.append(f"{str(value):<{width}}")
            else:
                row_data.append(f"{'-':<{width}}")
        
        result.append(" | ".join(row_data))
    
    result.append("")
    
    # 统计信息
    result.append("📊 统计信息:")
    if 'vol' in df.columns:
        total_vol = df['vol'].sum()
        result.append(f"  - 总成交量: {total_vol:,} 股")
    if 'amount' in df.columns:
        total_amount = df['amount'].sum()
        result.append(f"  - 总成交金额: {total_amount:,.2f} 元")
    if 'price' in df.columns:
        avg_price = df['price'].mean()
        if pd.notna(avg_price):
            result.append(f"  - 平均成交价: {avg_price:.2f} 元")
    if 'turnover_rate' in df.columns:
        avg_turnover = df['turnover_rate'].mean()
        if pd.notna(avg_turnover):
            result.append(f"  - 平均换手率: {avg_turnover:.4f}%")
    if 'volume_ratio' in df.columns:
        avg_vol_ratio = df['volume_ratio'].mean()
        if pd.notna(avg_vol_ratio):
            result.append(f"  - 平均量比: {avg_vol_ratio:.4f}")
    
    result.append("")
    result.append("📝 说明：")
    result.append("  - 数据来源：Tushare stk_auction接口")
    result.append("  - 功能：获取当日个股和ETF的集合竞价成交情况")
    result.append("  - 查询时间：每天9点25~29分之间可以获取当日的集合竞价成交数据")
    result.append("  - 权限要求：本接口是单独开权限的数据，已经开通了股票分钟权限的用户可自动获得本接口权限")
    result.append("  - 限量：单次最大返回8000行数据，可根据日期或代码循环获取历史")
    result.append("  - 成交量：集合竞价期间的成交量（股）")
    result.append("  - 成交均价：集合竞价期间的成交均价（元）")
    result.append("  - 成交金额：集合竞价期间的成交金额（元）")
    result.append("  - 换手率：集合竞价期间的换手率（%）")
    result.append("  - 量比：集合竞价期间的量比")
    
    return "\n".join(result)