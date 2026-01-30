"""板块成份股健康度分析工具"""
import pandas as pd
import json
from typing import TYPE_CHECKING, Dict, List, Optional
from datetime import datetime
import numpy as np

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

from cache.mapping_cache_manager import mapping_cache_manager
from cache.stock_daily_cache_manager import stock_daily_cache_manager
from cache.stock_intraday_cache_manager import stock_intraday_cache_manager
from cache.sector_strength_cache_manager import sector_strength_cache_manager
from config.token_manager import get_tushare_token
import tushare as ts

def register_sector_constituents_health_tools(mcp: "FastMCP"):
    """注册板块成份股健康度分析工具"""
    mcp.tool()(analyze_sector_constituents_health)

def analyze_sector_constituents_health(sector_code: str, sector_type: str = "em_concept") -> str:
    """
    实时分析板块中成份股的健康情况并提供投资建议
    
    参数:
        sector_code: 板块代码（如：BK1184.DC, 801053.SI）
        sector_type: 板块类型 ('sw_l2', 'em_industry', 'em_concept')
    """
    token = get_tushare_token()
    if not token:
        return "请先配置Tushare token"
    
    try:
        pro = ts.pro_api()
        now = datetime.now()
        trade_date = now.strftime("%Y%m%d")
        current_time_str = now.strftime("%H:%M:%S")

        # 1. 获取该板块下的成份股
        df_mapping = mapping_cache_manager.search_by_sector(sector_type, sector_code)
        if df_mapping.empty:
            return f"未找到板块 {sector_code} 的成份股映射，请先运行同步映射工具。"
        
        stock_list = df_mapping['ts_code'].tolist()
        stock_names = df_mapping.set_index('ts_code')['name'].to_dict()

        # 2. 获取成份股实时行情 (rt_k)
        print(f"[{current_time_str}] 正在获取成份股实时行情...", file=__import__('sys').stderr)
        fields = 'ts_code,open,close,high,low,pre_close,pct_chg,vol,amount,num,bid_volume1,ask_volume1'
        ts_code_str = ",".join(stock_list)
        
        # 分批获取，避免代码过长
        rt_dfs = []
        batch_size = 300
        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i:i+batch_size]
            try:
                df_batch = pro.rt_k(ts_code=",".join(batch), fields=fields)
                if df_batch is not None and not df_batch.empty:
                    rt_dfs.append(df_batch)
            except: continue
        
        if not rt_dfs:
            return "未能获取到成份股实时行情。"
            
        df_rt = pd.concat(rt_dfs).set_index('ts_code')
        
        # 确保关键数值列存在并转换为数值类型
        # 注意：Tushare rt_k 接口不一定能返回所有请求的字段，需要进行防御性处理
        numeric_cols = ['open', 'close', 'high', 'low', 'pre_close', 'pct_chg', 'vol', 'amount', 'num']
        for col in numeric_cols:
            if col not in df_rt.columns:
                df_rt[col] = 0.0
            df_rt[col] = pd.to_numeric(df_rt[col], errors='coerce').fillna(0.0)
        
        # 补齐 pct_chg: 如果 API 没返回或全为空，则根据 close/pre_close 手动计算
        if df_rt['pct_chg'].sum() == 0:
            mask = df_rt['pre_close'] > 0
            df_rt.loc[mask, 'pct_chg'] = (df_rt.loc[mask, 'close'] - df_rt.loc[mask, 'pre_close']) / df_rt.loc[mask, 'pre_close'] * 100
        
        # 3. 获取板块背景
        sector_name = ""
        if sector_type == 'sw_l2':
            sector_name = df_mapping.iloc[0]['sw_l2_name']
        elif sector_type == 'em_industry':
            sector_name = df_mapping.iloc[0]['em_industry_name']
        elif sector_type == 'em_concept':
            # 概念板块 mapping 中可能包含多个，取匹配的那一个
            try:
                for idx, row in df_mapping.iterrows():
                    codes = json.loads(row['em_concept_codes'])
                    names = json.loads(row['em_concept_names'])
                    if sector_code in codes:
                        sector_name = names[codes.index(sector_code)]
                        break
            except:
                sector_name = sector_code

        # 获取板块平均表现 (作为基准)
        sector_avg_pct = df_rt['pct_chg'].mean()
        
        # 4. 获取早盘基准 (09:35 左右)
        # 我们查找 09:35 或之后最近的一笔，作为日内动量基点
        print(f"正在获取早盘基点数据...", file=__import__('sys').stderr)
        morning_snapshots = {}
        for code in stock_list:
            snap = stock_intraday_cache_manager.get_historical_snapshot(code, trade_date, "09:35:00")
            if snap:
                morning_snapshots[code] = snap.get('close')

        # 5. 个股健康度计算
        constituents_health = []
        for ts_code, row in df_rt.iterrows():
            # 基础指标
            pct_chg = row.get('pct_chg', 0)
            amount = row.get('amount', 0)
            num = row.get('num', 0) or 1
            high = row.get('high', 0)
            low = row.get('low', 0)
            close = row.get('close', 0)
            open_p = row.get('open', 0)
            
            # 计算指标
            vpt = (amount * 1000) / num  # 单笔成交额
            price_range = high - low
            intraday_pos = (close - low) / price_range if price_range > 0 else 0.5
            is_positive = close > open_p
            
            # 动量：相比 09:35 的涨幅
            m_price = morning_snapshots.get(ts_code)
            momentum = (close / m_price - 1) * 100 if m_price else 0
            
            # 板块共振
            relative_pct = pct_chg - sector_avg_pct
            
            # --- 评分模型 ---
            # 1. 强度 (30分)
            score_str = min(max(pct_chg + 5, 0) / 15 * 20, 20) + (intraday_pos * 10)
            # 2. 动量 (20分)
            score_mom = min(max(momentum + 2, 0) / 5 * 20, 20)
            # 3. 活跃度 (20分)
            score_act = min(vpt / 50000 * 20, 20)
            # 4. 量能 (15分, 这里简单用涨跌幅与 VPT 的配合)
            score_vol = 15 if (pct_chg > 0 and vpt > 20000) or (pct_chg < 0 and vpt < 10000) else 7
            # 5. 共振 (15分)
            score_syn = min(max(relative_pct + 2, 0) / 4 * 15, 15)
            
            total_score = score_str + score_mom + score_act + score_vol + score_syn
            
            # 标签判定
            tags = []
            if intraday_pos > 0.8 and pct_chg > 3: tags.append("强力封锁")
            if vpt > 60000: tags.append("大资金入驻")
            if momentum > 2: tags.append("转折放量")
            if relative_pct > 3: tags.append("领涨龙头")
            if intraday_pos < 0.3 and pct_chg > 0: tags.append("冲高回落")
            
            constituents_health.append({
                'code': ts_code,
                'name': stock_names.get(ts_code, '未知'),
                'pct': pct_chg,
                'pos': intraday_pos,
                'vpt': vpt,
                'mom': momentum,
                'rel': relative_pct,
                'score': total_score,
                'tags': tags
            })

        # 6. 结果分组 (上下文优化)
        df_h = pd.DataFrame(constituents_health).sort_values('score', ascending=False)
        
        # 挑选 Leaders (Top 5)
        leaders = df_h.head(5).to_dict('records')
        
        # 挑选 Risky (Bottom 3)
        risky = df_h.tail(3).to_dict('records')
        
        # 挑选异动 (VPT 最大但涨幅不居前)
        anomalies = df_h[df_h['pct'] < 2].sort_values('vpt', ascending=False).head(2).to_dict('records')

        # 7. 整体评价与建议
        sector_score = df_h['score'].mean()
        rising_ratio = (df_h['pct'] > 0).mean() * 100
        
        advice = "持币观望"
        if sector_score > 70 and rising_ratio > 60:
            advice = "🔥 强烈看好：板块呈现集群进攻态势，领涨股位阶极高，资金介入深。"
        elif sector_score > 60:
            advice = "📈 稳健运行：板块重心上移，赚钱效应较好，建议关注分时回调机会。"
        elif sector_score < 40:
            advice = "📉 谨慎避险：板块整体位阶低迷，多股冲高回落，缺乏核心买盘支撑。"
        else:
            advice = "⚖️ 分歧震荡：个股分化严重，领涨股与跟风股脱节，需甄选个股。"

        # 8. 格式化输出
        output = [
            f"## 板块成份健康度分析: {sector_name} ({sector_code})",
            f"- **分析时刻**: {current_time_str}",
            f"- **板块平均涨幅**: {sector_avg_pct:.2f}%",
            f"- **上涨家数比**: {rising_ratio:.1f}%",
            f"- **板块健康均分**: {sector_score:.1f}",
            f"### 💡 投资建议",
            advice,
            f"---",
            f"### 🌟 健康阵营 (Top 5 Leader)",
            "| 股票 | 涨幅 | 位阶 | 单笔额 | 动量 | 分数 | 标签 |",
            "| --- | --- | --- | --- | --- | --- | --- |"
        ]
        for r in leaders:
            output.append(f"| {r['name']}({r['code']}) | {r['pct']:.2f}% | {r['pos']:.2f} | {r['vpt']/1000:.1f}k | {r['mom']:.1f}% | {r['score']:.1f} | {','.join(r['tags'])} |")
        
        output.append("\n### ⚠️ 风险/落后个股")
        for r in risky:
            output.append(f"- **{r['name']}**: 分数 {r['score']:.1f}，涨幅 {r['pct']:.2f}%，位阶 {r['pos']:.2f} (属于低迷或分歧状态)")

        if anomalies:
            output.append("\n### 🔍 值得注意的异动 (涨幅尚小但资金暗涌)")
            for r in anomalies:
                output.append(f"- **{r['name']}**: 单笔成交额 {r['vpt']/1000:.1f}k，涨幅 {r['pct']:.2f}%，分数 {r['score']:.1f}")

        return "\n".join(output)

    except Exception as e:
        import traceback
        return f"分析失败: {str(e)}\n{traceback.format_exc()}"
