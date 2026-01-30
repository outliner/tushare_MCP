
"""
全量拉取并建立股票与申万二级行业、东财行业、东财概念的映射关系 (Standalone Script)
"""
import sys
import pandas as pd
import tushare as ts
import json
from pathlib import Path

# Setup path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.token_manager import get_tushare_token
from cache.mapping_cache_manager import mapping_cache_manager
from tools.concept_tools import get_dc_board_codes

def collect_stock_sector_mapping():
    token = get_tushare_token()
    if not token:
        print("请先配置Tushare token")
        return

    try:
        pro = ts.pro_api()
        print("开始全量映射采集...", file=sys.stderr)
        
        # 1. 获取所有A股股票基本信息
        print("正在读取股票列表...", file=sys.stderr)
        df_basic = pro.stock_basic(list_status='L', fields='ts_code,name,industry')
        if df_basic.empty:
            print("未能获取到股票基本信息")
            return
        
        # 初始化汇总字典
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
        print("正在采集申万二级行业映射...", file=sys.stderr)
        sw_l2_classify = pro.index_classify(level='L2', src='SW2021')
        if not sw_l2_classify.empty:
            for _, ind in sw_l2_classify.iterrows():
                l2_code = ind['index_code']
                l2_name = ind['industry_name']
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
        
        # 3.1 行业板块
        print("正在采集东财行业板块映射...", file=sys.stderr)
        try:
            industry_boards = get_dc_board_codes(board_type='行业板块')
            if not industry_boards:
                # 尝试再次获取，有时网络问题
                industry_boards = get_dc_board_codes(board_type='行业板块')
                
            print(f"   - 发现 {len(industry_boards)} 个行业板块")
            processed_count = 0
            for board in industry_boards:
                b_code = board['ts_code']
                b_name = board['name']
                try:
                    m = pro.dc_member(ts_code=b_code)
                    if m is not None and not m.empty:
                        for _, member in m.iterrows():
                            m_code = member['con_code']
                            if m_code in stock_map:
                                stock_map[m_code]['em_industry_code'] = b_code
                                stock_map[m_code]['em_industry_name'] = b_name
                except:
                    continue
                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"     已处理 {processed_count} 个行业...", end='\r', file=sys.stderr)
            print("", file=sys.stderr)

            # 3.2 东财概念
            print("正在采集东财概念板块映射...", file=sys.stderr)
            concept_boards = get_dc_board_codes(board_type='概念板块')
            print(f"   - 发现 {len(concept_boards)} 个概念板块")
            processed_count = 0
            for board in concept_boards:
                b_code = board['ts_code']
                b_name = board['name']
                try:
                    m = pro.dc_member(ts_code=b_code)
                    if m is not None and not m.empty:
                        for _, member in m.iterrows():
                            m_code = member['con_code']
                            if m_code in stock_map:
                                # Avoid duplicates
                                if b_code not in stock_map[m_code]['em_concept_codes']:
                                    stock_map[m_code]['em_concept_codes'].append(b_code)
                                    stock_map[m_code]['em_concept_names'].append(b_name)
                except:
                    continue
                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"     已处理 {processed_count} 个概念...", end='\r', file=sys.stderr)
            print("", file=sys.stderr)

        except Exception as e:
            print(f"采集东财板块数据出错: {str(e)}", file=sys.stderr)

        # 4. 汇总与入库
        print("正在同步到本地数据库...", file=sys.stderr)
        final_list = list(stock_map.values())
        if not final_list:
             print("没有数据需要保存")
             return

        df_final = pd.DataFrame(final_list)
        
        saved_count = mapping_cache_manager.save_mapping(df_final)
        
        print(f"全量映射采集完成！")
        print(f"- 扫描股票总数: {len(df_basic)}")
        print(f"- 成功入库/更新记录: {saved_count}")
        
    except Exception as e:
        import traceback
        print(f"映射采集失败: {str(e)}")
        print(traceback.format_exc())

if __name__ == "__main__":
    collect_stock_sector_mapping()
