"""财务指标数据专用缓存管理器"""
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from config.settings import CACHE_DB


class FinaIndicatorCacheManager:
    """财务指标数据专用缓存管理器
    
    特点：
    1. 使用专门的表存储财务指标数据，字段直接映射到数据库列
    2. 缓存策略：永不失效（数据永久保存）
    3. 主键：(ts_code, end_date, ann_date)，确保同一股票同一报告期只有一条数据
    4. 支持按股票代码、公告日期、报告期、日期范围查询
    """
    
    def __init__(self, db_path: Path = CACHE_DB):
        """初始化财务指标缓存管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 启用WAL模式提升并发性能
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_database()
    
    def _init_database(self):
        """创建财务指标数据表"""
        cursor = self.conn.cursor()
        
        # 创建财务指标数据表
        # 根据 Tushare fina_indicator 接口的数据格式设计
        # 由于字段非常多，我们只存储主要字段，其他字段可以动态添加
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fina_indicator_data (
                ts_code TEXT NOT NULL,          -- 股票代码（如：600000.SH）
                ann_date TEXT,                  -- 公告日期
                end_date TEXT NOT NULL,         -- 报告期
                eps REAL,                       -- 每股收益
                dt_eps REAL,                    -- 每股收益(扣除非经常损益)
                total_revenue_ps REAL,          -- 每股营业总收入
                revenue_ps REAL,                -- 每股营业收入
                capital_rese_ps REAL,           -- 每股资本公积
                surplus_rese_ps REAL,           -- 每股盈余公积
                undist_profit_ps REAL,          -- 每股未分配利润
                extra_item REAL,                -- 非经常性损益
                profit_dedt REAL,               -- 扣除非经常性损益后的净利润
                gross_margin REAL,              -- 毛利
                current_ratio REAL,             -- 流动比率
                quick_ratio REAL,               -- 速动比率
                cash_ratio REAL,                -- 现金比率
                invturn_days REAL,              -- 存货周转天数
                arturn_days REAL,               -- 应收账款周转天数
                inv_turn REAL,                  -- 存货周转率
                ar_turn REAL,                   -- 应收账款周转率
                ca_turn REAL,                   -- 流动资产周转率
                fa_turn REAL,                   -- 固定资产周转率
                assets_turn REAL,               -- 总资产周转率
                op_income REAL,                 -- 经营活动净收益
                valuechange_income REAL,         -- 价值变动净收益
                interst_income REAL,            -- 利息费用
                daa REAL,                       -- 折旧与摊销
                ebit REAL,                      -- 息税前利润
                ebitda REAL,                    -- 息税折旧摊销前利润
                fcff REAL,                      -- 企业自由现金流量
                fcfe REAL,                      -- 股权自由现金流量
                current_exint REAL,             -- 流动负债合计
                noncurrent_exint REAL,          -- 非流动负债合计
                interestdebt REAL,              -- 带息债务
                netdebt REAL,                   -- 净债务
                tangible_asset REAL,             -- 有形资产
                working_capital REAL,            -- 营运资金
                networking_capital REAL,         -- 营运流动资本
                invest_capital REAL,             -- 全部投入资本
                retained_earnings REAL,          -- 留存收益
                diluted2_eps REAL,               -- 稀释每股收益
                bps REAL,                       -- 每股净资产
                ocfps REAL,                     -- 每股经营活动产生的现金流量净额
                retainedps REAL,                -- 每股留存收益
                cfps REAL,                      -- 每股现金流量净额
                ebit_ps REAL,                   -- 每股息税前利润
                fcff_ps REAL,                   -- 每股企业自由现金流量
                fcfe_ps REAL,                   -- 每股股东自由现金流量
                netprofit_margin REAL,           -- 销售净利率
                grossprofit_margin REAL,         -- 销售毛利率
                cost_of_sales REAL,              -- 销售成本率
                profit_of_sales REAL,            -- 销售期间费用率
                roe REAL,                       -- 净资产收益率
                roa REAL,                       -- 总资产报酬率
                roic REAL,                      -- 投入资本回报率
                roe_yearly REAL,                 -- 年化净资产收益率
                roa2_yearly REAL,                -- 年化总资产报酬率
                roe_avg REAL,                    -- 平均净资产收益率
                opincome_of_ebt REAL,            -- 经营活动净收益/利润总额
                investincome_of_ebt REAL,        -- 价值变动净收益/利润总额
                n_op_profit_of_ebt REAL,         -- 营业外收支净额/利润总额
                tax_to_ebt REAL,                 -- 所得税/利润总额
                dtprofit_to_profit REAL,          -- 扣除非经常损益后的净利润/净利润
                salescash_to_or REAL,            -- 销售商品提供劳务收到的现金/营业收入
                ocf_to_or REAL,                  -- 经营活动产生的现金流量净额/营业收入
                ocf_to_opincome REAL,             -- 经营活动产生的现金流量净额/经营活动净收益
                capitalized_to_da REAL,          -- 资本支出/折旧和摊销
                debt_to_assets REAL,             -- 资产负债率
                assets_to_eqt REAL,               -- 权益乘数
                dp_assets_to_eqt REAL,            -- 产权比率
                ca_to_assets REAL,                -- 流动资产/总资产
                nca_to_assets REAL,               -- 非流动资产/总资产
                tbassets_to_totalassets REAL,     -- 有形资产/总资产
                int_to_talcap REAL,               -- 带息债务/全部投入资本
                eqt_to_talcapital REAL,           -- 归属于母公司股东权益/全部投入资本
                currentdebt_to_debt REAL,         -- 流动负债/负债合计
                longdeb_to_debt REAL,             -- 非流动负债/负债合计
                ocf_to_shortdebt REAL,            -- 经营活动产生的现金流量净额/流动负债
                debtturn_days REAL,               -- 负债合计周转天数
                or_yoy REAL,                     -- 营业收入同比增长率
                q_gr_yoy REAL,                    -- 营业总收入同比增长率
                q_gr_qoq REAL,                    -- 营业总收入环比增长率
                q_sales_yoy REAL,                 -- 营业收入同比增长率(单季度)
                q_sales_qoq REAL,                 -- 营业收入环比增长率(单季度)
                q_op_yoy REAL,                    -- 营业利润同比增长率
                q_op_qoq REAL,                    -- 营业利润环比增长率
                q_profit_yoy REAL,                -- 净利润同比增长率
                q_profit_qoq REAL,                -- 净利润环比增长率
                q_netprofit_yoy REAL,             -- 归属母公司股东的净利润同比增长率
                q_netprofit_qoq REAL,             -- 归属母公司股东的净利润环比增长率
                equity_yoy REAL,                  -- 净资产同比增长率
                rd_exp REAL,                      -- 研发费用
                update_flag TEXT,                 -- 更新标识
                created_at REAL NOT NULL,         -- 数据创建时间戳
                PRIMARY KEY (ts_code, end_date, ann_date)
            )
        ''')
        
        # 创建索引以提升查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fina_indicator_ts_code ON fina_indicator_data(ts_code)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fina_indicator_end_date ON fina_indicator_data(end_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fina_indicator_ann_date ON fina_indicator_data(ann_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fina_indicator_ts_code_end_date ON fina_indicator_data(ts_code, end_date)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fina_indicator_created_at ON fina_indicator_data(created_at)
        ''')
        
        self.conn.commit()
    
    def save_fina_indicator_data(self, df: pd.DataFrame) -> int:
        """
        保存财务指标数据到数据库
        
        参数:
            df: 包含财务指标数据的DataFrame，必须包含 ts_code 和 end_date 列
        
        返回:
            保存的记录数量
        """
        if df.empty:
            return 0
        
        # 确保必要的列存在
        required_columns = ['ts_code', 'end_date']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame必须包含 '{col}' 列")
        
        cursor = self.conn.cursor()
        current_time = time.time()
        saved_count = 0
        
        # 获取所有可能的列名
        all_columns = [
            'ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps', 'total_revenue_ps', 'revenue_ps',
            'capital_rese_ps', 'surplus_rese_ps', 'undist_profit_ps', 'extra_item', 'profit_dedt',
            'gross_margin', 'current_ratio', 'quick_ratio', 'cash_ratio', 'invturn_days', 'arturn_days',
            'inv_turn', 'ar_turn', 'ca_turn', 'fa_turn', 'assets_turn', 'op_income', 'valuechange_income',
            'interst_income', 'daa', 'ebit', 'ebitda', 'fcff', 'fcfe', 'current_exint', 'noncurrent_exint',
            'interestdebt', 'netdebt', 'tangible_asset', 'working_capital', 'networking_capital',
            'invest_capital', 'retained_earnings', 'diluted2_eps', 'bps', 'ocfps', 'retainedps', 'cfps',
            'ebit_ps', 'fcff_ps', 'fcfe_ps', 'netprofit_margin', 'grossprofit_margin', 'cost_of_sales',
            'profit_of_sales', 'roe', 'roa', 'roic', 'roe_yearly', 'roa2_yearly', 'roe_avg',
            'opincome_of_ebt', 'investincome_of_ebt', 'n_op_profit_of_ebt', 'tax_to_ebt',
            'dtprofit_to_profit', 'salescash_to_or', 'ocf_to_or', 'ocf_to_opincome', 'capitalized_to_da',
            'debt_to_assets', 'assets_to_eqt', 'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets',
            'tbassets_to_totalassets', 'int_to_talcap', 'eqt_to_talcapital', 'currentdebt_to_debt',
            'longdeb_to_debt', 'ocf_to_shortdebt', 'debtturn_days', 'or_yoy', 'q_gr_yoy', 'q_gr_qoq',
            'q_sales_yoy', 'q_sales_qoq', 'q_op_yoy', 'q_op_qoq', 'q_profit_yoy', 'q_profit_qoq',
            'q_netprofit_yoy', 'q_netprofit_qoq', 'equity_yoy', 'rd_exp', 'update_flag'
        ]
        
        # 准备插入或更新的数据
        for _, row in df.iterrows():
            try:
                # 构建列名和值的列表
                columns = ['ts_code', 'end_date', 'created_at']
                values = [
                    str(row.get('ts_code', '')),
                    str(row.get('end_date', '')),
                    current_time
                ]
                
                # 添加可选字段
                optional_fields = [col for col in all_columns if col not in ['ts_code', 'end_date']]
                for col in optional_fields:
                    if col in df.columns:
                        columns.append(col)
                        val = row.get(col)
                        if pd.notna(val):
                            values.append(val)
                        else:
                            values.append(None)
                    else:
                        columns.append(col)
                        values.append(None)
                
                # 构建INSERT OR REPLACE语句
                placeholders = ','.join(['?'] * len(columns))
                columns_str = ','.join(columns)
                
                cursor.execute(f'''
                    INSERT OR REPLACE INTO fina_indicator_data ({columns_str})
                    VALUES ({placeholders})
                ''', values)
                
                saved_count += 1
            except Exception as e:
                # 记录错误但继续处理其他记录
                print(f"保存财务指标数据时出错: {str(e)}", file=__import__('sys').stderr)
                continue
        
        self.conn.commit()
        return saved_count
    
    def get_fina_indicator_data(
        self,
        ts_code: Optional[str] = None,
        ann_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = 'DESC'
    ) -> Optional[pd.DataFrame]:
        """
        从数据库查询财务指标数据
        
        参数:
            ts_code: 股票代码（如：600000.SH）
            ann_date: 公告日期（YYYYMMDD格式）
            start_date: 报告期开始日期（YYYYMMDD格式），与end_date配合使用
            end_date: 报告期结束日期（YYYYMMDD格式），与start_date配合使用
            limit: 限制返回的记录数
            order_by: 排序方式，'DESC'（降序，最新的在前）或 'ASC'（升序）
        
        返回:
            DataFrame，如果未找到数据则返回None
        """
        cursor = self.conn.cursor()
        
        # 构建查询条件
        conditions = []
        params = []
        
        if ts_code:
            conditions.append('ts_code = ?')
            params.append(ts_code)
        
        if ann_date:
            conditions.append('ann_date = ?')
            params.append(ann_date)
        
        if start_date or end_date:
            if start_date:
                conditions.append('end_date >= ?')
                params.append(start_date)
            if end_date:
                conditions.append('end_date <= ?')
                params.append(end_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        order_clause = f'ORDER BY end_date {order_by}'
        limit_clause = f'LIMIT {limit}' if limit else ''
        
        query = f'''
            SELECT * FROM fina_indicator_data
            WHERE {where_clause}
            {order_clause}
            {limit_clause}
        '''
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if not rows:
            return None
        
        # 获取列名
        column_names = [description[0] for description in cursor.description]
        
        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
        # 转换数值类型
        numeric_columns = [col for col in df.columns if col not in ['ts_code', 'ann_date', 'end_date', 'update_flag', 'created_at']]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def has_data(self, ts_code: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        """
        检查是否存在指定股票的财务指标数据
        
        参数:
            ts_code: 股票代码（可选），如果提供则检查特定股票
            end_date: 报告期（可选），如果提供则检查特定报告期
        
        返回:
            如果存在数据返回True，否则返回False
        """
        cursor = self.conn.cursor()
        
        conditions = []
        params = []
        
        if ts_code:
            conditions.append('ts_code = ?')
            params.append(ts_code)
        
        if end_date:
            conditions.append('end_date = ?')
            params.append(end_date)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        
        cursor.execute(f'''
            SELECT COUNT(*) FROM fina_indicator_data
            WHERE {where_clause}
        ''', params)
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def get_date_range(self, ts_code: str) -> Optional[Dict[str, str]]:
        """
        获取指定股票的财务指标数据日期范围
        
        参数:
            ts_code: 股票代码
        
        返回:
            包含 'start_date' 和 'end_date' 的字典，如果无数据则返回None
        """
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT MIN(end_date) as start_date, MAX(end_date) as end_date
            FROM fina_indicator_data
            WHERE ts_code = ?
        ''', (ts_code,))
        
        row = cursor.fetchone()
        if row and row[0] and row[1]:
            return {
                'start_date': row[0],
                'end_date': row[1]
            }
        return None
    
    def get_stats(self) -> Dict:
        """获取财务指标数据统计信息"""
        cursor = self.conn.cursor()
        
        # 按股票代码统计
        cursor.execute('''
            SELECT 
                ts_code,
                COUNT(*) as record_count,
                MIN(end_date) as earliest_date,
                MAX(end_date) as latest_date
            FROM fina_indicator_data
            GROUP BY ts_code
            ORDER BY ts_code
        ''')
        
        stats = {
            'by_stock': {}
        }
        
        for row in cursor.fetchall():
            ts_code, count, earliest, latest = row
            stats['by_stock'][ts_code] = {
                'record_count': count,
                'earliest_date': earliest,
                'latest_date': latest
            }
        
        # 总统计
        cursor.execute('SELECT COUNT(*) FROM fina_indicator_data')
        total_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM fina_indicator_data')
        stock_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT end_date) FROM fina_indicator_data')
        date_count = cursor.fetchone()[0]
        
        stats['total'] = {
            'total_records': total_count,
            'stock_count': stock_count,
            'date_count': date_count
        }
        
        return stats
    
    def clear_fina_indicator_data(self, ts_code: Optional[str] = None) -> int:
        """
        清理财务指标数据
        
        参数:
            ts_code: 如果指定，只清理该股票的数据；否则清理所有数据
        
        返回:
            删除的记录数量
        """
        cursor = self.conn.cursor()
        
        if ts_code:
            cursor.execute('DELETE FROM fina_indicator_data WHERE ts_code = ?', (ts_code,))
        else:
            cursor.execute('DELETE FROM fina_indicator_data')
        
        count = cursor.rowcount
        self.conn.commit()
        return count
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


# 创建全局财务指标缓存管理器实例
fina_indicator_cache_manager = FinaIndicatorCacheManager()

