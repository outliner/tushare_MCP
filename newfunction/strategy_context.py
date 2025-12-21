"""
策略上下文管理器 (StrategyContext)
- 管理市场状态 (Regime)
- 集中管理所有策略参数阈值
- 支持熊市(0)、震荡市(1)、牛市(2)三种模式

第一性原理：
- 震荡市：低波动、高分歧，需要防御潜伏，拒绝追高
- 牛市：高动量、低分歧，可以追涨
- 熊市：下跌趋势，需要极度保守
"""
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class StrategyContext:
    """
    策略上下文管理器
    
    Attributes:
        regime: 市场状态
            - 0: 熊市 (Bear)
            - 1: 震荡市 (Sideways) [当前默认]
            - 2: 牛市 (Bull)
    """
    regime: int = 1  # 默认震荡市
    
    # 参数字典，根据 regime 动态调整
    params: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """根据 regime 初始化参数"""
        self._init_params()
    
    def _init_params(self):
        """根据市场状态初始化策略参数"""
        if self.regime == 0:
            # 熊市参数：极度保守
            self.params = {
                # === Phase 2: 日线漏斗筛选 ===
                'pct_chg_min': 0.0,           # 涨幅下限
                'pct_chg_max': 2.5,           # 涨幅上限 (极度保守)
                'bias_min': -0.005,           # VWAP偏离下限
                'bias_max': 0.010,            # VWAP偏离上限
                'high_close_dist_max': 0.005, # 高收盘价距离上限
                'ats_ratio_min': 1.5,         # 大单异动下限 (要求更高)
                'turnover_max': 5.0,          # 换手率上限 (极度限制)
                
                # 市值分层换手率阈值
                'turnover_large_cap': 4.0,    # >200亿
                'turnover_mid_cap': 6.0,      # 50-200亿
                'turnover_small_cap': 8.0,    # <50亿
                
                # 爆发保护
                'vol_ratio_danger': 2.0,      # 危险量比阈值
                'turnover_danger': 5.0,       # 危险换手率阈值
                
                # 筹码分级
                'chip_tier1_winner_rate': 85, # Tier1胜率阈值
                'chip_tier2_winner_rate': 70, # Tier2胜率阈值
                'chip_tier2_concentration': 0.10,  # Tier2集中度阈值
                
                # === Phase 3: 15分钟线体检 ===
                'upper_shadow_max': 0.008,    # 上影线最大比例
                'tail_vol_ratio_min': 0.5,    # 尾盘成交量占比下限
                'close_to_high_ratio': 0.99,  # 收盘价接近最高价比例
                
                # 评分权重
                'score_weight_efficiency': 0.3,
                'score_weight_winner_rate': 0.5,
                'score_weight_ats_ratio': 0.2,
                
                # 输出控制
                'top_n_candidates': 15,
                'top_n_output': 6,
            }
            
        elif self.regime == 1:
            # 震荡市参数：防御潜伏型
            self.params = {
                # === Phase 2: 日线漏斗筛选 ===
                'pct_chg_min': 0.5,           # 涨幅下限 (拒绝死鱼)
                'pct_chg_max': 4.2,           # 涨幅上限 (拒绝追高，吃鱼身)
                'bias_min': -0.002,           # VWAP偏离下限 (允许微弱负溢价，主力打压吸筹)
                'bias_max': 0.015,            # VWAP偏离上限 (成本控制)
                'high_close_dist_max': 0.006, # 高收盘价距离上限 (更严格)
                'ats_ratio_min': 1.3,         # 大单异动下限 (主力必须在场)
                'turnover_max': 8.0,          # 换手率上限 (拒绝过热)
                
                # 市值分层换手率阈值 (震荡市全面收紧)
                'turnover_large_cap': 5.0,    # >200亿
                'turnover_mid_cap': 8.0,      # 50-200亿
                'turnover_small_cap': 12.0,   # <50亿
                
                # 爆发保护 (震荡市更严格)
                'vol_ratio_danger': 2.5,      # 危险量比阈值
                'turnover_danger': 6.0,       # 危险换手率阈值
                
                # 筹码分级 (震荡市更看重安全性)
                'chip_tier1_winner_rate': 75, # Tier1胜率阈值 (略放宽，增加候选)
                'chip_tier2_winner_rate': 55, # Tier2胜率阈值
                'chip_tier2_concentration': 0.15,  # Tier2集中度阈值
                
                # === Phase 3: 15分钟线体检 ===
                'upper_shadow_max': 0.008,    # 上影线最大比例
                'tail_vol_ratio_min': 0.6,    # 尾盘成交量占比下限 (拒绝死鱼)
                'close_to_high_ratio': 0.98,  # 收盘价接近最高价比例 (日内高点确认)
                
                # 评分权重 (震荡市更看重安全性)
                'score_weight_efficiency': 0.4,
                'score_weight_winner_rate': 0.4,
                'score_weight_ats_ratio': 0.2,
                
                # 输出控制
                'top_n_candidates': 15,
                'top_n_output': 9,
            }
            
        else:  # regime == 2
            # 牛市参数：进攻型（原策略）
            self.params = {
                # === Phase 2: 日线漏斗筛选 ===
                'pct_chg_min': 3.0,           # 涨幅下限
                'pct_chg_max': 8.5,           # 涨幅上限
                'bias_min': 0.005,            # VWAP偏离下限
                'bias_max': 0.05,             # VWAP偏离上限
                'high_close_dist_max': 0.008, # 高收盘价距离上限
                'ats_ratio_min': 1.2,         # 大单异动下限
                'turnover_max': 20.0,         # 换手率上限
                
                # 市值分层换手率阈值 (牛市放宽)
                'turnover_large_cap': 8.0,    # >200亿
                'turnover_mid_cap': 15.0,     # 50-200亿
                'turnover_small_cap': 20.0,   # <50亿
                
                # 爆发保护 (牛市放宽)
                'vol_ratio_danger': 3.0,      # 危险量比阈值
                'turnover_danger': 10.0,      # 危险换手率阈值
                
                # 筹码分级
                'chip_tier1_winner_rate': 80, # Tier1胜率阈值
                'chip_tier2_winner_rate': 60, # Tier2胜率阈值
                'chip_tier2_concentration': 0.12,  # Tier2集中度阈值
                
                # === Phase 3: 15分钟线体检 ===
                'upper_shadow_max': 0.01,     # 上影线最大比例
                'tail_vol_ratio_min': 0.4,    # 尾盘成交量占比下限
                'close_to_high_ratio': 0.97,  # 收盘价接近最高价比例
                
                # 评分权重 (牛市更看重进攻性)
                'score_weight_efficiency': 0.5,
                'score_weight_winner_rate': 0.3,
                'score_weight_ats_ratio': 0.2,
                
                # 输出控制
                'top_n_candidates': 15,
                'top_n_output': 9,
            }
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取参数值"""
        return self.params.get(key, default)
    
    def set_regime(self, regime: int):
        """切换市场状态并重新初始化参数"""
        if regime not in [0, 1, 2]:
            raise ValueError(f"Invalid regime: {regime}. Must be 0, 1, or 2.")
        self.regime = regime
        self._init_params()
    
    @property
    def regime_name(self) -> str:
        """获取当前市场状态名称"""
        names = {0: '熊市(Bear)', 1: '震荡市(Sideways)', 2: '牛市(Bull)'}
        return names.get(self.regime, 'Unknown')
    
    def describe(self) -> str:
        """打印当前策略参数"""
        lines = [
            f"\n{'='*60}",
            f" StrategyContext | 当前模式: {self.regime_name}",
            f"{'='*60}",
            f"\n[Phase 2 - 日线漏斗参数]",
            f"  涨幅区间: {self.get('pct_chg_min'):.1f}% ~ {self.get('pct_chg_max'):.1f}%",
            f"  VWAP偏离: {self.get('bias_min'):.3f} ~ {self.get('bias_max'):.3f}",
            f"  高收盘距离上限: {self.get('high_close_dist_max'):.3f}",
            f"  大单异动下限: {self.get('ats_ratio_min'):.1f}",
            f"  换手率上限: {self.get('turnover_max'):.1f}%",
            f"\n[市值分层换手率]",
            f"  >200亿: {self.get('turnover_large_cap'):.1f}%",
            f"  50-200亿: {self.get('turnover_mid_cap'):.1f}%",
            f"  <50亿: {self.get('turnover_small_cap'):.1f}%",
            f"\n[Phase 3 - 15分钟线体检参数]",
            f"  上影线上限: {self.get('upper_shadow_max'):.3f}",
            f"  尾盘量比下限: {self.get('tail_vol_ratio_min'):.1f}",
            f"  收盘接近高点比例: {self.get('close_to_high_ratio'):.2f}",
            f"\n[评分权重]",
            f"  效率: {self.get('score_weight_efficiency'):.1%}",
            f"  胜率: {self.get('score_weight_winner_rate'):.1%}",
            f"  大单异动: {self.get('score_weight_ats_ratio'):.1%}",
            f"{'='*60}",
        ]
        return '\n'.join(lines)


# 全局默认上下文 (震荡市)
default_context = StrategyContext(regime=1)


if __name__ == '__main__':
    # 测试三种模式
    for r in [0, 1, 2]:
        ctx = StrategyContext(regime=r)
        print(ctx.describe())

