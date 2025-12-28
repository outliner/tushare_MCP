"""测试板块名称优化效果"""
import sys
import io
from pathlib import Path

# 设置输出编码为UTF-8（Windows兼容）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.token_manager import get_tushare_token, init_env_file
import tushare as ts

init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)

# 测试 get_dc_board_codes 返回格式
from tools.concept_tools import get_dc_board_codes

print('=== 测试 get_dc_board_codes 返回格式 ===')
board_list = get_dc_board_codes(board_type='行业板块')
print(f'获取到 {len(board_list)} 个行业板块')
print('前5个板块:')
for item in board_list[:5]:
    print(f"  {item['ts_code']} -> {item['name']}")

print()
print('=== 验证名称映射可用 ===')
board_name_map = {item['ts_code']: item['name'] for item in board_list}
print(f'名称映射包含 {len(board_name_map)} 个条目')
print('示例映射:')
for code, name in list(board_name_map.items())[:5]:
    print(f"  {code} => {name}")

print()
print('[OK] 板块名称优化功能验证完成')

