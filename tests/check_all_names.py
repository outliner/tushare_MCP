"""快速检查所有行业板块的名称情况"""
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
from tools.concept_tools import get_dc_board_codes

init_env_file()
token = get_tushare_token()
if token:
    ts.set_token(token)

result = get_dc_board_codes(board_type='行业板块')
has_name = sum(1 for item in result if item['name'] != item['ts_code'])
no_name = len(result) - has_name

print(f'总板块数: {len(result)}')
print(f'有名称的板块: {has_name}')
print(f'无名称的板块: {no_name}')

if no_name > 0:
    print('\n无名称的板块示例（前10个）:')
    no_name_list = [item for item in result if item['name'] == item['ts_code']][:10]
    for item in no_name_list:
        print(f"  {item['ts_code']}")
else:
    print('\n[OK] 所有板块都有名称！')

print('\n有名称的板块示例（前10个）:')
has_name_list = [item for item in result if item['name'] != item['ts_code']][:10]
for item in has_name_list:
    print(f"  {item['ts_code']} -> {item['name']}")

