"""
测试 Streamable HTTP 模式的 MCP Server

该脚本用于测试 server_http.py 的功能
"""
import asyncio
import json
import httpx
import sys
from datetime import datetime

# Handle encoding on Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass


async def test_health_check():
    """测试健康检查端点"""
    print("\n" + "="*60)
    print("测试 1: 健康检查")
    print("="*60)
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get("http://127.0.0.1:8000/health", timeout=10.0)
            print(f"状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("响应内容:")
                print(json.dumps(data, indent=2, ensure_ascii=False))
                print("[PASS] 健康检查通过")
                return True
            else:
                print(f"[FAIL] 健康检查失败: {response.text}")
                return False
        
        except httpx.ConnectError:
            print("[FAIL] 无法连接到服务器")
            print("请确保已启动 HTTP 服务器:")
            print("  python server_http.py")
            return False
        
        except Exception as e:
            print(f"[FAIL] 测试失败: {str(e)}")
            return False


async def test_tools_list():
    """测试工具列表端点"""
    print("\n" + "="*60)
    print("测试 2: 获取工具列表")
    print("="*60)
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get("http://127.0.0.1:8000/tools", timeout=10.0)
            print(f"状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                tools_count = data.get("count", 0)
                print(f"工具总数: {tools_count}")
                
                if tools_count > 0:
                    print(f"\n前10个工具:")
                    for tool in data.get("tools", [])[:10]:
                        print(f"  - {tool['name']}")
                    
                    print("[PASS] 工具列表获取成功")
                    return True
                else:
                    print("[WARN] 未发现任何工具")
                    return False
            else:
                print(f"[FAIL] 获取工具列表失败: {response.text}")
                return False
        
        except Exception as e:
            print(f"[FAIL] 测试失败: {str(e)}")
            return False


async def test_mcp_endpoint():
    """测试 MCP 端点"""
    print("\n" + "="*60)
    print("测试 3: MCP 端点 (JSON-RPC)")
    print("="*60)
    
    async with httpx.AsyncClient() as client:
        try:
            # Test tools/list via JSON-RPC
            request_data = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {}
            }
            
            print(f"发送 JSON-RPC 请求: tools/list")
            
            # Streamable HTTP requires both application/json and text/event-stream in Accept header
            response = await client.post(
                "http://127.0.0.1:8000/mcp",
                json=request_data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                },
                timeout=30.0
            )
            
            print(f"状态码: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if "result" in data:
                        tools = data["result"].get("tools", [])
                        print(f"通过 MCP 获取到 {len(tools)} 个工具")
                        print("[PASS] MCP 端点工作正常")
                        return True
                    elif "error" in data:
                        print(f"[WARN] JSON-RPC 错误: {data['error']}")
                        return False
                except json.JSONDecodeError:
                    print(f"响应内容: {response.text[:200]}")
                    return True
            else:
                print(f"响应: {response.text[:200]}")
                return False
        
        except Exception as e:
            print(f"[FAIL] 测试失败: {str(e)}")
            return False


async def test_tool_call():
    """测试工具调用"""
    print("\n" + "="*60)
    print("测试 4: 工具调用 (get_stock_basic_info)")
    print("="*60)
    
    async with httpx.AsyncClient() as client:
        try:
            request_data = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "get_stock_basic_info",
                    "arguments": {"ts_code": "000001.SZ"}
                }
            }
            
            print(f"调用工具: get_stock_basic_info(ts_code='000001.SZ')")
            
            # Streamable HTTP requires both application/json and text/event-stream in Accept header
            response = await client.post(
                "http://127.0.0.1:8000/mcp",
                json=request_data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                },
                timeout=60.0
            )
            
            print(f"状态码: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if "result" in data:
                        content = data["result"].get("content", [])
                        if content:
                            text = content[0].get("text", "")[:300]
                            print(f"工具返回:\n{text}")
                        print("[PASS] 工具调用成功")
                        return True
                    elif "error" in data:
                        print(f"[WARN] 错误: {data['error']}")
                        return False
                except:
                    print(f"响应: {response.text[:300]}")
            return False
        
        except Exception as e:
            print(f"[FAIL] 测试失败: {str(e)}")
            return False


async def main():
    """运行所有测试"""
    print("\n" + "="*80)
    print(f"开始测试 Tushare MCP Server (Streamable HTTP)")
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    results = []
    
    # 测试 1: 健康检查
    result1 = await test_health_check()
    results.append(("健康检查", result1))
    
    if not result1:
        print("\n[WARN] 服务器未运行，后续测试跳过")
        print("请先启动服务器: python server_http.py")
        return
    
    # 测试 2: 工具列表
    result2 = await test_tools_list()
    results.append(("工具列表", result2))
    
    # 测试 3: MCP 端点
    result3 = await test_mcp_endpoint()
    results.append(("MCP端点", result3))
    
    # 测试 4: 工具调用
    result4 = await test_tool_call()
    results.append(("工具调用", result4))
    
    # 输出测试总结
    print("\n" + "="*80)
    print("测试总结")
    print("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status} - {name}")
    
    print(f"\n总计: {passed}/{total} 测试通过")
    
    if passed == total:
        print("\n所有测试通过！服务器运行正常。")
        print("\n下一步:")
        print("1. 在 Claude Desktop 配置文件中添加:")
        print('   {"url": "http://127.0.0.1:8000/mcp"}')
        print("2. 重启 Claude Desktop")
        print("3. 开始使用 Tushare MCP 工具")
    else:
        print("\n[WARN] 部分测试失败，请检查服务器日志")
    
    print("="*80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n测试被中断")
    except Exception as e:
        print(f"\n测试过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()

