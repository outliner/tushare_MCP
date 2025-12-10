#!/bin/bash
# Tushare MCP Server HTTP SSE 模式启动脚本 (Linux/macOS)
#
# 使用方式:
#   chmod +x start_http_server.sh
#   ./start_http_server.sh

echo "========================================"
echo "Tushare MCP Server (HTTP SSE Mode)"
echo "========================================"
echo ""

# 检查 Python 是否安装
if ! command -v python3 &> /dev/null; then
    echo "[错误] 未找到 Python，请先安装 Python 3.9+"
    exit 1
fi

# 检查依赖是否安装
echo "[1/4] 检查依赖..."
if ! python3 -c "import mcp" &> /dev/null; then
    echo "[警告] 依赖未安装，正在安装..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "[错误] 依赖安装失败"
        exit 1
    fi
fi

# 检查端口占用
echo "[2/4] 检查端口占用 (8000)..."
if lsof -Pi :8000 -sTCP:LISTEN -t &> /dev/null; then
    echo "[警告] 端口 8000 已被占用！"
    echo ""
    
    # 获取占用端口的进程信息
    PID=$(lsof -Pi :8000 -sTCP:LISTEN -t)
    PROCESS_NAME=$(ps -p $PID -o comm=)
    
    echo "占用端口的进程信息:"
    echo "  PID: $PID"
    echo "  进程名: $PROCESS_NAME"
    echo ""
    
    lsof -Pi :8000 -sTCP:LISTEN
    echo ""
    
    echo "请选择操作:"
    echo "  1. 终止占用端口的进程并继续启动"
    echo "  2. 取消启动（手动处理）"
    echo ""
    
    read -p "请输入选择 (1 或 2): " CHOICE
    
    if [ "$CHOICE" = "1" ]; then
        echo ""
        echo "正在终止进程 PID: $PID ..."
        kill -9 $PID &> /dev/null
        
        if [ $? -eq 0 ]; then
            echo "[成功] 进程已终止"
            sleep 2
        else
            echo "[错误] 无法终止进程，可能需要 sudo 权限"
            echo "请使用 sudo 运行此脚本，或手动终止进程后重试:"
            echo "  sudo kill -9 $PID"
            exit 1
        fi
    else
        echo ""
        echo "[取消] 启动已取消"
        echo ""
        echo "您可以手动终止占用端口的进程:"
        echo "  kill -9 $PID"
        echo ""
        echo "或者修改 server_http.py 使用其他端口"
        exit 0
    fi
    echo ""
fi

# 检查 Tushare Token
echo "[3/4] 检查 Tushare Token..."
if ! python3 -c "from config.token_manager import get_tushare_token; token = get_tushare_token(); exit(0 if token else 1)" &> /dev/null; then
    echo "[警告] 未配置 Tushare Token"
    echo "请在 .env 文件中配置 TUSHARE_TOKEN"
    echo "或通过 /tools 接口查看配置说明"
    echo ""
fi

# 启动服务器
echo "[4/4] 启动 HTTP SSE 服务器..."
echo ""
echo "========================================"
echo "服务器信息:"
echo "  - SSE 端点:    http://127.0.0.1:8000/sse"
echo "  - 健康检查:    http://127.0.0.1:8000/health"
echo "  - 工具列表:    http://127.0.0.1:8000/tools"
echo "  - 消息端点:    http://127.0.0.1:8000/messages"
echo ""
echo "按 Ctrl+C 停止服务器"
echo "========================================"
echo ""

python3 server_http.py

