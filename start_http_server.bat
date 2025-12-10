@echo off
setlocal enabledelayedexpansion
REM Tushare MCP Server HTTP SSE 模式启动脚本 (Windows)
REM 
REM 使用方式:
REM   1. 双击运行此脚本
REM   2. 或在命令行中执行: start_http_server.bat

echo ========================================
echo Tushare MCP Server (HTTP SSE Mode)
echo ========================================
echo.

REM 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Python，请先安装 Python 3.9+
    pause
    exit /b 1
)

REM 检查依赖是否安装
echo [1/4] 检查依赖...
python -c "import mcp" >nul 2>&1
if errorlevel 1 (
    echo [警告] 依赖未安装，正在安装...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [错误] 依赖安装失败
        pause
        exit /b 1
    )
)

REM 检查端口占用
echo [2/4] 检查端口占用 (8000)...
netstat -ano | findstr ":8000" | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo [警告] 端口 8000 已被占用！
    echo.
    
    REM 获取占用端口的进程ID
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
        set PID=%%a
        goto :found_pid
    )
    
    :found_pid
    echo 占用端口的进程 PID: !PID!
    
    REM 尝试获取进程名称
    for /f "tokens=1" %%b in ('tasklist /FI "PID eq !PID!" /NH') do (
        echo 进程名称: %%b
        set PROCESS_NAME=%%b
    )
    
    echo.
    echo 请选择操作:
    echo   1. 终止占用端口的进程并继续启动
    echo   2. 取消启动（手动处理）
    echo.
    
    set /p CHOICE="请输入选择 (1 或 2): "
    
    if "!CHOICE!"=="1" (
        echo.
        echo 正在终止进程 PID: !PID! ...
        taskkill /PID !PID! /F >nul 2>&1
        if errorlevel 1 (
            echo [错误] 无法终止进程，可能需要管理员权限
            echo 请以管理员身份运行此脚本，或手动终止进程后重试
            pause
            exit /b 1
        ) else (
            echo [成功] 进程已终止
            timeout /t 2 >nul
        )
    ) else (
        echo.
        echo [取消] 启动已取消
        echo.
        echo 您可以手动终止占用端口的进程:
        echo   taskkill /PID !PID! /F
        echo.
        echo 或者修改 server_http.py 使用其他端口
        pause
        exit /b 0
    )
    echo.
)

REM 检查 Tushare Token
echo [3/4] 检查 Tushare Token...
python -c "from config.token_manager import get_tushare_token; token = get_tushare_token(); exit(0 if token else 1)" >nul 2>&1
if errorlevel 1 (
    echo [警告] 未配置 Tushare Token
    echo 请在 .env 文件中配置 TUSHARE_TOKEN
    echo 或通过 /tools 接口查看配置说明
    echo.
)

REM 启动服务器
echo [4/4] 启动 Streamable HTTP 服务器...
echo.
echo ========================================
echo 服务器信息:
echo   - MCP 端点:    http://127.0.0.1:8000/mcp
echo   - 健康检查:    http://127.0.0.1:8000/health
echo   - 工具列表:    http://127.0.0.1:8000/tools
echo.
echo 按 Ctrl+C 停止服务器
echo ========================================
echo.

python server_http.py

REM 如果服务器异常退出
if errorlevel 1 (
    echo.
    echo [错误] 服务器启动失败
    pause
)

