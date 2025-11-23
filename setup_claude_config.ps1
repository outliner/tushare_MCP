# Tushare MCP Server - Claude Desktop 配置脚本
# 此脚本帮助您自动配置 Claude Desktop 以使用 Tushare MCP Server

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Tushare MCP Server - Claude Desktop 配置" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 获取当前脚本所在目录（项目根目录）
$projectPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$serverScript = Join-Path $projectPath "server.py"

# 检查 server.py 是否存在
if (-not (Test-Path $serverScript)) {
    Write-Host "错误: 找不到 server.py 文件！" -ForegroundColor Red
    Write-Host "请确保在项目根目录运行此脚本。" -ForegroundColor Red
    exit 1
}

# 获取 Python 路径
Write-Host "正在检测 Python 路径..." -ForegroundColor Yellow
$pythonPath = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $pythonPath) {
    $pythonPath = (Get-Command python3 -ErrorAction SilentlyContinue).Source
}
if (-not $pythonPath) {
    Write-Host "错误: 找不到 Python！请确保 Python 已安装并在 PATH 中。" -ForegroundColor Red
    exit 1
}
Write-Host "找到 Python: $pythonPath" -ForegroundColor Green

# 确定 Claude Desktop 配置目录
$claudeConfigDir = Join-Path $env:APPDATA "Claude"
$claudeConfigFile = Join-Path $claudeConfigDir "claude_desktop_config.json"

Write-Host ""
Write-Host "Claude Desktop 配置文件位置: $claudeConfigFile" -ForegroundColor Yellow

# 确保配置目录存在
if (-not (Test-Path $claudeConfigDir)) {
    Write-Host "创建 Claude 配置目录..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $claudeConfigDir -Force | Out-Null
}

# 读取现有配置（如果存在）
$configJson = "{}"
if (Test-Path $claudeConfigFile) {
    Write-Host "读取现有配置文件..." -ForegroundColor Yellow
    try {
        $configJson = Get-Content $claudeConfigFile -Raw
        # 验证 JSON 格式
        $null = $configJson | ConvertFrom-Json
    } catch {
        Write-Host "警告: 无法解析现有配置文件，将创建新配置。" -ForegroundColor Yellow
        $configJson = "{}"
    }
}

# 解析 JSON 为 Hashtable
$config = $configJson | ConvertFrom-Json | ConvertTo-Json -Depth 10 | ConvertFrom-Json

# 转换为 Hashtable 以便修改
$configHash = @{}
$config.PSObject.Properties | ForEach-Object {
    $configHash[$_.Name] = $_.Value
}

# 初始化 mcpServers
if (-not $configHash.mcpServers) {
    $configHash["mcpServers"] = @{}
} else {
    # 如果 mcpServers 是 PSCustomObject，转换为 Hashtable
    if ($configHash.mcpServers -is [PSCustomObject]) {
        $mcpServersHash = @{}
        $configHash.mcpServers.PSObject.Properties | ForEach-Object {
            $mcpServersHash[$_.Name] = $_.Value
        }
        $configHash["mcpServers"] = $mcpServersHash
    }
}

# 配置 Tushare MCP Server
# 将路径中的反斜杠转换为双反斜杠（JSON 转义）
$serverScriptEscaped = $serverScript -replace '\\', '\\'

$tushareConfig = @{
    command = "python"
    args = @($serverScriptEscaped)
}

# 添加 tushare 配置
$configHash["mcpServers"]["tushare"] = $tushareConfig

# 保存配置
Write-Host ""
Write-Host "正在保存配置..." -ForegroundColor Yellow
try {
    $configHash | ConvertTo-Json -Depth 10 | Set-Content $claudeConfigFile -Encoding UTF8
    Write-Host "配置已成功保存！" -ForegroundColor Green
} catch {
    Write-Host "错误: 无法保存配置文件: $_" -ForegroundColor Red
    exit 1
}

# 显示配置摘要
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "配置摘要" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Python 路径: $pythonPath" -ForegroundColor White
Write-Host "Server 脚本: $serverScript" -ForegroundColor White
Write-Host "配置文件: $claudeConfigFile" -ForegroundColor White
Write-Host ""

# 检查 .env 文件
$envFile = Join-Path $projectPath ".env"
if (Test-Path $envFile) {
    Write-Host "✓ 找到 .env 文件，Token 将自动加载" -ForegroundColor Green
} else {
    Write-Host "⚠ 未找到 .env 文件" -ForegroundColor Yellow
    Write-Host "  请在项目根目录创建 .env 文件并添加:" -ForegroundColor Yellow
    Write-Host "  TUSHARE_TOKEN=your_token_here" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "下一步操作:" -ForegroundColor Cyan
Write-Host "1. 确保已在项目根目录创建 .env 文件并配置了 TUSHARE_TOKEN" -ForegroundColor White
Write-Host "2. 完全关闭 Claude Desktop 应用" -ForegroundColor White
Write-Host "3. 重新启动 Claude Desktop" -ForegroundColor White
Write-Host "4. 在 Claude 中尝试: '请检查 token 状态'" -ForegroundColor White
Write-Host ""

