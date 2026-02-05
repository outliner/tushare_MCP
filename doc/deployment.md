# Tushare MCP Server 部署与远程访问指南

本指南介绍如何将 Tushare MCP 服务共享给他人或通过反向代理发布。

## 1. 远程访问配置

默认情况下，服务器现在配置为监听 `0.0.0.0`，这意味着它可以接收来自局域网的请求。

- **获取你的局域网 IP**: 在 Windows 命令行运行 `ipconfig`，查找 `IPv4 地址` (例如 `192.168.1.5`)。
- **分享地址**: 你可以将 `http://192.168.1.5:8001/tools` 发送给局域网内的其他同事，他们即可在浏览器中查看工具列表。

> [!TIP]
> 如果想要修改监听地址或端口，可以修改 `.env` 文件添加：
> ```env
> MCP_HOST=0.0.0.0
> MCP_PORT=8001
> ```

## 2. Nginx 反向代理配置

如果你需要通过公网域名发布，请参考以下 Nginx 配置。**务必禁用代理缓冲**以支持 SSE 流。

```nginx
server {
    listen 80;
    server_name your.domain.com;

    location / {
        proxy_pass http://127.0.0.1:8001;
        
        # 必须：关闭代理缓冲
        proxy_buffering off;
        proxy_cache off;
        
        # 必须：传递必要的头部
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # 建议：增加超时时长
        proxy_read_timeout 600s;
        
        # 支持 SSE
        proxy_set_header Connection '';
        proxy_http_version 1.1;
    }
}
```

## 4. 使用 frp 进行公网代理 (针对阿里云等)

如果你想通过阿里云服务器公网访问本机的 MCP 服务，可以使用 frp 工具。

### 4.1 服务端配置 (阿里云 frps.toml)

如果您想使用 `8001` 端口，需要修改阿里云上的 `frps.toml`：

```toml
[common]
bind_port = 7000
# 修改 allow_ports 包含 8001
allow_ports = "6000-6010, 8001" 
vhost_http_port = 80
```

> [!IMPORTANT]
> 修改完 `frps.toml` 后，您需要：
> 1. 重启阿里云上的 `frps` 服务。
> 2. **最重要**: 在阿里云控制台的“安全组”中，新开一个入方向规则，允许 `8001` 端口。

### 4.2 客户端配置 (本地 frpc.toml)

根据您服务器端的 `vhost_http_port = 80` 配置，您有两种选择：

#### 方案 A: 使用子域名 (推荐)
如果您有域名并解析到了阿里云，可以使用这种方式，不需要加端口号访问。

```toml
[[proxies]]
name = "tushare-mcp-http"
type = "http"
localIP = "127.0.0.1"
localPort = 8001
customDomains = ["mcp.yourdomain.com"] # 替换为您的域名
```

#### 方案 B: 使用 TCP 代理 (简单)
直接使用端口号访问，不需要域名。

```toml
[[proxies]]
name = "tushare-mcp-tcp"
type = "tcp"
localIP = "127.0.0.1"
localPort = 8001
remotePort = 8001 # 与阿里云安全组开放端口一致
```

### 4.3 访问方式

- **方案 A**: `http://mcp.yourdomain.com/health`
- **方案 B**: `http://阿里云IP:8001/health`

> [!IMPORTANT]
> **关于 SSE 流支持**: 只要 Nginx 或 frp 没开启缓冲（Buffering），MCP 就能正常工作。您的 `frps` 配置非常标准，建议优先尝试方案 B 验证连通性。
