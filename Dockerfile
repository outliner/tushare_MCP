FROM python:3.10-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libffi-dev \
    libc-dev \
    make \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制requirements.txt
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 安装 supervisor 进程管理器
RUN pip install --no-cache-dir supervisor

# 复制其余项目文件
COPY . .

# 创建日志目录和缓存目录
RUN mkdir -p /app/logs /app/.cache

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 暴露 HTTP 端口
EXPOSE 8000

# 使用 supervisord 启动所有服务
CMD ["supervisord", "-c", "supervisord.conf"]