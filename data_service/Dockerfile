FROM python:3.10-slim

WORKDIR /app

# 安装依赖
COPY data_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝数据服务代码
COPY data_service/ data_service/

# 创建数据目录
RUN mkdir -p /app/data

# 环境变量
ENV PYTHONUNBUFFERED=1
ENV DATA_SERVICE_DB_PATH=/app/data/cache.db
ENV DATA_SERVICE_PORT=8001

EXPOSE 8001

CMD ["uvicorn", "data_service.app:app", "--host", "0.0.0.0", "--port", "8001"]
