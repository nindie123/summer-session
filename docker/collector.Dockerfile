FROM python:3.12-slim

WORKDIR /app

# 安装依赖
COPY data_collector/pyproject.toml /app/
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir "aiokafka>=0.10.0" "orjson>=3.9.0" "structlog>=24.1.0"

# 复制源代码
COPY data_collector/src/ /app/src/

EXPOSE 9001 8080

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

CMD ["python", "-m", "src.main"]
