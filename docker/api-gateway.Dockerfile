FROM python:3.12-slim

WORKDIR /app

# 安装依赖
COPY api_gateway/pyproject.toml /app/
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir "fastapi>=0.110.0" "uvicorn[standard]>=0.29.0" "influxdb-client>=1.44.0" "orjson>=3.9.0"

# 复制源代码
COPY api_gateway/src/ /app/src/

EXPOSE 8000

ENV INFLUXDB_URL=http://influxdb:8086
ENV INFLUXDB_TOKEN=admin-token
ENV INFLUXDB_ORG=hospital
ENV INFLUXDB_BUCKET=vitals

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
