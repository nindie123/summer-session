FROM python:3.12-slim

WORKDIR /app

# 安装依赖
COPY scripts/test_device.py /app/test_device.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "test_device.py"]
