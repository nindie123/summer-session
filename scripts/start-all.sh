#!/bin/bash
# —————————————————————————————————————
# 启动全部服务（开发环境）
# —————————————————————————————————————
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Starting Hospital Vital Signs Monitor ==="
echo ""

# 1. 启动基础设施 (Kafka + InfluxDB + Redis)
echo "[1/5] Starting infrastructure (Kafka, InfluxDB, Redis)..."
cd "$PROJECT_DIR"
docker-compose -f docker/docker-compose.yml up -d kafka influxdb redis
echo "  Waiting for Kafka..."
sleep 15
echo "  Done."

# 2. 初始化 Kafka Topic
echo "[2/5] Initializing Kafka topics..."
docker-compose -f docker/docker-compose.yml up kafka-init
echo "  Done."

# 3. 启动采集层
echo "[3/5] Starting data collector..."
cd "$PROJECT_DIR/data_collector"
pip install -q -e ".[dev]"
python src/main.py &
COLLECTOR_PID=$!
echo "  Collector PID: $COLLECTOR_PID"
sleep 2

# 4. 启动 API 网关
echo "[4/5] Starting API gateway..."
cd "$PROJECT_DIR/api_gateway"
pip install -q -e ".[dev]"
uvicorn src.main:app --host 0.0.0.0 --port 8000 &
API_PID=$!
echo "  API Gateway PID: $API_PID"
sleep 2

# 5. 启动设备模拟器（需预先编译）
echo "[5/5] Starting device simulator..."
cd "$PROJECT_DIR/device_simulator"
if [ -f "build/bin/simulator" ]; then
    ./build/bin/simulator config/devices.json &
    SIM_PID=$!
    echo "  Simulator PID: $SIM_PID"
else
    echo "  [WARN] Simulator binary not found. Build it first:"
    echo "    cd device_simulator && mkdir build && cd build && cmake .. && make"
fi

echo ""
echo "=== All services started ==="
echo ""
echo "  TCP Collector:  localhost:9001"
echo "  API Gateway:    http://localhost:8000"
echo "  API Docs:       http://localhost:8000/docs"
echo "  Kafka:          localhost:9092"
echo "  InfluxDB:       http://localhost:8086"
echo ""
echo "Press Ctrl+C to stop all services."

# 捕获 SIGINT，统一关闭
trap "echo 'Shutting down...'; kill $COLLECTOR_PID $API_PID $SIM_PID 2>/dev/null; echo 'Done.'" SIGINT
wait
