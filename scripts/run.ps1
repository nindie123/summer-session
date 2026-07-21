#!/usr/bin/env pwsh
# summer_all1 - 一键启动全链路
# 医院实时生命体征监护系统
# L1(模拟器) → L2(采集层) → L3(Flink计算) → API查询

param(
    [switch]$NoTest,       # 跳过发送测试数据
    [switch]$NoFlink       # 不启动 Flink
)

$ErrorActionPreference = "Stop"
$PROJECT_DIR = "D:\little\summer_all1"
$DOCKER_DIR = "$PROJECT_DIR\docker"

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║    医院实时生命体征监护系统 - 一键启动                    ║" -ForegroundColor Cyan
Write-Host "║    L1(模拟器) → L2(采集层) → L3(Flink) → API查询       ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ── 1. 清理旧环境 ───────────────────────────────────
Write-Host "[1/6] 清理旧容器..." -ForegroundColor Yellow
cd $DOCKER_DIR
docker compose down -v 2>$null
Write-Host "  ✅ 清理完成"

# ── 2. 构建镜像 ─────────────────────────────────────
Write-Host "[2/6] 构建镜像..." -ForegroundColor Yellow
docker compose build collector 2>&1 | Out-Null
Write-Host "  ✅ 采集层镜像"

if (-not $NoFlink) {
    docker compose build flink-job 2>&1 | Out-Null
    Write-Host "  ✅ Flink 作业镜像"
}

docker compose build api-gateway 2>&1 | Out-Null
Write-Host "  ✅ API 网关镜像"

# ── 3. 启动基础设施 ────────────────────────────────
Write-Host "[3/6] 启动基础设施 (Kafka + InfluxDB + Redis)..." -ForegroundColor Yellow
docker compose up -d zookeeper kafka 2>&1 | Out-Null
Write-Host "  等待 Kafka 就绪..."
Start-Sleep -Seconds 15

docker compose up -d redis influxdb kafka-init 2>&1 | Out-Null
Write-Host "  ✅ 基础设施就绪"

# ── 4. 启动核心服务 ────────────────────────────────
Write-Host "[4/6] 启动采集层 + API 网关..." -ForegroundColor Yellow
docker compose up -d collector api-gateway 2>&1 | Out-Null
Write-Host "  ✅ 采集层 (TCP :9001)"
Write-Host "  ✅ API 网关 (:8000)"

# ── 5. 启动 Flink ──────────────────────────────────
if (-not $NoFlink) {
    Write-Host "[5/6] 启动 Flink 实时计算层..." -ForegroundColor Yellow
    docker compose up -d jobmanager taskmanager 2>&1 | Out-Null
    Write-Host "  等待 Flink JobManager..."
    Start-Sleep -Seconds 15

    Write-Host "  提交 Flink 作业..."
    docker exec docker-jobmanager-1 sh -c "flink run /opt/flink/jobs/flink-computation-1.0.0.jar" 2>&1 | Out-Null
    Start-Sleep -Seconds 3
    Write-Host "  ✅ Flink 实时计算已启动"
}

# ── 6. 发送测试数据 ────────────────────────────────
if (-not $NoTest) {
    Write-Host "[6/6] 发送测试数据..." -ForegroundColor Yellow
    docker compose run --rm device-simulator 2>&1 | Out-Null
    Start-Sleep -Seconds 10
    Write-Host "  ✅ 测试数据已发送"
}

# ── 完成 ───────────────────────────────────────────
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║  ✅ 全链路启动完成！                                      ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  📋 访问入口:" -ForegroundColor Cyan
Write-Host "  ─────────────────────────────────────────────"
Write-Host "  API 文档:       http://localhost:8000/docs" -ForegroundColor White
Write-Host "  病区概览:       http://localhost:8000/api/v1/wards/ICU-EAST/overview" -ForegroundColor White
Write-Host "  P0001 体征:     http://localhost:8000/api/v1/patients/P0001/vitals" -ForegroundColor White
Write-Host "  P0001 MEWS:     http://localhost:8000/api/v1/patients/P0001/mews" -ForegroundColor White
Write-Host "  Flink 监控:     http://localhost:8081" -ForegroundColor White
Write-Host ""

Write-Host "  📦 容器状态:" -ForegroundColor Cyan
docker ps --format "table {{.Names}}\t{{.Status}}" 2>$null
Write-Host ""

Write-Host "  再次发送测试数据:  docker compose -f $DOCKER_DIR\docker-compose.yml run --rm device-simulator" -ForegroundColor Gray
Write-Host "  停止项目:          docker compose -f $DOCKER_DIR\docker-compose.yml down" -ForegroundColor Gray
Write-Host ""
