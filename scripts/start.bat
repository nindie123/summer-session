@echo off
chcp 65001 >nul
title 医院监护系统 - 一键启动

echo ╔════════════════════════════════════════════════╗
echo ║  医院实时生命体征监护系统 - 一键启动           ║
echo ║  L1(模拟器) → L2(采集层) → L3(Flink) → API   ║
echo ╚════════════════════════════════════════════════╝
echo.

cd /d D:\little\summer_all1\docker

echo [1/4] 启动基础设施 (Kafka + InfluxDB + Redis)...
docker compose up -d zookeeper kafka 2>nul
echo 等待 Kafka 就绪...
timeout /t 15 /nobreak >nul

docker compose up -d redis influxdb kafka-init 2>nul
echo ✓ 基础设施就绪
echo.

echo [2/4] 启动采集层和API网关...
docker compose up -d collector api-gateway 2>nul
echo ✓ 采集层 (:9001)
echo ✓ API网关 (:8000)
echo.

echo [3/4] 启动 Flink 实时计算...
docker compose up -d jobmanager taskmanager 2>nul
echo 等待 Flink 就绪...
timeout /t 15 /nobreak >nul
docker exec docker-jobmanager-1 sh -c "flink run /opt/flink/jobs/flink-computation-1.0.0.jar" 2>nul
timeout /t 3 /nobreak >nul
echo ✓ Flink 作业已提交
echo.

echo [4/4] 发送测试数据...
docker compose run --rm device-simulator 2>nul
timeout /t 8 /nobreak >nul
echo ✓ 测试数据已发送
echo.

echo ╔════════════════════════════════════════════════╗
echo ║  ✅ 全链路启动完成!                             ║
echo ╚════════════════════════════════════════════════╝
echo.
echo   访问入口:
echo   ────────────────────────────────────
echo   API 文档:     http://localhost:8000/docs
echo   病区概览:     http://localhost:8000/api/v1/wards/ICU-EAST/overview
echo   P0001 体征:   http://localhost:8000/api/v1/patients/P0001/vitals
echo   P0001 评分:   http://localhost:8000/api/v1/patients/P0001/mews
echo   Flink 监控:   http://localhost:8081
echo.
pause
