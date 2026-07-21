@echo off
chcp 65001 >nul
title 医院监护系统 - 停止

echo ╔════════════════════════════════════════════════╗
echo ║  停止所有服务                                  ║
echo ╚════════════════════════════════════════════════╝
echo.

cd /d D:\little\summer_all1\docker

echo 停止容器...
docker compose down -v 2>nul
echo ✓ 所有容器已停止
echo.
echo 要保留数据卷（不清除InfluxDB历史数据），去掉 -v 参数：
echo   docker compose down
echo.
pause
