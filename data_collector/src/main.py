"""采集层入口。"""

import asyncio
import os
import sys
from typing import Optional

from src.server.tcp_server import TcpServer
from src.state.health_checker import HealthChecker
from src.observability.logger import get_logger

logger = get_logger(__name__)


async def main() -> None:
    """启动采集层服务。"""
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    server = TcpServer(
        host="0.0.0.0",
        port=9001,
        max_connections=500,
        kafka_bootstrap_servers=kafka_bootstrap,
    )

    health_checker = HealthChecker(
        conn_manager=server.conn_manager,
        device_tracker=server.device_tracker,
        idle_threshold_sec=30,
        check_interval_sec=10,
    )

    # 尝试启动 Kafka（允许失败，降级运行）
    try:
        await server.router.start()
        print("[COLLECTOR] Kafka producer connected.")
    except Exception as e:
        print(f"[COLLECTOR] Kafka not available, running without it: {e}")
        logger.warning("kafka_unavailable", error=str(e))

    # 启动健康检查
    await health_checker.start()

    # 启动 TCP 服务器
    server_task = asyncio.create_task(server.start())

    print("[COLLECTOR] Data collector started. Press Ctrl+C to stop.")

    # 优雅关闭（兼容 Windows）
    stop_event = asyncio.Event()

    def shutdown() -> None:
        if not stop_event.is_set():
            logger.info("shutdown_initiated")
            stop_event.set()

    if sys.platform != "win32":
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown)
            except NotImplementedError:
                pass
    else:
        # Windows: 用 asyncio 的 Event 配合 Ctrl+C 中断
        print("[COLLECTOR] Press Ctrl+C to stop.")

    # 等待 shutdown 信号或 Ctrl+C
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        shutdown()

    print("[COLLECTOR] Shutting down...")
    await server.stop()
    await health_checker.stop()
    print("[COLLECTOR] Shutdown complete.")


if __name__ == "__main__":
    # 导入 signal 仅在非 Windows 平台
    if sys.platform != "win32":
        import signal
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[COLLECTOR] Interrupted by user.")
