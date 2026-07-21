"""健康检查器 — 定时扫描连接活性。"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from src.server.conn_manager import ConnectionManager
from src.state.device_tracker import DeviceTracker
from src.observability.logger import get_logger

logger = get_logger(__name__)


class HealthChecker:
    """定时检查所有设备连接活性。

    长时间无数据的连接将被标记为可疑。
    """

    def __init__(
        self,
        conn_manager: ConnectionManager,
        device_tracker: DeviceTracker,
        idle_threshold_sec: int = 30,
        check_interval_sec: int = 10,
    ) -> None:
        self.conn_manager = conn_manager
        self.device_tracker = device_tracker
        self.idle_threshold_sec = idle_threshold_sec
        self.check_interval_sec = check_interval_sec
        self._task: Optional[asyncio.Task[None]] = None
        self._running = False

    async def start(self) -> None:
        """启动健康检查循环。"""
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info("health_checker_started", extra={
            "check_interval_sec": self.check_interval_sec,
        })

    async def stop(self) -> None:
        """停止健康检查。"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _check_loop(self) -> None:
        """定时检查循环。"""
        while self._running:
            try:
                await asyncio.sleep(self.check_interval_sec)
                self._check_idle_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("health_check_error", extra={"error": str(e)})

    def _check_idle_connections(self) -> None:
        """检查空闲连接。"""
        now = datetime.now(timezone.utc)
        threshold_seconds = self.idle_threshold_sec

        for conn in self.conn_manager.get_all():
            try:
                last_active = datetime.fromisoformat(
                    conn.last_activity_at.replace("Z", "+00:00")
                )
                idle_sec = (now - last_active).total_seconds()

                if idle_sec > threshold_seconds:
                    logger.warning("device_idle", extra={
                        "device_id": conn.device_id,
                        "patient_id": conn.patient_id,
                        "idle_seconds": round(idle_sec, 1),
                    })
            except (ValueError, TypeError):
                pass
