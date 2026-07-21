"""连接管理器 — 跟踪所有设备连接状态。"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import uuid


@dataclass
class ConnectionInfo:
    """连接信息。"""
    conn_id: str
    device_id: str
    patient_id: str
    peer: str
    connected_at: str
    last_activity_at: str


class ConnectionManager:
    """连接管理器。

    维护所有设备连接的注册表，支持按 device_id 检索。
    线程安全：所有操作在 asyncio 单线程中执行，无需锁。
    """

    def __init__(self) -> None:
        # device_id → ConnectionInfo
        self._connections: dict[str, ConnectionInfo] = {}

    def register(
        self,
        device_id: str,
        patient_id: str,
        peer: str,
    ) -> str:
        """注册一个新连接。

        Args:
            device_id: 设备 ID。
            patient_id: 患者 ID。
            peer: 对端地址。

        Returns:
            连接 ID。
        """
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        conn_id = f"conn_{uuid.uuid4().hex[:12]}"

        info = ConnectionInfo(
            conn_id=conn_id,
            device_id=device_id,
            patient_id=patient_id,
            peer=peer,
            connected_at=now,
            last_activity_at=now,
        )
        self._connections[device_id] = info
        return conn_id

    def unregister(self, device_id: str) -> None:
        """注销一个连接。"""
        self._connections.pop(device_id, None)

    def update_activity(self, device_id: str) -> None:
        """更新连接的最后活跃时间。"""
        info = self._connections.get(device_id)
        if info:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            info.last_activity_at = now

    def get(self, device_id: str) -> Optional[ConnectionInfo]:
        """获取连接信息。"""
        return self._connections.get(device_id)

    @property
    def active_count(self) -> int:
        return len(self._connections)

    @property
    def active_device_ids(self) -> list[str]:
        return list(self._connections.keys())

    def get_all(self) -> list[ConnectionInfo]:
        """获取所有连接信息。"""
        return list(self._connections.values())
