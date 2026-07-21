"""设备在线跟踪器。"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class DeviceStatus:
    """设备状态。"""
    device_id: str
    patient_id: str
    online: bool
    connected_at: Optional[str] = None
    last_data_at: Optional[str] = None
    data_count: int = 0


class DeviceTracker:
    """跟踪设备在线状态和数据健康度。"""

    def __init__(self) -> None:
        self._devices: dict[str, DeviceStatus] = {}

    async def mark_online(self, device_id: str, patient_id: str) -> None:
        """标记设备在线。"""
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self._devices[device_id] = DeviceStatus(
            device_id=device_id,
            patient_id=patient_id,
            online=True,
            connected_at=now,
            last_data_at=now,
        )

    async def mark_offline(self, device_id: str) -> None:
        """标记设备离线。"""
        status = self._devices.get(device_id)
        if status:
            status.online = False

    async def record_data(self, device_id: str) -> None:
        """记录一次数据接收。"""
        status = self._devices.get(device_id)
        if status:
            status.last_data_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            status.data_count += 1

    def get_status(self, device_id: str) -> Optional[DeviceStatus]:
        """获取设备状态。"""
        return self._devices.get(device_id)

    @property
    def online_count(self) -> int:
        return sum(1 for d in self._devices.values() if d.online)

    @property
    def offline_count(self) -> int:
        return sum(1 for d in self._devices.values() if not d.online)

    @property
    def all_devices(self) -> list[DeviceStatus]:
        return list(self._devices.values())
