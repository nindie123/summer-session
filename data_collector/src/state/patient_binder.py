"""患者-设备绑定管理器。"""

from typing import Optional


class PatientBinder:
    """维护 deviceId ↔ patientId 映射关系。

    当前为内存实现（开发环境），生产环境应使用 Redis + MySQL。
    """

    def __init__(self) -> None:
        # patient_id → set[device_id]
        self._patient_to_devices: dict[str, set[str]] = {}
        # device_id → patient_id
        self._device_to_patient: dict[str, str] = {}

    async def bind(self, patient_id: str, device_id: str) -> None:
        """绑定设备到患者。

        Args:
            patient_id: 患者 ID。
            device_id: 设备 ID。
        """
        self._device_to_patient[device_id] = patient_id
        if patient_id not in self._patient_to_devices:
            self._patient_to_devices[patient_id] = set()
        self._patient_to_devices[patient_id].add(device_id)

    async def unbind(self, device_id: str) -> None:
        """解除设备绑定。"""
        patient_id = self._device_to_patient.pop(device_id, None)
        if patient_id and patient_id in self._patient_to_devices:
            self._patient_to_devices[patient_id].discard(device_id)

    def get_patient_id(self, device_id: str) -> Optional[str]:
        """根据设备 ID 获取患者 ID。"""
        return self._device_to_patient.get(device_id)

    def get_device_ids(self, patient_id: str) -> set[str]:
        """根据患者 ID 获取所有关联设备 ID。"""
        return self._patient_to_devices.get(patient_id, set())

    @property
    def device_count(self) -> int:
        return len(self._device_to_patient)

    @property
    def patient_count(self) -> int:
        return len(self._patient_to_devices)
