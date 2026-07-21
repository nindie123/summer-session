"""枚举定义。"""

from enum import StrEnum, IntEnum


class DeviceType(StrEnum):
    """设备类型。"""
    MONITOR = "Monitor"
    TEMP_SENSOR = "TempSensor"


class ValidationStatus(StrEnum):
    """验证状态。"""
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


class SignalQuality(StrEnum):
    """信号质量。"""
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"


class ObservationStatus(StrEnum):
    """观测值状态 (FHIR Observation.status)。"""
    FINAL = "final"
    INTERIM = "interim"
    AMENDED = "amended"


class MessageType(StrEnum):
    """心跳/认证消息类型。"""
    AUTH = "auth"
    AUTH_ACK = "auth_ack"
    VITALS = "vitals"
    DISCONNECT = "disconnect"


# LOINC 编码到参数名的映射
LOINC_MAP: dict[str, str] = {
    "8867-4": "heartRate",
    "8480-6": "sysBP",
    "8462-4": "diaBP",
    "2708-6": "spo2",
    "9279-1": "respiratoryRate",
    "8310-5": "temperature",
}

PARAM_LOINC_MAP: dict[str, str] = {v: k for k, v in LOINC_MAP.items()}
