"""JSON 解析器 — 将字节流解析为结构化数据。"""

from collections.abc import Callable
from typing import Any
import orjson

from src.models.enums import MessageType
from src.models.patient_vital_record import (
    Observation,
    PatientVitalRecord,
    PatientInfo,
    SourceInfo,
)


def parse_vitals_message(
    payload: bytes,
    trace_id: str,
    received_at: str,
) -> PatientVitalRecord:
    """将 JSON vitals 消息解析为 PatientVitalRecord。

    Args:
        payload: JSON 字节流。
        trace_id: 链路追踪 ID。
        received_at: 服务器接收时间 (ISO 8601)。

    Returns:
        解析后的 PatientVitalRecord。

    Raises:
        ValueError: JSON 格式错误或缺少必要字段。
    """
    try:
        data: dict[str, Any] = orjson.loads(payload)
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e

    msg_type = data.get("type", "")
    if msg_type not in (MessageType.VITALS,):
        raise ValueError(f"Unsupported message type: {msg_type}")

    device_id = data.get("deviceId", "")
    device_type = data.get("deviceType", "")
    patient_id = data.get("patientId", "")
    device_ts = data.get("timestamp", "")

    if not all([device_id, device_type, patient_id]):
        raise ValueError(f"Missing required fields: deviceId={device_id}, patientId={patient_id}")

    raw_observations: list[dict] = data.get("observations", [])
    if not raw_observations:
        # 允许空 observations（心跳消息），但记录 warning
        pass

    observations = [
        Observation(
            loinc_code=obs.get("code", ""),
            display_name=obs.get("name", ""),
            value=float(obs.get("value", 0)),
            unit=obs.get("unit", ""),
            effective_timestamp=device_ts,
            device_timestamp=obs.get("deviceTimestamp", device_ts),
            status="final",
        )
        for obs in raw_observations
    ]

    record = PatientVitalRecord(
        trace_id=trace_id,
        source=SourceInfo(
            device_id=device_id,
            device_type=device_type,
            device_model=data.get("deviceModel", "Simulator-v1.0"),
        ),
        patient=PatientInfo(
            patient_id=patient_id,
        ),
        observations=observations,
    )
    record.processing.received_at = received_at

    return record


def parse_auth_message(payload: bytes) -> dict[str, Any]:
    """解析 auth 消息。

    Args:
        payload: JSON 字节流。

    Returns:
        包含 deviceId, deviceType, patientId, secret 的字典。
    """
    try:
        data: dict[str, Any] = orjson.loads(payload)
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Invalid auth JSON: {e}") from e

    return {
        "device_id": data.get("deviceId", ""),
        "device_type": data.get("deviceType", ""),
        "patient_id": data.get("patientId", ""),
        "secret": data.get("secret", ""),
    }
