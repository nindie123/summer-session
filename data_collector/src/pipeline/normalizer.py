"""标准化器 — 时间/单位/编码统一。"""

from datetime import datetime, timezone

from src.models.patient_vital_record import PatientVitalRecord, Observation


class Normalizer:
    """标准化器 — 统一时间格式、单位、编码。"""

    def normalize(self, record: PatientVitalRecord) -> PatientVitalRecord:
        """标准化一条记录。

        Args:
            record: 待标准化的记录，原地修改并返回。

        Returns:
            标准化后的记录。
        """
        # 时间标准化：确保所有时间戳为 ISO 8601 + Z
        for obs in record.observations:
            obs.effective_timestamp = self._normalize_timestamp(obs.effective_timestamp)
            obs.device_timestamp = self._normalize_timestamp(obs.device_timestamp)

        record.processing.received_at = self._normalize_timestamp(
            record.processing.received_at
        )

        # 单位统一：确保已知参数使用标准单位
        unit_map: dict[str, str] = {
            "bpm": "/min",
            "beats/min": "/min",
            "beats per minute": "/min",
            "breaths/min": "/min",
            "C": "°C",
            "celsius": "°C",
            "F": "°F",
        }
        for obs in record.observations:
            lower_unit = obs.unit.lower()
            if lower_unit in unit_map:
                obs.unit = unit_map[lower_unit]

        return record

    def _normalize_timestamp(self, ts: str) -> str:
        """将各种时间格式统一为 ISO 8601 UTC。

        Args:
            ts: 原始时间字符串。

        Returns:
            标准化后的 ISO 8601 UTC 字符串。
        """
        if not ts:
            return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
                   f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"

        # 如果已经是 ISO 8601 格式，确保以 Z 结尾
        if ts.endswith("Z"):
            return ts
        if "+" in ts or "T" in ts:
            # 尝试解析并转为 UTC
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%dT%H:%M:%S.") + \
                       f"{dt.microsecond // 1000:03d}Z"
            except ValueError:
                pass

        return ts
