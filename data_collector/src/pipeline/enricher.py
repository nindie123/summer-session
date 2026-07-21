"""元数据富化器 — 追加 traceId, serverTimestamp, 处理延迟等。"""

import time
from datetime import datetime, timezone
from typing import Optional
import uuid

from src.models.patient_vital_record import PatientVitalRecord, ProcessingInfo, DataQuality


class Enricher:
    """为消息追加处理元数据。"""

    def enrich(
        self,
        record: PatientVitalRecord,
        trace_id: Optional[str] = None,
    ) -> PatientVitalRecord:
        """富化记录。

        Args:
            record: 待富化的记录，原地修改并返回。
            trace_id: 可指定的 traceId（如为 None 则自动生成）。

        Returns:
            富化后的记录。
        """
        now = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%dT%H:%M:%S.") + \
                  f"{now.microsecond // 1000:03d}Z"

        # traceId
        if not record.trace_id:
            record.trace_id = trace_id or f"trace_{uuid.uuid4().hex[:24]}"

        # messageId
        if not record.message_id:
            record.message_id = f"msg_{uuid.uuid4().hex[:24]}"

        # 处理元数据
        record.processing.received_at = now_str

        # 计算处理延迟（从设备时间戳到服务器时间戳）
        if record.observations:
            earliest_device_ts = min(
                obs.effective_timestamp for obs in record.observations
            )
            try:
                device_dt = datetime.fromisoformat(
                    earliest_device_ts.replace("Z", "+00:00")
                )
                latency_ms = (now - device_dt).total_seconds() * 1000
                record.processing.processing_latency_ms = round(max(0, latency_ms), 1)
            except (ValueError, TypeError):
                record.processing.processing_latency_ms = 0.0

        # 信号质量标记（全零值检查）
        has_zero = any(obs.value == 0 for obs in record.observations)
        record.processing.data_quality = DataQuality(
            signal_quality="poor" if has_zero else "good",
            artifacts_detected=has_zero,
            signal_lost=has_zero,
        )

        return record
