"""统一体征数据模型 — PatientVitalRecord。"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import uuid


@dataclass
class Observation:
    """单条体征观测值。"""

    loinc_code: str
    display_name: str
    value: float
    unit: str
    effective_timestamp: str
    device_timestamp: str
    status: str = "final"
    body_site: Optional[str] = None
    method: Optional[str] = None


@dataclass
class SourceInfo:
    """设备与位置来源信息。"""

    device_id: str
    device_type: str
    device_model: str = "unknown"
    bed_id: Optional[str] = None
    ward_id: Optional[str] = None


@dataclass
class PatientInfo:
    """患者标识与位置信息。"""

    patient_id: str
    assigned_bed_id: Optional[str] = None
    mrn: Optional[str] = None
    name_hash: Optional[str] = None


@dataclass
class DataQuality:
    """信号质量元数据。"""

    signal_quality: str = "good"
    artifacts_detected: bool = False
    signal_lost: bool = False


@dataclass
class ProcessingInfo:
    """服务端处理元数据。"""

    received_at: str = ""
    processing_latency_ms: float = 0.0
    validation_status: str = "passed"
    data_quality: DataQuality = field(default_factory=DataQuality)


@dataclass
class PatientVitalRecord:
    """统一体征记录 — 采集层标准输出模型。

    设计要点:
      - observations[] 数组：一条设备消息可能携带多个体征
      - LOINC 编码：每种体征语义化标记，便于 FHIR 映射
      - source vs patient 分离：设备可重新分配给不同患者（转床场景）
      - messageId: 全局唯一，Flink Exactly-Once 幂等消费基础
      - traceId: 贯穿 5 层架构的全链路追踪
    """

    schema_version: str = "2.0"
    message_id: str = ""
    trace_id: str = ""

    source: Optional[SourceInfo] = None
    patient: Optional[PatientInfo] = None
    observations: list[Observation] = field(default_factory=list)
    processing: ProcessingInfo = field(default_factory=ProcessingInfo)

    def __post_init__(self) -> None:
        """自动生成 ID（如未提供）。"""
        now = datetime.now(timezone.utc)
        if not self.message_id:
            self.message_id = f"msg_{uuid.uuid4().hex[:24]}"
        if not self.trace_id:
            self.trace_id = f"trace_{uuid.uuid4().hex[:24]}"
        if not self.processing.received_at:
            self.processing.received_at = now.isoformat().replace("+00:00", "Z")

    def to_dict(self) -> dict:
        """序列化为字典（用于 Kafka 输出）。"""
        return {
            "schemaVersion": self.schema_version,
            "messageId": self.message_id,
            "traceId": self.trace_id,
            "source": {
                "deviceId": self.source.device_id if self.source else "",
                "deviceType": self.source.device_type if self.source else "",
                "deviceModel": self.source.device_model if self.source else "unknown",
                "bedId": self.source.bed_id if self.source else None,
                "wardId": self.source.ward_id if self.source else None,
            },
            "patient": {
                "patientId": self.patient.patient_id if self.patient else "",
                "assignedBedId": self.patient.assigned_bed_id if self.patient else None,
                "mrn": self.patient.mrn if self.patient else None,
            },
            "observations": [
                {
                    "loincCode": obs.loinc_code,
                    "displayName": obs.display_name,
                    "value": obs.value,
                    "unit": obs.unit,
                    "effectiveTimestamp": obs.effective_timestamp,
                    "deviceTimestamp": obs.device_timestamp,
                    "status": obs.status,
                    "bodySite": obs.body_site,
                    "method": obs.method,
                }
                for obs in self.observations
            ],
            "processing": {
                "receivedAt": self.processing.received_at,
                "processingLatencyMs": self.processing.processing_latency_ms,
                "validationStatus": self.processing.validation_status,
                "dataQuality": {
                    "signalQuality": self.processing.data_quality.signal_quality,
                    "artifactsDetected": self.processing.data_quality.artifacts_detected,
                    "signalLost": self.processing.data_quality.signal_lost,
                },
            },
        }
