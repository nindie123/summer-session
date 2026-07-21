"""API 响应 Schema。"""

from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel


class ApiResponse(BaseModel):
    """统一响应格式。"""
    code: int = 200
    message: str = "success"
    data: Any = None
    pagination: Optional[dict] = None
    trace_id: str = ""

    model_config = {"json_schema_extra": {"examples": [{
        "code": 200,
        "message": "success",
        "data": {},
        "pagination": {"page": 1, "pageSize": 100, "total": 0},
        "traceId": "trace_...",
    }]}}


class VitalSignPoint(BaseModel):
    """体征时序点。"""
    timestamp: str
    # 动态参数，如 heartRate=72, spo2=98
    model_config = {"extra": "allow"}


class MewsPoint(BaseModel):
    """MEWS 时序点。"""
    timestamp: str
    total_score: int
    risk_level: str
    components: dict[str, int]


class AlertRecord(BaseModel):
    """告警记录。"""
    alert_id: str
    timestamp: str
    type: str
    severity: str
    description: str
    mews_score: int


class PatientSnapshot(BaseModel):
    """患者快照。"""
    patient_id: str
    timestamp: str
    vitals: dict[str, float]
    mews_score: int
    risk_level: str
    active_devices: list[str]
    devices_online: bool


class WardPatient(BaseModel):
    """病区中的患者摘要。"""
    patient_id: str
    bed_id: str
    risk_level: str
    mews_score: int
    last_update: str
    vitals_summary: dict[str, float]


class WardOverview(BaseModel):
    """病区概览。"""
    ward_id: str
    timestamp: str
    summary: dict[str, int]
    patients: list[WardPatient]
