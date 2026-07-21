"""告警查询路由（简单实现 — 未来可以从独立存储中查询）。"""

from typing import Optional

from fastapi import APIRouter, Query

from src.models.schemas import ApiResponse, AlertRecord

router = APIRouter(prefix="/api/v1")

# 当前为内存存储（演示用）
_alerts_store: list[AlertRecord] = []


def store_alert(alert: AlertRecord) -> None:
    """存储告警（供其他模块调用）。"""
    _alerts_store.append(alert)
    if len(_alerts_store) > 10000:
        _alerts_store[:1000] = []


@router.get("/patients/{patient_id}/alerts", response_model=ApiResponse)
async def get_patient_alerts(
    patient_id: str,
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    severity: Optional[str] = Query(None, description="Filter: WARNING/CRITICAL/EMERGENCY"),
    limit: int = Query(100, le=1000),
):
    """获取患者告警历史。"""
    filtered = [a for a in _alerts_store if a.patient_id == patient_id]
    if severity:
        filtered = [a for a in filtered if a.severity == severity]
    filtered.sort(key=lambda a: a.timestamp, reverse=True)

    return ApiResponse(
        data={"patientId": patient_id, "alerts": filtered[:limit]},
        pagination={"page": 1, "pageSize": limit, "total": len(filtered)},
    )
