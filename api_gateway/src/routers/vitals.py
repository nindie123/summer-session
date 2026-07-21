"""体征查询路由。"""

from typing import Optional

from fastapi import APIRouter, Query

from src.clients.influx_client import InfluxQueryClient
from src.models.schemas import ApiResponse, VitalSignPoint

router = APIRouter(prefix="/api/v1")
influx = InfluxQueryClient()


@router.get("/patients/{patient_id}/vitals", response_model=ApiResponse)
async def get_patient_vitals(
    patient_id: str,
    start: Optional[str] = Query(None, description="Start time ISO 8601"),
    end: Optional[str] = Query(None, description="End time ISO 8601"),
    limit: int = Query(1000, description="Max records", le=10000),
    parameters: Optional[str] = Query(None, description="Comma-separated params"),
):
    """获取患者时序体征数据。"""
    param_list = parameters.split(",") if parameters else None
    points = influx.query_vitals(patient_id, param_list, start, end, limit)
    return ApiResponse(
        data={
            "patientId": patient_id,
            "parameters": param_list or ["all"],
            "points": points,
        },
        pagination={"page": 1, "pageSize": limit, "total": len(points)},
    )


@router.get("/patients/{patient_id}/snapshot", response_model=ApiResponse)
async def get_patient_snapshot(patient_id: str):
    """获取患者当前最新快照。"""
    # 查询最近 10 秒内的数据作为当前快照
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    points = influx.query_vitals(
        patient_id,
        start=(now - timedelta(seconds=10)).isoformat(),
        end=now.isoformat(),
        limit=1,
    )

    if not points:
        return ApiResponse(
            code=404,
            message="No recent data",
            data={"patientId": patient_id, "devicesOnline": False},
        )

    latest = points[-1]
    return ApiResponse(data={
        "patientId": patient_id,
        "timestamp": latest.get("timestamp", ""),
        "vitals": {k: v for k, v in latest.items() if k != "timestamp"},
        "devicesOnline": True,
    })
