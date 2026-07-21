"""MEWS 评分查询路由。"""

from typing import Optional

from fastapi import APIRouter, Query

from src.clients.influx_client import InfluxQueryClient
from src.models.schemas import ApiResponse

router = APIRouter(prefix="/api/v1")
influx = InfluxQueryClient()


@router.get("/patients/{patient_id}/mews", response_model=ApiResponse)
async def get_patient_mews(
    patient_id: str,
    start: Optional[str] = Query(None, description="Start time ISO 8601"),
    end: Optional[str] = Query(None, description="End time ISO 8601"),
    limit: int = Query(1000, description="Max records", le=10000),
):
    """获取患者 MEWS 评分历史。"""
    points = influx.query_mews(patient_id, start, end, limit)
    return ApiResponse(
        data={"patientId": patient_id, "points": points},
        pagination={"page": 1, "pageSize": limit, "total": len(points)},
    )
