"""病区概览路由。"""

from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter

from src.clients.influx_client import InfluxQueryClient
from src.models.schemas import ApiResponse, WardOverview, WardPatient

router = APIRouter(prefix="/api/v1")
influx = InfluxQueryClient()

# 真实患者列表（和设备模拟器保持一致）
WARD_PATIENTS: dict[str, list[dict[str, str]]] = {
    "ICU-EAST": [
        {"bed_id": "ICU-101", "patient_id": "P0001"},
        {"bed_id": "ICU-102", "patient_id": "P0002"},
    ],
}


@router.get("/wards/{ward_id}/overview", response_model=ApiResponse)
async def get_ward_overview(ward_id: str):
    """获取病区所有患者概览。"""
    patients = WARD_PATIENTS.get(ward_id, [])
    if not patients:
        return ApiResponse(code=404, message=f"Unknown ward: {ward_id}")

    patients_data = []
    risk_counts: dict[str, int] = {"STABLE": 0, "WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0}
    now = datetime.now(timezone.utc)

    for entry in patients:
        pid = entry["patient_id"]
        bed_id = entry["bed_id"]

        # 查最新 MEWS（过去 2 小时内的数据）
        mews_points = influx.query_mews(
            pid,
            start=(now - timedelta(hours=2)).isoformat(),
            end=now.isoformat(),
            limit=1,
        )

        if mews_points:
            latest = mews_points[-1]
            risk_level = latest.get("riskLevel", "STABLE")
            risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1

            # 查最新 vitals（过去 2 小时）
            vitals_points = influx.query_vitals(
                pid,
                start=(now - timedelta(hours=2)).isoformat(),
                end=now.isoformat(),
                limit=50,
            )
            vitals_summary: dict[str, float] = {}
            if vitals_points:
                # 取最新一条中非 timestamp 字段
                latest_vitals = vitals_points[-1]
                for k, v in latest_vitals.items():
                    if k != "timestamp" and isinstance(v, (int, float)):
                        vitals_summary[k] = v

            patients_data.append(WardPatient(
                patient_id=pid,
                bed_id=bed_id,
                risk_level=risk_level,
                mews_score=latest.get("totalScore", 0),
                last_update=latest.get("timestamp", ""),
                vitals_summary=vitals_summary,
            ))
        else:
            risk_counts["STABLE"] = risk_counts.get("STABLE", 0) + 1
            patients_data.append(WardPatient(
                patient_id=pid,
                bed_id=bed_id,
                risk_level="STABLE",
                mews_score=0,
                last_update="",
                vitals_summary={},
            ))

    overview = WardOverview(
        ward_id=ward_id,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") +
                  f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
        summary=risk_counts,
        patients=patients_data,
    )

    return ApiResponse(data=overview.model_dump())
