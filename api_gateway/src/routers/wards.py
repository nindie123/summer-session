"""病区概览路由 — 动态从 InfluxDB 发现患者。"""

from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import APIRouter

from src.clients.influx_client import InfluxQueryClient
from src.models.schemas import ApiResponse, WardOverview, WardPatient

router = APIRouter(prefix="/api/v1")
influx = InfluxQueryClient()


@router.get("/wards/{ward_id}/overview", response_model=ApiResponse)
async def get_ward_overview(ward_id: str):
    """获取病区所有患者概览（从 InfluxDB 动态查询）。"""
    now = datetime.now(timezone.utc)
    lookback = now - timedelta(hours=2)

    # 查询过去2小时内有哪些患者有数据
    query = f'''
    from(bucket: "vitals")
        |> range(start: {lookback.isoformat()}, stop: {now.isoformat()})
        |> filter(fn: (r) => r["_measurement"] == "mews")
        |> group(columns: ["patientId"])
        |> distinct(column: "patientId")
    '''
    tables = influx._query_api.query(query, org=influx._org)
    patient_ids: set[str] = set()
    for table in tables:
        for record in table.records:
            pid = record.values.get("patientId")
            if pid:
                patient_ids.add(str(pid))

    if not patient_ids:
        # 回退：直接查询 vitals 表
        query2 = f'''
        from(bucket: "vitals")
            |> range(start: {lookback.isoformat()}, stop: {now.isoformat()})
            |> filter(fn: (r) => r["_measurement"] == "vitals")
            |> group(columns: ["patientId"])
            |> distinct(column: "patientId")
        '''
        tables2 = influx._query_api.query(query2, org=influx._org)
        for table in tables2:
            for record in table.records:
                pid = record.values.get("patientId")
                if pid:
                    patient_ids.add(str(pid))

    patients_data: list[dict[str, Any]] = []
    risk_counts: dict[str, int] = {"STABLE": 0, "WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0}

    for pid in sorted(patient_ids):
        # 查最新 MEWS
        mews_points = influx.query_mews(
            pid,
            start=lookback.isoformat(),
            end=now.isoformat(),
            limit=1,
        )

        if mews_points:
            latest = mews_points[-1]
            risk_level = latest.get("riskLevel", "STABLE")
            risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1

            # 查最新 vitals
            vitals_points = influx.query_vitals(
                pid,
                start=lookback.isoformat(),
                end=now.isoformat(),
                limit=50,
            )
            vitals_summary: dict[str, float] = {}
            if vitals_points:
                for k, v in vitals_points[-1].items():
                    if k != "timestamp" and isinstance(v, (int, float)):
                        vitals_summary[k] = v

            patients_data.append(WardPatient(
                patient_id=pid,
                bed_id="",
                risk_level=risk_level,
                mews_score=latest.get("totalScore", 0),
                last_update=latest.get("timestamp", ""),
                vitals_summary=vitals_summary,
            ).model_dump())
        else:
            risk_counts["STABLE"] = risk_counts.get("STABLE", 0) + 1
            patients_data.append(WardPatient(
                patient_id=pid,
                bed_id="",
                risk_level="STABLE",
                mews_score=0,
                last_update="",
                vitals_summary={},
            ).model_dump())

    overview = WardOverview(
        ward_id=ward_id,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") +
                  f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
        summary=risk_counts,
        patients=patients_data,
    )

    return ApiResponse(data=overview.model_dump())
