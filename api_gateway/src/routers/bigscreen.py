"""大屏可视化专用接口 — 实时推送 + 聚合查询。"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from src.clients.influx_client import InfluxQueryClient
from src.models.schemas import ApiResponse

router = APIRouter(prefix="/api/v1/bigscreen")
influx = InfluxQueryClient()


# ── WebSocket 连接管理 ──────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for conn in self.active_connections:
            try:
                await conn.send_text(message)
            except Exception:
                pass


manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 实时推送 — 每 3 秒推送全部患者体征 + MEWS。

    连接后自动接收数据，不需要发送任何消息。
    """
    await manager.connect(websocket)
    try:
        while True:
            data = await build_snapshot()
            await websocket.send_text(json.dumps(data, ensure_ascii=False))
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ── 大屏聚合接口 ────────────────────────────────────


@router.get("/overview")
async def bigscreen_overview():
    """大屏概览 — 一次性返回全部患者完整数据。

    返回:
      - 患者列表（体征 + MEWS + 趋势 + 风险）
      - 病区统计（总数、各风险等级数量）
      - 告警摘要
    """
    now = datetime.now(timezone.utc)
    lookback = now - timedelta(hours=6)

    # 发现所有患者（异步执行）
    patient_ids = await asyncio.to_thread(_discover_patients, lookback, now)

    patients_data = []
    risk_counts: dict[str, int] = {
        "STABLE": 0, "WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0
    }
    ward_alerts: list[dict] = []
    total_beds = 50  # 总床位数

    for pid in sorted(patient_ids):
        # 体征（异步查询，不阻塞事件循环）
        vitals_points = await influx.async_query_vitals(
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

        # MEWS（异步查询）
        mews_points = await influx.async_query_mews(
            pid,
            start=lookback.isoformat(),
            end=now.isoformat(),
            limit=1,
        )
        risk = "STABLE"
        mews_score = 0
        if mews_points:
            risk = mews_points[-1].get("riskLevel", "STABLE")
            mews_score = mews_points[-1].get("totalScore", 0)

        risk_counts[risk] = risk_counts.get(risk, 0) + 1

        # 获取最近 N 个 MEWS 点用于趋势展示（异步查询）
        mews_history = await influx.async_query_mews(
            pid,
            start=lookback.isoformat(),
            end=now.isoformat(),
            limit=20,
        )
        mews_trend = [
            {
                "t": p.get("timestamp", "")[11:19] if len(p.get("timestamp", "")) > 19 else p.get("timestamp", ""),
                "s": p.get("totalScore", 0),
            }
            for p in mews_history
        ]

        patients_data.append({
            "patientId": pid,
            "riskLevel": risk,
            "mewsScore": mews_score,
            "vitals": {
                "heartRate": vitals_summary.get("heartRate"),
                "sysBP": vitals_summary.get("sysBP"),
                "diaBP": vitals_summary.get("diaBP"),
                "spo2": vitals_summary.get("spo2"),
                "respiratoryRate": vitals_summary.get("respiratoryRate"),
                "temperature": vitals_summary.get("temperature"),
            },
            "mewsTrend": mews_trend[-10:] if len(mews_trend) > 10 else mews_trend,
            "bedId": "",
        })

    return ApiResponse(data={
        "wardId": "ICU-EAST",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") +
                    f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
        "totalBeds": total_beds,
        "occupiedBeds": len(patient_ids),
        "summary": risk_counts,
        "patients": patients_data,
        "alerts": ward_alerts,
    })


def _discover_patients(start, end) -> set[str]:
    """从 InfluxDB 发现所有患者 ID。"""
    patient_ids: set[str] = set()
    try:
        for measurement in ("mews", "vitals"):
            query = f'''
            from(bucket: "vitals")
                |> range(start: {start.isoformat()}, stop: {end.isoformat()})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> group(columns: ["patientId"])
                |> distinct(column: "patientId")
            '''
            tables = influx._query_api.query(query, org=influx._org)
            for table in tables:
                for record in table.records:
                    pid = record.values.get("patientId")
                    if pid:
                        patient_ids.add(str(pid))
    except Exception:
        pass
    return patient_ids


async def build_snapshot() -> dict:
    """构建 WebSocket 推送的快照数据。"""
    now = datetime.now(timezone.utc)
    lookback = now - timedelta(hours=2)

    patient_ids = await asyncio.to_thread(_discover_patients, lookback, now)
    patients = []

    for pid in sorted(patient_ids):
        vp = await influx.async_query_vitals(pid, start=lookback.isoformat(), end=now.isoformat(), limit=10)
        vitals: dict[str, float] = {}
        if vp:
            for k, v in vp[-1].items():
                if k != "timestamp" and isinstance(v, (int, float)):
                    vitals[k] = v

        mp = await influx.async_query_mews(pid, start=lookback.isoformat(), end=now.isoformat(), limit=1)
        mews_score = mp[-1].get("totalScore", 0) if mp else 0
        risk = mp[-1].get("riskLevel", "STABLE") if mp else "STABLE"

        patients.append({
            "id": pid,
            "mews": mews_score,
            "risk": risk,
            "v": {
                "hr": vitals.get("heartRate"),
                "sbp": vitals.get("sysBP"),
                "dbp": vitals.get("diaBP"),
                "spo2": vitals.get("spo2"),
                "rr": vitals.get("respiratoryRate"),
                "temp": vitals.get("temperature"),
            },
        })

    risk_counts = {"STABLE": 0, "WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0}
    for p in patients:
        r = p["risk"]
        risk_counts[r] = risk_counts.get(r, 0) + 1

    return {
        "t": datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "ts": int(time.time()),
        "summary": risk_counts,
        "patients": patients,
    }
