"""InfluxDB 查询客户端。"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi


class InfluxQueryClient:
    """封装 InfluxDB v2 查询操作。"""

    def __init__(
        self,
        url: str = "",
        token: str = "",
        org: str = "",
    ) -> None:
        import os
        url = url or os.environ.get("INFLUXDB_URL", "http://localhost:8086")
        token = token or os.environ.get("INFLUXDB_TOKEN", "admin-token")
        org = org or os.environ.get("INFLUXDB_ORG", "hospital")
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._query_api = self._client.query_api()
        self._org = org

    def query_vitals(
        self,
        patient_id: str,
        parameters: Optional[list[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """查询患者体征时序数据。

        Args:
            patient_id: 患者 ID。
            parameters: 参数名列表（如 ["heartRate", "spo2"]），None 表示全部。
            start: 起始时间 ISO 8601。
            end: 结束时间 ISO 8601。
            limit: 返回条数上限。

        Returns:
            时序点列表。
        """
        start_str = start or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        end_str = end or datetime.now(timezone.utc).isoformat()

        param_filter = ""
        if parameters:
            params_quoted = [f'"{p}"' for p in parameters]
            param_filter = f' and parameter =~ /({"|".join(params_quoted)})/'

        # 使用 last() 获取每个参数的最新值，然后用 pivot 合并为一行
        # 适配 Flink 将每个体征写入不同行的格式
        query = f'''
        from(bucket: "vitals")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r["_measurement"] == "vitals")
            |> filter(fn: (r) => r["patientId"] == "{patient_id}")
            {param_filter}
            |> last()
            |> group(columns: ["patientId"])
            |> pivot(rowKey: ["patientId"], columnKey: ["parameter"], valueColumn: "_value")
        '''

        tables = self._query_api.query(query, org=self._org)

        points = []
        for table in tables:
            for record in table.records:
                try:
                    ts = record.get_time()
                except KeyError:
                    ts = None
                if ts is None:
                    ts = datetime.now(timezone.utc)
                point = {
                    "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.") +
                                f"{ts.microsecond // 1000:03d}Z",
                }
                # 提取所有参数列
                for key, value in record.values.items():
                    if isinstance(value, (int, float)) and key not in (
                        "_start", "_stop", "_time", "_value", "_field", "_measurement",
                        "patientId", "parameter", "unit", "result", "table",
                    ):
                        point[key] = value
                points.append(point)

        return points

    def query_mews(
        self,
        patient_id: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """查询患者 MEWS 评分历史。"""
        start_str = start or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        end_str = end or datetime.now(timezone.utc).isoformat()

        query = f'''
        from(bucket: "vitals")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r["_measurement"] == "mews")
            |> filter(fn: (r) => r["patientId"] == "{patient_id}")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> group()
            |> sort(desc: true, columns: ["_time"])
            |> limit(n: {limit})
        '''

        tables = self._query_api.query(query, org=self._org)

        points = []
        for table in tables:
            for record in table.records:
                points.append({
                    "timestamp": record.get_time().strftime("%Y-%m-%dT%H:%M:%S.") +
                                f"{record.get_time().microsecond // 1000:03d}Z",
                    "totalScore": record.values.get("totalScore", 0),
                    "riskLevel": record.values.get("riskLevel", "STABLE"),
                    "heartRate": record.values.get("heartRate", 0),
                    "sysBP": record.values.get("sysBP", 0),
                    "respiratoryRate": record.values.get("respiratoryRate", 0),
                    "temperature": record.values.get("temperature", 0),
                })

        return points

    async def async_query_vitals(
        self,
        patient_id: str,
        parameters: Optional[list[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """异步查询体征时序数据（不阻塞事件循环）。"""
        return await asyncio.to_thread(
            self.query_vitals, patient_id, parameters, start, end, limit
        )

    async def async_query_mews(
        self,
        patient_id: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """异步查询 MEWS 评分历史（不阻塞事件循环）。"""
        return await asyncio.to_thread(
            self.query_mews, patient_id, start, end, limit
        )

    def close(self) -> None:
        """关闭客户端。"""
        self._client.close()
