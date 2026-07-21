"""消息路由器 — 按类型分发到不同 Kafka Topic。"""

from typing import Optional

from src.kafka.producer import KafkaProducer
from src.models.patient_vital_record import PatientVitalRecord
from src.observability.logger import get_logger

logger = get_logger(__name__)


class Router:
    """消息路由器。

    路由规则:
      - 验证通过的消息 → standardized.vitals
      - 验证告警的消息 → standardized.vitals（带 validationStatus=warning）
      - 验证失败的消息 → dead.letter.queue
      - Kafka 不可用时 → 记录日志，消息不丢失（未来可加本地缓冲）
    """

    TOPIC_VITALS = "standardized.vitals"
    TOPIC_DLQ = "dead.letter.queue"

    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self._kafka_ok = False

    async def start(self) -> None:
        """启动路由器（启动 Kafka 生产者）。"""
        try:
            await self.producer.start()
            self._kafka_ok = True
        except Exception as e:
            self._kafka_ok = False
            logger.warning("router_start_failed", error=str(e))
            raise  # 让调用方决定是否降级

    @property
    def available(self) -> bool:
        return self._kafka_ok

    async def route(self, record: PatientVitalRecord) -> None:
        """路由一条处理完成的记录。

        Args:
            record: 处理完成的体征记录。
        """
        if not self._kafka_ok:
            # Kafka 降级：仅日志输出
            patient_id = record.patient.patient_id if record.patient else "unknown"
            obs_count = len(record.observations)
            print(f"[ROUTER] (Kafka unavailable) {patient_id}: {obs_count} observations, trace={record.trace_id}")
            logger.info("route_downgraded", extra={
                "patient_id": patient_id,
                "message_id": record.message_id,
                "observations": obs_count,
            })
            return

        patient_id = record.patient.patient_id if record.patient else "unknown"
        data = record.to_dict()

        validation = record.processing.validation_status

        if validation == "failed":
            await self.producer.send(
                topic=self.TOPIC_DLQ,
                key=patient_id,
                value={**data, "dlqReason": "validation_failed"},
            )
            logger.info("route_to_dlq", extra={
                "patient_id": patient_id,
                "message_id": record.message_id,
            })
        else:
            await self.producer.send(
                topic=self.TOPIC_VITALS,
                key=patient_id,
                value=data,
            )

    async def route_failed(self, raw_payload: bytes, reason: str) -> None:
        """将解析失败的消息直接路由到 DLQ 或日志。

        Args:
            raw_payload: 原始字节。
            reason: 失败原因。
        """
        if not self._kafka_ok:
            print(f"[ROUTER] DLQ (unavailable): {reason}")
            return

        import orjson
        try:
            data = orjson.loads(raw_payload)
            patient_id = data.get("patientId", "unknown")
        except Exception:
            patient_id = "unknown"

        await self.producer.send(
            topic=self.TOPIC_DLQ,
            key=patient_id,
            value={
                "rawPayload": raw_payload.decode("utf-8", errors="replace"),
                "dlqReason": reason,
                "routedAt": __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S.") + "000Z",
            },
        )

    async def close(self) -> None:
        """关闭路由器。"""
        await self.producer.close()
