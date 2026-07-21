"""Kafka 异步生产者。"""

from typing import Optional
import orjson

from aiokafka import AIOKafkaProducer

from src.observability.logger import get_logger

logger = get_logger(__name__)


class KafkaProducer:
    """封装 AIOKafkaProducer，提供消息发送和重试。"""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: str = "data-collector",
    ) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._started = False

    async def start(self) -> None:
        """启动生产者（可能抛出连接异常）。"""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            acks=1,
            compression_type=None,
            max_request_size=1048576,
            request_timeout_ms=5000,
            connections_max_idle_ms=30000,
            linger_ms=100,  # 稍等一点点聚合 batch
        )
        await self._producer.start()
        self._started = True
        logger.info("kafka_producer_started", extra={
            "bootstrap_servers": self._bootstrap_servers,
        })

    @property
    def available(self) -> bool:
        return self._started and self._producer is not None

    async def send(
        self,
        topic: str,
        key: str,
        value: dict,
    ) -> None:
        """发送消息到 Kafka。

        Args:
            topic: Kafka Topic。
            key: 消息 Key（通常是 patientId）。
            value: 消息 Value（字典，自动序列化为 JSON）。
        """
        if not self.available:
            return

        try:
            json_bytes = orjson.dumps(value)
            await self._producer.send(
                topic=topic,
                key=key.encode("utf-8"),
                value=json_bytes,
            )
            await self._producer.flush()  # 确保发送完成
        except Exception as e:
            logger.error("kafka_send_failed", extra={
                "topic": topic, "key": key, "error": str(e),
            })

    async def close(self) -> None:
        """关闭生产者。"""
        if self._producer:
            try:
                await self._producer.stop()
            except Exception:
                pass
            self._started = False
            logger.info("kafka_producer_stopped")
