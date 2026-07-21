"""Prometheus 指标。"""

from prometheus_client import Counter, Gauge, Histogram, generate_latest
from src.server.conn_manager import ConnectionManager


# 接收消息总数
messages_received = Counter(
    "collector_messages_received_total",
    "Total messages received from devices",
    ["device_type"],
)

# 处理成功消息数
messages_processed = Counter(
    "collector_messages_processed_total",
    "Total messages successfully processed",
    ["validation_status"],
)

# 死信消息数
messages_dlq = Counter(
    "collector_messages_dlq_total",
    "Total messages routed to dead letter queue",
    ["reason"],
)

# 活跃连接数
active_connections = Gauge(
    "collector_active_connections",
    "Current number of active device connections",
)

# 设备在线数
devices_online = Gauge(
    "collector_devices_online",
    "Current number of online devices",
)

# 处理延迟
processing_latency = Histogram(
    "collector_processing_latency_ms",
    "Message processing latency in milliseconds",
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500],
)


def update_connection_metrics(conn_manager: ConnectionManager) -> None:
    """更新连接相关指标。"""
    active_connections.set(conn_manager.active_count)
