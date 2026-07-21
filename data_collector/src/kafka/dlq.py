"""死信队列处理。"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class DeadLetterMessage:
    """死信消息。"""
    original_payload: str
    error_reason: str
    device_id: str
    patient_id: str
    failed_at: str
    retry_count: int = 0


class DeadLetterQueue:
    """死信队列本地缓冲。

    验证失败/投递失败的消息先缓存在本地，
    可人工审查后重新投递。
    """

    def __init__(self, max_size: int = 1000) -> None:
        self._messages: list[DeadLetterMessage] = []
        self._max_size = max_size

    def push(
        self,
        payload: str,
        reason: str,
        device_id: str = "",
        patient_id: str = "",
    ) -> None:
        """添加一条死信消息。"""
        msg = DeadLetterMessage(
            original_payload=payload,
            error_reason=reason,
            device_id=device_id,
            patient_id=patient_id,
            failed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )

        if len(self._messages) >= self._max_size:
            self._messages.pop(0)  # 移除最旧的

        self._messages.append(msg)

    def pop_all(self) -> list[DeadLetterMessage]:
        """取出所有死信消息（用于重放）。"""
        msgs = list(self._messages)
        self._messages.clear()
        return msgs

    @property
    def count(self) -> int:
        return len(self._messages)
