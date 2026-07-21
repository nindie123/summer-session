"""处理管道编排 — 串联 Parser → Validator → Normalizer → Enricher。"""

from typing import Any
import time

from src.models.patient_vital_record import PatientVitalRecord
from src.pipeline.parser import parse_vitals_message
from src.pipeline.validator import Validator, ValidationResult
from src.pipeline.normalizer import Normalizer
from src.pipeline.enricher import Enricher
from src.observability.logger import get_logger


logger = get_logger(__name__)


class ProcessingContext:
    """处理上下文 — 贯穿整个管道的状态容器。"""

    def __init__(self, raw_payload: bytes) -> None:
        self.raw_payload = raw_payload
        self.record: PatientVitalRecord | None = None
        self.validation: ValidationResult | None = None
        self.aborted: bool = False
        self.abort_reason: str = ""
        self.start_time: float = time.monotonic()

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000


class ProcessingPipeline:
    """处理管道 — 串联所有处理阶段。

    支持短路：如果 Validator 失败，不再执行后续阶段。
    """

    def __init__(self) -> None:
        self.validator = Validator()
        self.normalizer = Normalizer()
        self.enricher = Enricher()

    async def process(
        self,
        raw_payload: bytes,
        trace_id: str,
        received_at: str,
    ) -> ProcessingContext:
        """处理一条原始消息。

        Args:
            raw_payload: 原始 JSON 字节流。
            trace_id: 链路追踪 ID。
            received_at: 服务器接收时间。

        Returns:
            处理上下文，包含解析后的记录和验证结果。
        """
        ctx = ProcessingContext(raw_payload)

        try:
            # ① 解析
            ctx.record = parse_vitals_message(
                raw_payload, trace_id, received_at
            )
        except ValueError as e:
            ctx.aborted = True
            ctx.abort_reason = f"Parse failed: {e}"
            logger.warning("pipeline_aborted", reason=ctx.abort_reason)
            return ctx

        # ② 验证
        if ctx.record.observations:
            ctx.validation = self.validator.validate(ctx.record.observations)
            if ctx.validation.severity == "failed":
                ctx.record.processing.validation_status = "failed"
                # 验证失败仍继续（记录状态，由 Router 决定是否送 DLQ）
            elif ctx.validation.severity == "warning":
                ctx.record.processing.validation_status = "warning"

        # ③ 标准化
        ctx.record = self.normalizer.normalize(ctx.record)

        # ④ 富化
        ctx.record = self.enricher.enrich(ctx.record, trace_id=trace_id)

        return ctx
