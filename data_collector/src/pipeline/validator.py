"""数据验证器 — Schema 校验 + 值域校验 + 逻辑校验。"""

from dataclasses import dataclass, field
from typing import Any

from src.models.patient_vital_record import Observation


@dataclass
class ValidationResult:
    """验证结果。"""

    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    severity: str = "passed"  # passed | warning | failed


# 各参数的有效值域
PARAM_RANGES: dict[str, tuple[float, float]] = {
    "8867-4": (0, 300),     # HR
    "8480-6": (0, 300),     # SBP
    "8462-4": (0, 200),     # DBP
    "2708-6": (0, 100),     # SpO2
    "9279-1": (0, 60),      # RR
    "8310-5": (30.0, 43.0), # Temp
}


class Validator:
    """数据验证器 — 检查观测值在合理范围内。"""

    def validate(self, observations: list[Observation]) -> ValidationResult:
        """验证观测值列表。

        Args:
            observations: 待验证的观测值列表。

        Returns:
            验证结果。
        """
        result = ValidationResult()

        for obs in observations:
            self._validate_single(obs, result)

        return result

    def _validate_single(
        self,
        obs: Observation,
        result: ValidationResult,
    ) -> None:
        """验证单条观测值。"""
        # 范围校验
        if obs.loinc_code in PARAM_RANGES:
            min_val, max_val = PARAM_RANGES[obs.loinc_code]
            if obs.value < min_val or obs.value > max_val:
                result.passed = False
                result.errors.append(
                    f"{obs.display_name} ({obs.loinc_code}): "
                    f"value {obs.value} out of range [{min_val}, {max_val}]"
                )
                result.severity = "failed"

        # 逻辑校验：SpO2 不会高于 100%
        if obs.loinc_code == "2708-6" and obs.value > 100:
            result.passed = False
            result.errors.append(f"SpO2 value {obs.value} exceeds 100%")
            result.severity = "failed"

        # 信号丢失检测：全零值
        if obs.loinc_code == "2708-6" and obs.value == 0:
            result.warnings.append("SpO2=0: possible signal loss")
            if result.severity == "passed":
                result.severity = "warning"
