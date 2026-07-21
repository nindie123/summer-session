"""结构化日志配置。"""

import logging
import sys
from typing import Any


class JsonFormatter(logging.Formatter):
    """简单的 JSON 日志格式化器。"""

    def format(self, record: logging.LogRecord) -> str:
        import json
        from datetime import datetime, timezone

        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") +
                         f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # 合并 extra 字段
        if hasattr(record, "extra"):
            log_entry["context"] = record.extra
        elif record.args:
            log_entry["context"] = record.args

        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
            }

        return json.dumps(log_entry, ensure_ascii=False)


class StructLogger:
    """结构化日志包装器。"""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)

        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(JsonFormatter())
            self._logger.addHandler(handler)

    def info(self, message: str, **extra: Any) -> None:
        self._logger.info(message, extra={"extra": extra})

    def warning(self, message: str, **extra: Any) -> None:
        self._logger.warning(message, extra={"extra": extra})

    def error(self, message: str, **extra: Any) -> None:
        self._logger.error(message, extra={"extra": extra})

    def debug(self, message: str, **extra: Any) -> None:
        self._logger.debug(message, extra={"extra": extra})


_loggers: dict[str, StructLogger] = {}


def get_logger(name: str) -> StructLogger:
    """获取或创建结构化日志器。"""
    if name not in _loggers:
        _loggers[name] = StructLogger(name)
    return _loggers[name]
