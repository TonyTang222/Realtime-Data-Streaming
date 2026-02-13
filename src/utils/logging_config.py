"""Structured Logging Configuration"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter.

    Output example::

        {
            "timestamp": "2024-01-15T10:30:45.123456Z",
            "level": "INFO",
            "logger": "src.producers.kafka_producer",
            "message": "Message sent successfully",
            "extra": {
                "topic": "user_data",
                "partition": 0,
                "offset": 12345
            }
        }
    """

    def __init__(self, include_timestamp: bool = True):
        super().__init__()
        self.include_timestamp = include_timestamp

    def format(self, record: logging.LogRecord) -> str:
        # Base fields
        log_data: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add timestamp
        if self.include_timestamp:
            log_data["timestamp"] = datetime.now(timezone.utc).isoformat()

        # Collect extra fields passed via logger.info("msg", extra={...})
        # Exclude built-in LogRecord attributes
        builtin_attrs = {
            "name",
            "msg",
            "args",
            "created",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "exc_info",
            "exc_text",
            "thread",
            "threadName",
            "message",
            "asctime",
            "taskName",
        }

        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in builtin_attrs and not k.startswith("_")
        }
        if extras:
            log_data["extra"] = extras

        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Serialize to JSON (default=str handles non-serializable objects)
        return json.dumps(log_data, default=str, ensure_ascii=False)


def setup_logging(
    level: Optional[str] = None, log_file: Optional[str] = None
) -> logging.Logger:
    """
    Configure structured JSON logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional file path for log output.
    """
    # Load defaults from settings (deferred import to avoid circular dependency)
    from src.config.settings import get_settings

    settings = get_settings()

    log_level = level or settings.logging.level

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(JSONFormatter())
    root_logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        # Always use JSON format for files
        file_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module *name* (typically ``__name__``)."""
    return logging.getLogger(name)
