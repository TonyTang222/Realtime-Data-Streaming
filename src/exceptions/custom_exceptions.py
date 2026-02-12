"""
Custom Exception Hierarchy for the Streaming Pipeline
"""

from typing import Optional, Any


class StreamingPipelineError(Exception):
    """Base exception for all pipeline errors."""

    def __init__(self, message: str, *args, **kwargs):
        self.message = message
        super().__init__(message, *args)

    def __str__(self) -> str:
        return self.message


# ============================================================
# API Exceptions
# ============================================================

class APIError(StreamingPipelineError):
    """Raised when external API calls fail."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self) -> str:
        parts = [self.message]
        if self.status_code:
            parts.append(f"[HTTP {self.status_code}]")
        return " ".join(parts)


class APIConnectionError(APIError):
    """Raised when unable to connect to API."""
    pass


class APITimeoutError(APIError):
    """Raised when API request times out."""
    pass


class APIRateLimitError(APIError):
    """Raised when API rate limit is exceeded (HTTP 429)."""

    def __init__(
        self,
        message: str,
        retry_after: Optional[int] = None,
        status_code: int = 429
    ):
        super().__init__(message, status_code=status_code)
        self.retry_after = retry_after


# ============================================================
# Kafka Exceptions
# ============================================================

class KafkaError(StreamingPipelineError):
    """Base exception for Kafka-related errors."""
    pass


class KafkaConnectionError(KafkaError):
    """Raised when unable to connect to Kafka broker."""
    pass


class KafkaPublishError(KafkaError):
    """Raised when message publishing to Kafka fails."""

    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        key: Optional[str] = None,
        original_data: Optional[Any] = None
    ):
        super().__init__(message)
        self.topic = topic
        self.key = key
        self.original_data = original_data

    def __str__(self) -> str:
        parts = [self.message]
        if self.topic:
            parts.append(f"[topic={self.topic}]")
        if self.key:
            parts.append(f"[key={self.key}]")
        return " ".join(parts)


# ============================================================
# Cassandra Exceptions
# ============================================================

class CassandraError(StreamingPipelineError):
    """Base exception for Cassandra-related errors."""
    pass


class CassandraConnectionError(CassandraError):
    """Raised when unable to connect to Cassandra."""
    pass


class CassandraWriteError(CassandraError):
    """Raised when write to Cassandra fails."""

    def __init__(
        self,
        message: str,
        keyspace: Optional[str] = None,
        table: Optional[str] = None
    ):
        super().__init__(message)
        self.keyspace = keyspace
        self.table = table


class CassandraReadError(CassandraError):
    """Raised when read from Cassandra fails."""
    pass


# ============================================================
# Data Validation Exceptions
# ============================================================

class DataValidationError(StreamingPipelineError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        expected_type: Optional[str] = None
    ):
        super().__init__(message)
        self.field = field
        self.value = value
        self.expected_type = expected_type

    def __str__(self) -> str:
        parts = [self.message]
        if self.field:
            parts.append(f"[field={self.field}]")
        if self.value is not None:
            value_str = str(self.value)[:50]
            parts.append(f"[value={value_str}]")
        return " ".join(parts)


class TransformationError(StreamingPipelineError):
    """Raised when data transformation fails."""

    def __init__(
        self,
        message: str,
        source_data: Optional[Any] = None
    ):
        super().__init__(message)
        self.source_data = source_data


# ============================================================
# Helper Functions
# ============================================================

def is_retryable(error: Exception) -> bool:
    """
    Determine whether an error is retryable.

    Retryable errors:
    - Connection errors
    - Timeout errors
    - Rate limit errors

    Non-retryable errors:
    - Validation errors
    - Schema errors
    - Serialization errors
    """
    retryable_types = (
        APIConnectionError,
        APITimeoutError,
        APIRateLimitError,
        KafkaConnectionError,
        CassandraConnectionError,
    )
    return isinstance(error, retryable_types)
