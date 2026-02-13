"""Production-Ready Kafka Producer with Dead Letter Queue."""

import json
import logging
from typing import Any, Callable, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

from src.config.schemas import DLQMessage
from src.config.settings import KafkaConfig, get_settings
from src.exceptions.custom_exceptions import KafkaConnectionError
from src.utils.retry import exponential_backoff_retry

logger = logging.getLogger(__name__)


class ResilientKafkaProducer:
    """
    Kafka Producer with retry and Dead Letter Queue support.

    Features:
        - Automatic connection retry
        - Message send retry
        - Failed messages automatically sent to DLQ
        - Synchronous and asynchronous send
        - Connection state tracking

    Usage::

        with ResilientKafkaProducer() as producer:
            producer.send({"key": "value"})
    """

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        on_send_success: Optional[Callable[[Dict[str, Any], Any], None]] = None,
        on_send_failure: Optional[Callable[[Dict[str, Any], Exception], None]] = None,
    ):
        """Initialize producer.

        Args:
            config: Kafka configuration (defaults from environment variables)
            on_send_success: Callback invoked on successful send.
            on_send_failure: Callback invoked on send failure.
        """
        self.config = config or get_settings().kafka
        self.on_send_success = on_send_success
        self.on_send_failure = on_send_failure
        self._producer: Optional[KafkaProducer] = None
        self._connected = False

        # Stats
        self._stats = {"sent": 0, "failed": 0, "sent_to_dlq": 0}

    @property
    def is_connected(self) -> bool:
        """Whether the producer is connected."""
        return self._connected and self._producer is not None

    @property
    def stats(self) -> Dict[str, int]:
        """Get statistics."""
        return self._stats.copy()

    @exponential_backoff_retry(
        max_retries=3,
        base_delay=2.0,
        max_delay=30.0,
        retryable_exceptions=(NoBrokersAvailable, KafkaTimeoutError, ConnectionError),
    )
    def connect(self) -> "ResilientKafkaProducer":
        """Connect to Kafka with retry.

        Returns:
            self (supports method chaining)

        Raises:
            KafkaConnectionError: If connection fails.
        """
        if self._connected and self._producer is not None:
            return self

        try:
            # Parse bootstrap servers
            servers = [s.strip() for s in self.config.bootstrap_servers.split(",")]

            self._producer = KafkaProducer(
                bootstrap_servers=servers,
                # Reliability settings
                acks=self.config.acks,
                retries=self.config.retries,
                max_block_ms=self.config.max_block_ms,
                # Performance settings
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                # Serialization
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )

            self._connected = True
            logger.info(
                "Kafka producer connected successfully",
                extra={"bootstrap_servers": servers},
            )
            return self

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise KafkaConnectionError(f"Failed to connect to Kafka: {e}")

    def send(
        self,
        data: Dict[str, Any],
        topic: Optional[str] = None,
        key: Optional[str] = None,
        sync: bool = True,
    ) -> bool:
        """Send a message to Kafka.

        Args:
            data: Data to send (dict, will be JSON-serialized).
            topic: Kafka topic (defaults to config topic).
            key: Message key for partitioning.
            sync: Whether to wait for acknowledgement (default True).

        Returns:
            True on success, False on failure (message sent to DLQ).
        """
        if not self.is_connected:
            self.connect()

        topic = topic or self.config.topic_user_data
        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            try:
                future = self._producer.send(topic, value=data, key=key)

                if sync:
                    record_metadata = future.get(timeout=10)

                    logger.debug(
                        "Message sent successfully",
                        extra={
                            "topic": record_metadata.topic,
                            "partition": record_metadata.partition,
                            "offset": record_metadata.offset,
                        },
                    )

                self._stats["sent"] += 1

                if self.on_send_success:
                    self.on_send_success(data, record_metadata if sync else None)

                return True

            except KafkaError as e:
                retry_count += 1
                logger.warning(
                    f"Kafka send failed (attempt {retry_count}/{max_retries}): {e}",
                    extra={
                        "topic": topic,
                        "attempt": retry_count,
                        "error_type": type(e).__name__,
                    },
                )

                if retry_count >= max_retries:
                    self._stats["failed"] += 1
                    self._send_to_dlq(data, topic, e, retry_count)

                    if self.on_send_failure:
                        self.on_send_failure(data, e)

                    return False

        return False

    def _send_to_dlq(
        self,
        original_data: Dict[str, Any],
        source_topic: str,
        error: Exception,
        retry_count: int,
    ) -> bool:
        """Send a failed message to the Dead Letter Queue."""
        try:
            dlq_message = DLQMessage.from_error(
                original_message=original_data,
                error=error,
                source_topic=source_topic,
                retry_count=retry_count,
            )

            future = self._producer.send(
                self.config.topic_dlq, value=dlq_message.dict()
            )
            future.get(timeout=10)

            self._stats["sent_to_dlq"] += 1

            logger.info(
                "Message sent to DLQ",
                extra={
                    "dlq_topic": self.config.topic_dlq,
                    "source_topic": source_topic,
                    "error_type": type(error).__name__,
                },
            )
            return True

        except Exception as dlq_error:
            logger.error(
                f"Failed to send message to DLQ: {dlq_error}",
                extra={"original_error": str(error), "dlq_error": str(dlq_error)},
            )
            return False

    def flush(self, timeout: float = 10.0) -> None:
        """Flush all pending messages."""
        if self._producer:
            self._producer.flush(timeout=timeout)
            logger.debug("Producer flushed")

    def close(self) -> None:
        """Close the producer connection after flushing pending messages."""
        if self._producer:
            self.flush()
            self._producer.close()
            self._producer = None
            self._connected = False
            logger.info("Kafka producer closed", extra={"stats": self._stats})

    def __enter__(self) -> "ResilientKafkaProducer":
        """Enter context manager."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit context manager."""
        self.close()
        return False

    def __del__(self):
        """Destructor to ensure resource cleanup."""
        if self._connected:
            self.close()
