"""Dead Letter Queue Consumer"""

import json
import time
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass, field

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..config.settings import get_settings
from ..utils.logging_config import get_logger
from ..exceptions.custom_exceptions import KafkaConnectionError

logger = get_logger(__name__)


@dataclass
class DLQMessage:
    """Structured representation of a DLQ message."""
    original_message: Dict[str, Any]
    error_message: str
    error_type: str
    timestamp: str
    topic: str = ""
    partition: int = 0
    offset: int = 0
    retry_count: int = 0

    @property
    def is_retryable(self) -> bool:
        """Whether this message is eligible for retry."""
        retryable_errors = [
            'ConnectionError',
            'TimeoutError',
            'CassandraWriteError',
            'KafkaConnectionError',
        ]
        return self.error_type in retryable_errors and self.retry_count < 3

    @property
    def age_hours(self) -> float:
        """Age of the message in the DLQ, in hours."""
        try:
            msg_time = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
            return (datetime.now(msg_time.tzinfo) - msg_time).total_seconds() / 3600
        except (ValueError, AttributeError):
            return 0.0


@dataclass
class DLQStats:
    """DLQ statistics."""
    total_messages: int = 0
    retryable_count: int = 0
    non_retryable_count: int = 0
    error_type_counts: Dict[str, int] = field(default_factory=dict)
    oldest_message_hours: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_messages': self.total_messages,
            'retryable_count': self.retryable_count,
            'non_retryable_count': self.non_retryable_count,
            'error_type_counts': self.error_type_counts,
            'oldest_message_hours': round(self.oldest_message_hours, 2)
        }


class DLQConsumer:
    """
    Dead Letter Queue Consumer.

    Consumes failed messages from the DLQ, analyzes errors, and
    optionally retries or routes them for manual processing.
    """

    def __init__(
        self,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_commit: bool = False
    ):
        """
        Initialize DLQ Consumer.

        Args:
            topic: DLQ topic name (defaults to settings value).
            group_id: Consumer group ID.
            auto_commit: Whether to auto-commit offsets (False recommended).
        """
        self.settings = get_settings()
        self.topic = topic or self.settings.kafka.dlq_topic
        self.group_id = group_id or f"dlq-consumer-{self.settings.environment}"
        self.auto_commit = auto_commit
        self._consumer: Optional[KafkaConsumer] = None

        logger.info(
            "DLQConsumer initialized",
            extra={
                'topic': self.topic,
                'group_id': self.group_id,
                'auto_commit': self.auto_commit
            }
        )

    def _create_consumer(self) -> KafkaConsumer:
        """Create a Kafka Consumer."""
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=self.auto_commit,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=5000
            )
            logger.info("Kafka consumer created successfully")
            return consumer
        except KafkaError as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise KafkaConnectionError(f"Cannot connect to Kafka: {e}")

    @property
    def consumer(self) -> KafkaConsumer:
        """Lazy initialization of consumer."""
        if self._consumer is None:
            self._consumer = self._create_consumer()
        return self._consumer

    def _parse_dlq_message(self, raw_message) -> DLQMessage:
        """Parse a raw Kafka message into a DLQMessage."""
        value = raw_message.value

        return DLQMessage(
            original_message=value.get('original_message', {}),
            error_message=value.get('error', 'Unknown error'),
            error_type=value.get('error_type', 'Unknown'),
            timestamp=value.get('timestamp', ''),
            topic=value.get('topic', ''),
            partition=raw_message.partition,
            offset=raw_message.offset,
            retry_count=value.get('retry_count', 0)
        )

    def analyze_dlq(self, max_messages: int = 1000) -> DLQStats:
        """
        Analyze messages in the DLQ. Read-only (does not commit).

        Args:
            max_messages: Maximum number of messages to analyze.

        Returns:
            DLQStats with aggregated results.
        """
        stats = DLQStats()
        oldest_hours = 0.0

        logger.info(f"Analyzing DLQ (max {max_messages} messages)...")

        count = 0
        for raw_message in self.consumer:
            if count >= max_messages:
                break

            try:
                msg = self._parse_dlq_message(raw_message)
                stats.total_messages += 1

                if msg.is_retryable:
                    stats.retryable_count += 1
                else:
                    stats.non_retryable_count += 1

                error_type = msg.error_type
                stats.error_type_counts[error_type] = \
                    stats.error_type_counts.get(error_type, 0) + 1

                if msg.age_hours > oldest_hours:
                    oldest_hours = msg.age_hours

                count += 1

            except Exception as e:
                logger.warning(f"Failed to parse DLQ message: {e}")
                continue

        stats.oldest_message_hours = oldest_hours

        logger.info(
            "DLQ analysis complete",
            extra=stats.to_dict()
        )

        logger.info(
            "DLQ gauge update",
            extra={
                'dlq_total_messages': stats.total_messages,
                'dlq_retryable_messages': stats.retryable_count
            }
        )

        return stats

    def process_messages(
        self,
        handler: Callable[[DLQMessage], bool],
        max_messages: Optional[int] = None,
        commit_on_success: bool = True
    ) -> Dict[str, int]:
        """
        Process messages from the DLQ using the provided handler.

        Args:
            handler: Callable that receives a DLQMessage; return True on success.
            max_messages: Maximum messages to process (None = unlimited).
            commit_on_success: Whether to commit offset after successful processing.

        Returns:
            Dict with 'processed' and 'failed' counts.
        """
        processed = 0
        failed = 0

        logger.info(f"Starting to process DLQ messages (max: {max_messages or 'unlimited'})")

        for raw_message in self.consumer:
            if max_messages and processed + failed >= max_messages:
                break

            try:
                msg = self._parse_dlq_message(raw_message)

                success = handler(msg)

                if success:
                    processed += 1
                    if commit_on_success and not self.auto_commit:
                        self.consumer.commit()

                    logger.debug("Incremented dlq_messages_processed")
                    logger.debug(
                        "DLQ message processed",
                        extra={'offset': msg.offset, 'error_type': msg.error_type}
                    )
                else:
                    failed += 1
                    logger.debug("Incremented dlq_messages_failed")

            except Exception as e:
                failed += 1
                logger.error(f"Error processing DLQ message: {e}")
                logger.debug("Incremented dlq_processing_errors")

        logger.info(
            "DLQ processing complete",
            extra={'processed': processed, 'failed': failed}
        )

        return {'processed': processed, 'failed': failed}

    def retry_messages(
        self,
        target_topic: Optional[str] = None,
        max_messages: int = 100,
        only_retryable: bool = True
    ) -> Dict[str, int]:
        """
        Retry DLQ messages by re-sending them to the target topic.

        Args:
            target_topic: Destination topic (defaults to the original topic).
            max_messages: Maximum messages to retry.
            only_retryable: If True, only retry messages where is_retryable is True.

        Returns:
            Dict with 'retried', 'skipped', and 'failed' counts.
        """
        from ..producers.kafka_producer import ResilientKafkaProducer

        stats = {'retried': 0, 'skipped': 0, 'failed': 0}

        logger.info(
            f"Starting DLQ retry (max: {max_messages}, only_retryable: {only_retryable})"
        )

        with ResilientKafkaProducer() as producer:
            count = 0
            for raw_message in self.consumer:
                if count >= max_messages:
                    break

                try:
                    msg = self._parse_dlq_message(raw_message)

                    if only_retryable and not msg.is_retryable:
                        stats['skipped'] += 1
                        logger.debug(
                            f"Skipping non-retryable message",
                            extra={'error_type': msg.error_type, 'retry_count': msg.retry_count}
                        )
                        count += 1
                        continue

                    topic = target_topic or msg.topic or self.settings.kafka.topic

                    msg.original_message['_retry_count'] = msg.retry_count + 1

                    success = producer.send(topic, msg.original_message)

                    if success:
                        stats['retried'] += 1
                        if not self.auto_commit:
                            self.consumer.commit()
                        logger.info(
                            "Message retried successfully",
                            extra={'topic': topic, 'retry_count': msg.retry_count + 1}
                        )
                    else:
                        stats['failed'] += 1

                    count += 1

                except Exception as e:
                    stats['failed'] += 1
                    logger.error(f"Error retrying message: {e}")
                    count += 1

        logger.info("DLQ retry complete", extra=stats)

        return stats

    def get_sample_messages(self, count: int = 5) -> List[DLQMessage]:
        """
        Retrieve sample messages from the DLQ for debugging.

        Args:
            count: Number of messages to retrieve.

        Returns:
            List of DLQMessage instances.
        """
        messages = []

        for raw_message in self.consumer:
            if len(messages) >= count:
                break
            try:
                msg = self._parse_dlq_message(raw_message)
                messages.append(msg)
            except Exception as e:
                logger.warning(f"Failed to parse message: {e}")

        return messages

    def close(self):
        """Close the consumer."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None
            logger.info("DLQ consumer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


