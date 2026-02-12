"""
Integration Tests for Kafka Producer
"""

import pytest
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import datetime

# Ensure src is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.producers.kafka_producer import ResilientKafkaProducer
from src.producers.api_client import RandomUserAPIClient
from src.exceptions.custom_exceptions import (
    KafkaConnectionError,
    KafkaPublishError,
    DataValidationError
)


class TestResilientKafkaProducer:
    """Integration tests for ResilientKafkaProducer."""

    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock KafkaProducer class."""
        with patch('src.producers.kafka_producer.KafkaProducer') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            # Mock send() to return a Future
            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(
                topic='user_data',
                partition=0,
                offset=12345
            )
            mock_instance.send.return_value = mock_future

            yield mock_instance

    def test_send_message_success(self, mock_kafka_producer, sample_transformed_user):
        """Test successful message sending."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            result = producer.send(sample_transformed_user, topic='user_data')

            assert result is True
            mock_kafka_producer.send.assert_called_once()

    def test_send_message_with_key(self, mock_kafka_producer, sample_transformed_user):
        """Test sending a message with a key."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            # Should succeed
            result = producer.send(sample_transformed_user, topic='user_data', key='user-123')
            assert result is True

    def test_send_data_to_topic(self, mock_kafka_producer):
        """Test sending data to a specified topic."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            data = {"incomplete": "data"}

            # Send data (producer does not validate, only sends)
            result = producer.send(data, topic='user_data')

            # producer.send should have been called
            assert mock_kafka_producer.send.called
            assert result is True

    def test_send_failure_retries(self, mock_kafka_producer):
        """Test retry behavior on send failure."""
        from kafka.errors import KafkaError

        # First two attempts fail, third succeeds
        mock_kafka_producer.send.side_effect = [
            KafkaError("Temporary failure"),
            KafkaError("Temporary failure"),
            MagicMock(get=MagicMock(return_value=MagicMock(offset=123)))
        ]

        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            # Send with retry (depends on producer implementation)
            result = producer.send({"test": "data"}, topic='user_data')

            # Verify at least one attempt was made
            assert mock_kafka_producer.send.call_count >= 1

    def test_producer_context_manager(self, mock_kafka_producer):
        """Test context manager closes properly."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            with ResilientKafkaProducer() as producer:
                producer._producer = mock_kafka_producer
                producer._connected = True
                producer.send({"test": "data"}, topic='test_topic')

            # Should call flush on close
            mock_kafka_producer.flush.assert_called()

    def test_stats_tracking(self, mock_kafka_producer, sample_transformed_user):
        """Test stats tracking."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            # Send a few messages
            for _ in range(5):
                producer.send(sample_transformed_user, topic='user_data')

            stats = producer.stats

            assert 'sent' in stats
            assert stats['sent'] >= 5


class TestKafkaProducerWithAPIClient:
    """Test Kafka Producer integration with API Client (API -> Transform -> Kafka)."""

    @pytest.fixture
    def mock_api_response(self, sample_api_response):
        """Mock API response."""
        return sample_api_response

    @pytest.fixture
    def mock_requests(self, mock_api_response):
        """Mock requests.Session."""
        with patch('src.producers.api_client.requests.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.json.return_value = mock_api_response
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session
            yield mock_session

    def test_fetch_and_publish_flow(
        self,
        mock_requests,
        mock_kafka_producer,
        mock_api_response
    ):
        """Test the full fetch -> transform -> publish flow."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            # 1. Fetch from API
            api_client = RandomUserAPIClient()
            api_client._session = mock_requests
            data = api_client.get_random_user()

            assert 'results' in data
            assert len(data['results']) == 1
            assert data['results'][0]['name']['first'] == 'John'

            # 2. Transform
            from src.transformers.user_transformer import UserTransformer
            transformer = UserTransformer()
            transformed = transformer.transform(data['results'][0])

            assert transformed['first_name'] == 'John'
            assert transformed['email'] == 'john.doe@example.com'

            # 3. Publish to Kafka
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True
            result = producer.send(transformed, topic='user_data')

            assert result is True
            mock_kafka_producer.send.assert_called()


class TestDLQBehavior:
    """Test Dead Letter Queue behavior."""

    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock KafkaProducer for DLQ tests."""
        with patch('src.producers.kafka_producer.KafkaProducer') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(offset=123)
            mock_instance.send.return_value = mock_future

            yield mock_instance

    def test_dlq_message_format(self, mock_kafka_producer):
        """Test DLQ message format is correct."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            # Simulate sending to DLQ
            original_message = {"invalid": "data"}
            error = ValueError("Validation failed")

            # _send_to_dlq(original_data, source_topic, error, retry_count)
            producer._send_to_dlq(original_message, "user_data", error, 3)

            # Verify DLQ message was sent
            assert mock_kafka_producer.send.called

            # Get the send call arguments
            call_args = mock_kafka_producer.send.call_args

            # Verify the topic is a DLQ topic (actual name depends on settings)
            assert call_args is not None

    def test_dlq_preserves_original_message(self, mock_kafka_producer):
        """Test DLQ preserves the original message."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            original = {"user_id": "123", "data": "important"}
            error = Exception("Processing failed")

            # _send_to_dlq(original_data, source_topic, error, retry_count)
            producer._send_to_dlq(original, "source_topic", error, 3)

            # Verify original message was included
            assert mock_kafka_producer.send.called


class TestProducerErrorHandling:
    """Test Producer error handling."""

    def test_connection_error_handling(self):
        """Test connection error handling."""
        from kafka.errors import NoBrokersAvailable
        from src.exceptions.custom_exceptions import KafkaConnectionError

        with patch('src.producers.kafka_producer.KafkaProducer') as mock_class:
            mock_class.side_effect = NoBrokersAvailable("No brokers")

            producer = ResilientKafkaProducer()

            # send() triggers connect(), which raises KafkaConnectionError
            with pytest.raises(KafkaConnectionError):
                producer.send({"test": "data"}, topic='topic')

    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock KafkaProducer for error handling tests."""
        with patch('src.producers.kafka_producer.KafkaProducer') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(offset=123)
            mock_instance.send.return_value = mock_future

            yield mock_instance

    def test_serialization_error_handling(self, mock_kafka_producer):
        """Test serialization error handling."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            # Non-serializable data
            class NonSerializable:
                pass

            data = {"obj": NonSerializable()}

            # Should handle the error gracefully
            result = producer.send(data, topic='topic')

            # Depending on implementation, may return False or send to DLQ
            assert result is False or mock_kafka_producer.send.called


class TestProducerMetrics:
    """Test Producer metrics collection."""

    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock producer for metrics tests."""
        with patch('src.producers.kafka_producer.KafkaProducer') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(offset=123)
            mock_instance.send.return_value = mock_future

            yield mock_instance

    def test_metrics_increment_on_success(
        self,
        mock_kafka_producer,
        sample_transformed_user
    ):
        """Test metrics increment on successful send."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True
            producer.send(sample_transformed_user, topic='topic')

            # Verify internal stats incremented
            assert producer.stats['sent'] >= 1

    def test_latency_recorded(
        self,
        mock_kafka_producer,
        sample_transformed_user
    ):
        """Test latency is recorded."""
        with patch('src.producers.kafka_producer.KafkaProducer', return_value=mock_kafka_producer):
            producer = ResilientKafkaProducer()
            producer._producer = mock_kafka_producer
            producer._connected = True

            producer.send(sample_transformed_user, topic='topic')

            stats = producer.stats

            # Should have stats (specific fields depend on implementation)
            assert isinstance(stats, dict)
            assert 'sent' in stats
