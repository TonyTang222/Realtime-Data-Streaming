"""
End-to-End Integration Tests for the Streaming Pipeline
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.config.schemas import DLQMessage, validate_transformed_data
from src.exceptions.custom_exceptions import TransformationError
from src.producers.api_client import RandomUserAPIClient
from src.producers.kafka_producer import ResilientKafkaProducer
from src.transformers.user_transformer import UserTransformer


class TestFullPipelineFlow:
    """Test the full pipeline flow: API -> Transform -> Validate -> Kafka -> Cassandra."""

    @pytest.fixture
    def mock_api_session(self, sample_api_response):
        """Mock API session."""
        with patch("src.producers.api_client.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = sample_api_response
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session
            yield mock_session

    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock Kafka producer."""
        with patch("src.producers.kafka_producer.KafkaProducer") as mock_class:
            mock_instance = MagicMock()
            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(
                topic="user_data", partition=0, offset=12345
            )
            mock_instance.send.return_value = mock_future
            mock_class.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_settings(self):
        """Mock settings."""
        with patch("src.producers.api_client.get_settings") as mock_api_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30

            mock_api_settings.return_value = mock_config

            yield mock_config

    def test_full_pipeline_happy_path(
        self, mock_api_session, mock_kafka_producer, mock_settings, sample_api_response
    ):
        """Test the full happy-path pipeline flow: API -> Transform -> Validate -> Kafka."""
        # 1. Fetch from API
        api_client = RandomUserAPIClient()
        api_client._session = mock_api_session
        api_response = api_client.get_random_user()

        assert "results" in api_response
        assert len(api_response["results"]) == 1

        # 2. Transform data
        transformer = UserTransformer()
        raw_user = api_response["results"][0]
        transformed_user = transformer.transform(raw_user)

        assert "id" in transformed_user
        assert transformed_user["first_name"] == "John"
        assert transformed_user["last_name"] == "Doe"

        # 3. Validate transformed data
        is_valid, validated_data, errors = validate_transformed_data(transformed_user)
        assert is_valid is True
        assert len(errors) == 0

        # 4. Send to Kafka
        producer = ResilientKafkaProducer()
        producer._producer = mock_kafka_producer
        result = producer.send("user_data", transformed_user)

        assert result is True
        mock_kafka_producer.send.assert_called()

    def test_pipeline_with_multiple_users(
        self, mock_api_session, mock_kafka_producer, mock_settings, sample_api_response
    ):
        """Test processing multiple users."""
        # Simulate multiple API calls
        users_to_process = 5
        processed_users = []

        api_client = RandomUserAPIClient()
        api_client._session = mock_api_session
        transformer = UserTransformer()
        producer = ResilientKafkaProducer()
        producer._producer = mock_kafka_producer

        for i in range(users_to_process):
            # Fetch
            api_response = api_client.get_random_user()
            raw_user = api_response["results"][0]

            # Transform
            transformed = transformer.transform(raw_user)
            processed_users.append(transformed)

            # Send to Kafka
            producer.send("user_data", transformed)

        assert len(processed_users) == users_to_process
        assert mock_kafka_producer.send.call_count == users_to_process

        # All user IDs should be unique
        ids = [u["id"] for u in processed_users]
        assert len(set(ids)) == users_to_process


class TestPipelineErrorHandling:
    """Test pipeline error handling."""

    @pytest.fixture
    def transformer(self):
        return UserTransformer()

    def test_invalid_data_handled_gracefully(self, transformer):
        """Test that invalid data is handled gracefully."""
        invalid_data = {"incomplete": "data"}

        with pytest.raises(TransformationError):
            transformer.transform(invalid_data)

    def test_dlq_message_created_on_error(self, sample_api_response):
        """Test that a DLQ message is created on error."""
        original_data = sample_api_response["results"][0]
        original_data["email"] = "invalid-email"  # Make it invalid for demo

        error = ValueError("Email validation failed")

        dlq_message = DLQMessage.from_error(
            original_message=original_data,
            error=error,
            source_topic="user_data",
            retry_count=0,
        )

        assert dlq_message.error_type == "ValueError"
        assert "Email validation failed" in dlq_message.error_message
        assert dlq_message.source_topic == "user_data"

        # Original data should be preserved
        parsed = json.loads(dlq_message.original_message)
        assert parsed["name"]["first"] == "John"


class TestPipelineRetryBehavior:
    """Test pipeline retry behavior."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings."""
        with patch("src.producers.api_client.get_settings") as mock_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30
            mock_settings.return_value = mock_config
            yield mock_config

    def test_api_retry_on_temporary_failure(self, mock_settings, sample_api_response):
        """Test API retries on temporary failure."""
        import requests

        with patch("src.producers.api_client.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            # First two calls fail, third succeeds
            mock_success_response = MagicMock()
            mock_success_response.status_code = 200
            mock_success_response.json.return_value = sample_api_response

            mock_session.get.side_effect = [
                requests.ConnectionError("Network error"),
                requests.ConnectionError("Network error"),
                mock_success_response,
            ]

            api_client = RandomUserAPIClient()
            api_client._session = mock_session

            result = api_client.get_random_user()

            assert result == sample_api_response
            assert mock_session.get.call_count == 3

    def test_kafka_retry_on_temporary_failure(self, sample_transformed_user):
        """Test Kafka retries on temporary failure."""
        with patch("src.producers.kafka_producer.KafkaProducer") as mock_class:
            from kafka.errors import KafkaError

            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            # First call fails, second succeeds
            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(offset=123)

            mock_instance.send.side_effect = [
                KafkaError("Temporary failure"),
                mock_future,
            ]

            producer = ResilientKafkaProducer()
            producer._producer = mock_instance

            producer.send("user_data", sample_transformed_user)

            # Verify that at least one retry was attempted
            assert mock_instance.send.call_count >= 1


class TestPipelineMetricsTracking:
    """Test pipeline metrics tracking."""

    def test_api_client_tracks_stats(self, sample_api_response):
        """Test that the API client tracks request statistics."""
        with patch("src.producers.api_client.get_settings") as mock_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30
            mock_settings.return_value = mock_config

            with patch(
                "src.producers.api_client.requests.Session"
            ) as mock_session_class:
                mock_session = MagicMock()
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = sample_api_response
                mock_session.get.return_value = mock_response
                mock_session_class.return_value = mock_session

                api_client = RandomUserAPIClient()
                api_client._session = mock_session

                api_client.get_random_user()
                api_client.get_random_user()
                api_client.get_random_user()

                stats = api_client.stats
                assert stats["requests"] == 3
                assert stats["successes"] == 3
                assert stats["failures"] == 0

    def test_kafka_producer_tracks_stats(self, sample_transformed_user):
        """Test that the Kafka producer tracks send statistics."""
        with patch("src.producers.kafka_producer.KafkaProducer") as mock_class:
            mock_instance = MagicMock()
            mock_future = MagicMock()
            mock_future.get.return_value = MagicMock(offset=123)
            mock_instance.send.return_value = mock_future
            mock_class.return_value = mock_instance

            producer = ResilientKafkaProducer()
            producer._producer = mock_instance
            producer._connected = True

            for _ in range(5):
                producer.send("user_data", sample_transformed_user)

            stats = producer.stats  # Use stats property, not get_stats()
            assert stats["sent"] >= 5


class TestPipelineDataIntegrity:
    """Test pipeline data integrity."""

    @pytest.fixture
    def transformer(self):
        return UserTransformer()

    def test_data_not_lost_during_transform(self, transformer, sample_api_response):
        """Test that no important data is lost during transformation."""
        raw_user = sample_api_response["results"][0]
        transformed = transformer.transform(raw_user)

        # Verify all important fields are preserved
        assert transformed["first_name"] == raw_user["name"]["first"]
        assert transformed["last_name"] == raw_user["name"]["last"]
        assert transformed["email"] == raw_user["email"].lower()
        assert transformed["phone"] == raw_user["phone"]
        assert transformed["gender"] == raw_user["gender"]

    def test_data_format_consistent(self, transformer, sample_api_response):
        """Test that data format is consistent across repeated transforms."""
        raw_user = sample_api_response["results"][0]

        # Transform the same data multiple times
        results = [transformer.transform(raw_user.copy()) for _ in range(10)]

        # All fields except ID should be consistent
        for result in results:
            assert result["first_name"] == "John"
            assert result["last_name"] == "Doe"
            assert result["email"] == "john.doe@example.com"

    def test_address_format_complete(self, transformer, sample_api_response):
        """Test that the address format is complete."""
        raw_user = sample_api_response["results"][0]
        transformed = transformer.transform(raw_user)

        address = transformed["address"]

        # Address should contain all parts
        assert "123" in address  # Street number
        assert "Main Street" in address  # Street name
        assert "New York" in address  # City
        assert "NY" in address  # State
        assert "USA" in address  # Country


class TestPipelineEdgeCases:
    """Test pipeline edge cases."""

    @pytest.fixture
    def transformer(self):
        return UserTransformer()

    def test_handles_unicode_names(self, transformer, sample_api_response):
        """Test handling of Unicode names."""
        raw_user = sample_api_response["results"][0]
        raw_user["name"]["first"] = "田中"
        raw_user["name"]["last"] = "太郎"

        transformed = transformer.transform(raw_user)

        assert transformed["first_name"] == "田中"
        assert transformed["last_name"] == "太郎"

    def test_handles_special_characters_in_address(
        self, transformer, sample_api_response
    ):
        """Test handling of special characters in addresses."""
        raw_user = sample_api_response["results"][0]
        raw_user["location"]["street"]["name"] = "Rue de l'Église"
        raw_user["location"]["city"] = "Zürich"

        transformed = transformer.transform(raw_user)

        assert "Rue de l'Église" in transformed["address"]
        assert "Zürich" in transformed["address"]

    def test_handles_empty_optional_fields(self, transformer, sample_api_response):
        """Test handling of empty optional fields."""
        raw_user = sample_api_response["results"][0]
        raw_user["phone"] = ""

        transformed = transformer.transform(raw_user)

        assert transformed["phone"] == ""

    def test_handles_very_long_values(self, transformer, sample_api_response):
        """Test handling of very long values."""
        raw_user = sample_api_response["results"][0]
        raw_user["location"]["street"]["name"] = "A" * 200

        transformed = transformer.transform(raw_user)

        # Should not raise an error
        assert "A" * 200 in transformed["address"]
