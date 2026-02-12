"""
Unit Tests for Custom Exceptions
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.exceptions.custom_exceptions import (
    # Base
    StreamingPipelineError,
    # API
    APIError,
    APIConnectionError,
    APITimeoutError,
    APIRateLimitError,
    # Kafka
    KafkaError,
    KafkaConnectionError,
    KafkaPublishError,
    # Cassandra
    CassandraError,
    CassandraConnectionError,
    CassandraWriteError,
    CassandraReadError,
    # Data
    DataValidationError,
    TransformationError,
    # Helper
    is_retryable
)


class TestExceptionHierarchy:
    """Test exception inheritance hierarchy."""

    def test_all_inherit_from_streaming_pipeline_error(self):
        """Test all custom exceptions inherit from StreamingPipelineError."""
        exceptions = [
            APIError,
            APIConnectionError,
            APITimeoutError,
            APIRateLimitError,
            KafkaError,
            KafkaConnectionError,
            KafkaPublishError,
            CassandraError,
            CassandraConnectionError,
            CassandraWriteError,
            DataValidationError,
            TransformationError,
        ]

        for exc_class in exceptions:
            assert issubclass(exc_class, StreamingPipelineError), \
                f"{exc_class.__name__} should inherit from StreamingPipelineError"

    def test_api_exceptions_inherit_from_api_error(self):
        """Test API exceptions inherit from APIError."""
        api_exceptions = [
            APIConnectionError,
            APITimeoutError,
            APIRateLimitError,
        ]

        for exc_class in api_exceptions:
            assert issubclass(exc_class, APIError), \
                f"{exc_class.__name__} should inherit from APIError"

    def test_kafka_exceptions_inherit_from_kafka_error(self):
        """Test Kafka exceptions inherit from KafkaError."""
        kafka_exceptions = [
            KafkaConnectionError,
            KafkaPublishError,
        ]

        for exc_class in kafka_exceptions:
            assert issubclass(exc_class, KafkaError), \
                f"{exc_class.__name__} should inherit from KafkaError"

    def test_cassandra_exceptions_inherit_from_cassandra_error(self):
        """Test Cassandra exceptions inherit from CassandraError."""
        cassandra_exceptions = [
            CassandraConnectionError,
            CassandraWriteError,
        ]

        for exc_class in cassandra_exceptions:
            assert issubclass(exc_class, CassandraError), \
                f"{exc_class.__name__} should inherit from CassandraError"

    def test_data_exceptions_inherit_correctly(self):
        """Test data validation exceptions inherit correctly."""
        assert issubclass(DataValidationError, StreamingPipelineError)
        assert issubclass(TransformationError, StreamingPipelineError)


class TestAPIExceptions:
    """Test API-related exceptions."""

    def test_api_error_basic(self):
        """Test basic APIError functionality."""
        error = APIError("API call failed")
        assert str(error) == "API call failed"
        assert error.message == "API call failed"
        assert error.status_code is None
        assert error.response_body is None

    def test_api_error_with_status_code(self):
        """Test APIError with status code."""
        error = APIError("Bad request", status_code=400)
        assert error.status_code == 400
        assert "[HTTP 400]" in str(error)

    def test_api_error_with_response_body(self):
        """Test APIError with response body."""
        error = APIError("Error", status_code=500, response_body='{"error": "internal"}')
        assert error.response_body == '{"error": "internal"}'

    def test_api_rate_limit_error_with_retry_after(self):
        """Test APIRateLimitError with retry_after."""
        error = APIRateLimitError("Rate limited", retry_after=60)
        assert error.retry_after == 60
        assert error.status_code == 429

    def test_api_rate_limit_error_default_status_code(self):
        """Test APIRateLimitError default status code."""
        error = APIRateLimitError("Rate limited")
        assert error.status_code == 429


class TestKafkaExceptions:
    """Test Kafka-related exceptions."""

    def test_kafka_publish_error_basic(self):
        """Test basic KafkaPublishError functionality."""
        error = KafkaPublishError("Publish failed")
        assert error.message == "Publish failed"
        assert error.topic is None
        assert error.key is None
        assert error.original_data is None

    def test_kafka_publish_error_with_metadata(self):
        """Test KafkaPublishError with metadata."""
        original = {"user_id": "123"}
        error = KafkaPublishError(
            "Publish failed",
            topic="user_data",
            key="user-123",
            original_data=original
        )

        assert error.topic == "user_data"
        assert error.key == "user-123"
        assert error.original_data == original
        assert "[topic=user_data]" in str(error)
        assert "[key=user-123]" in str(error)


class TestCassandraExceptions:
    """Test Cassandra-related exceptions."""

    def test_cassandra_write_error_basic(self):
        """Test basic CassandraWriteError functionality."""
        error = CassandraWriteError("Write failed")
        assert error.message == "Write failed"
        assert error.keyspace is None
        assert error.table is None

    def test_cassandra_write_error_with_metadata(self):
        """Test CassandraWriteError with metadata."""
        error = CassandraWriteError(
            "Write failed",
            keyspace="spark_streams",
            table="created_users"
        )
        assert error.keyspace == "spark_streams"
        assert error.table == "created_users"


class TestDataValidationExceptions:
    """Test data validation exceptions."""

    def test_data_validation_error_basic(self):
        """Test basic DataValidationError functionality."""
        error = DataValidationError("Validation failed")
        assert error.message == "Validation failed"
        assert error.field is None
        assert error.value is None
        assert error.expected_type is None

    def test_data_validation_error_with_field_info(self):
        """Test DataValidationError with field info."""
        error = DataValidationError(
            "Invalid email",
            field="email",
            value="not-an-email",
            expected_type="email"
        )

        assert error.field == "email"
        assert error.value == "not-an-email"
        assert error.expected_type == "email"
        assert "[field=email]" in str(error)
        assert "[value=not-an-email]" in str(error)

    def test_data_validation_error_truncates_long_values(self):
        """Test DataValidationError truncates long values."""
        long_value = "x" * 100
        error = DataValidationError("Invalid", field="data", value=long_value)

        str_repr = str(error)
        # Value should be truncated to 50 characters
        assert len(str_repr) < len(long_value) + 50

    def test_transformation_error_with_source_data(self):
        """Test TransformationError with source data."""
        source = {"incomplete": "data"}
        error = TransformationError("Transform failed", source_data=source)

        assert error.source_data == source


class TestIsRetryable:
    """Test is_retryable() helper function."""

    def test_retryable_exceptions(self):
        """Test exceptions that should be retryable."""
        retryable_errors = [
            APIConnectionError("Connection failed"),
            APITimeoutError("Request timed out"),
            APIRateLimitError("Rate limited"),
            KafkaConnectionError("Cannot connect to broker"),
            CassandraConnectionError("Cannot connect to Cassandra"),
        ]

        for error in retryable_errors:
            assert is_retryable(error) is True, \
                f"{type(error).__name__} should be retryable"

    def test_non_retryable_exceptions(self):
        """Test exceptions that should not be retryable."""
        non_retryable_errors = [
            DataValidationError("Invalid data"),
            TransformationError("Transform failed"),
            KafkaPublishError("Publish failed"),
            CassandraWriteError("Write failed"),
            APIError("Client error", status_code=400),  # 4xx errors
        ]

        for error in non_retryable_errors:
            assert is_retryable(error) is False, \
                f"{type(error).__name__} should not be retryable"

    def test_standard_exceptions_not_retryable(self):
        """Test standard exceptions are not retryable."""
        standard_errors = [
            ValueError("Invalid value"),
            TypeError("Type mismatch"),
            KeyError("Missing key"),
            Exception("Generic error"),
        ]

        for error in standard_errors:
            assert is_retryable(error) is False


class TestExceptionRaising:
    """Test exceptions can be raised and caught correctly."""

    def test_can_catch_by_base_class(self):
        """Test catching by base class."""
        with pytest.raises(StreamingPipelineError):
            raise APIConnectionError("Connection failed")

    def test_can_catch_by_intermediate_class(self):
        """Test catching by intermediate class."""
        with pytest.raises(APIError):
            raise APIRateLimitError("Rate limited")

    def test_can_catch_specific_class(self):
        """Test catching by specific class."""
        with pytest.raises(APIRateLimitError):
            raise APIRateLimitError("Rate limited")

    def test_exception_preserves_traceback(self):
        """Test exception preserves traceback."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise TransformationError("Transform failed") from e
        except TransformationError as e:
            assert e.__cause__ is not None
            assert isinstance(e.__cause__, ValueError)
