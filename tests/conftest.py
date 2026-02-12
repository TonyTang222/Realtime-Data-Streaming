"""
Pytest Configuration and Fixtures
"""

import pytest
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Dict, Any

# Ensure src is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ============================================================
# Sample Data Fixtures
# ============================================================

@pytest.fixture
def sample_api_response() -> Dict[str, Any]:
    """Sample response from the randomuser.me API."""
    return {
        "results": [{
            "gender": "male",
            "name": {
                "title": "Mr",
                "first": "John",
                "last": "Doe"
            },
            "location": {
                "street": {
                    "number": 123,
                    "name": "Main Street"
                },
                "city": "New York",
                "state": "NY",
                "country": "USA",
                "postcode": "10001"
            },
            "email": "john.doe@example.com",
            "login": {
                "uuid": "abc123",
                "username": "johndoe",
                "password": "secret",
                "salt": "xyz",
                "md5": "...",
                "sha1": "...",
                "sha256": "..."
            },
            "dob": {
                "date": "1990-01-15T10:30:00.000Z",
                "age": 34
            },
            "registered": {
                "date": "2020-05-20T08:00:00.000Z",
                "age": 4
            },
            "phone": "555-1234",
            "cell": "555-5678",
            "picture": {
                "large": "https://example.com/large.jpg",
                "medium": "https://example.com/medium.jpg",
                "thumbnail": "https://example.com/thumb.jpg"
            },
            "nat": "US"
        }],
        "info": {
            "seed": "test",
            "results": 1,
            "page": 1,
            "version": "1.4"
        }
    }


@pytest.fixture
def sample_transformed_user() -> Dict[str, Any]:
    """Sample transformed user data for Kafka and Cassandra tests."""
    return {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "first_name": "John",
        "last_name": "Doe",
        "gender": "male",
        "address": "123 Main Street, New York 10001, NY, USA",
        "email": "john.doe@example.com",
        "username": "johndoe",
        "dob": "1990-01-15T10:30:00.000Z",
        "registered_date": "2020-05-20T08:00:00.000Z",
        "phone": "555-1234",
        "picture": "https://example.com/medium.jpg"
    }


@pytest.fixture
def invalid_api_response() -> Dict[str, Any]:
    """Invalid API response for error handling tests."""
    return {
        "results": [{
            "gender": "unknown",  # Invalid gender
            "name": {
                "first": "",      # Empty name
                "last": ""
            },
            "email": "not-an-email",  # Invalid email
            # Missing other required fields
        }]
    }


@pytest.fixture
def empty_api_response() -> Dict[str, Any]:
    """Empty API response."""
    return {"results": []}


@pytest.fixture
def malformed_api_response() -> str:
    """Malformed response (not a dict)."""
    return "this is not json"


# ============================================================
# Mock Fixtures
# ============================================================

@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka Producer."""
    with patch('kafka.KafkaProducer') as mock_class:
        # Create mock instance
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


@pytest.fixture
def mock_cassandra_session():
    """Mock Cassandra Session."""
    with patch('cassandra.cluster.Cluster') as mock_cluster:
        mock_session = MagicMock()
        mock_cluster.return_value.connect.return_value = mock_session
        yield mock_session


@pytest.fixture
def mock_requests_get():
    """Mock requests.get()."""
    with patch('requests.get') as mock_get:
        yield mock_get


@pytest.fixture
def mock_requests_session():
    """Mock requests.Session."""
    with patch('requests.Session') as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        yield mock_session


# ============================================================
# Environment Fixtures
# ============================================================

@pytest.fixture
def test_settings():
    """Test settings that override defaults."""
    with patch.dict('os.environ', {
        'ENVIRONMENT': 'development',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'KAFKA_TOPIC_USER_DATA': 'test_user_data',
        'KAFKA_TOPIC_DLQ': 'test_user_data_dlq',
        'CASSANDRA_HOSTS': 'localhost',
        'LOG_LEVEL': 'DEBUG',
        'LOG_JSON_FORMAT': 'false',
    }):
        # Reload settings
        from src.config.settings import reload_settings
        settings = reload_settings()
        yield settings


# ============================================================
# Utility Fixtures
# ============================================================

@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    """Create a temporary checkpoint directory."""
    checkpoint_dir = tmp_path / "checkpoint"
    checkpoint_dir.mkdir()
    return str(checkpoint_dir)
