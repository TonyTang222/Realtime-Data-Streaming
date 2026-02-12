"""
Unit Tests for Random User API Client
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.producers.api_client import RandomUserAPIClient
from src.exceptions.custom_exceptions import (
    APIError,
    APIConnectionError,
    APITimeoutError,
    APIRateLimitError
)


class TestRandomUserAPIClient:
    """Unit tests for RandomUserAPIClient."""

    @pytest.fixture
    def mock_session(self):
        """Mock requests.Session."""
        with patch('src.producers.api_client.requests.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session
            yield mock_session

    @pytest.fixture
    def client(self):
        """Create an API client instance."""
        with patch('src.producers.api_client.get_settings') as mock_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30
            mock_settings.return_value = mock_config
            return RandomUserAPIClient()

    # ============================================================
    # Happy Path Tests
    # ============================================================

    def test_get_random_user_success(self, client, mock_session, sample_api_response):
        """Test successful random user retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_session.get.return_value = mock_response

        client._session = mock_session
        result = client.get_random_user()

        assert result == sample_api_response
        assert 'results' in result
        assert len(result['results']) == 1
        mock_session.get.assert_called_once()

    def test_stats_increment_on_success(self, client, mock_session, sample_api_response):
        """Test stats increment on success."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_session.get.return_value = mock_response

        client._session = mock_session
        initial_requests = client.stats['requests']
        initial_successes = client.stats['successes']

        client.get_random_user()

        assert client.stats['requests'] == initial_requests + 1
        assert client.stats['successes'] == initial_successes + 1

    def test_session_lazy_initialization(self, client):
        """Test session lazy initialization."""
        assert client._session is None

        with patch('src.producers.api_client.requests.Session') as mock_class:
            mock_class.return_value = MagicMock()
            session = client._get_session()
            assert session is not None
            mock_class.assert_called_once()

    def test_session_reused(self, client):
        """Test session is reused across calls."""
        with patch('src.producers.api_client.requests.Session') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            session1 = client._get_session()
            session2 = client._get_session()

            assert session1 is session2
            mock_class.assert_called_once()

    # ============================================================
    # Error Handling Tests
    # ============================================================

    def test_connection_error_raises_api_connection_error(self, client, mock_session):
        """Test connection error raises APIConnectionError."""
        mock_session.get.side_effect = requests.ConnectionError("Connection refused")
        client._session = mock_session

        with pytest.raises(APIConnectionError):
            client.get_random_user()

        assert client.stats['retries'] >= 1

    def test_timeout_error_raises_api_timeout_error(self, client, mock_session):
        """Test timeout raises APITimeoutError."""
        mock_session.get.side_effect = requests.Timeout("Request timed out")
        client._session = mock_session

        with pytest.raises(APITimeoutError):
            client.get_random_user()

        assert client.stats['retries'] >= 1

    def test_rate_limit_raises_api_rate_limit_error(self, client, mock_session):
        """Test 429 status code raises APIRateLimitError."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {'Retry-After': '60'}
        mock_session.get.return_value = mock_response
        client._session = mock_session

        with pytest.raises(APIRateLimitError) as exc_info:
            client.get_random_user()

        assert exc_info.value.retry_after == 60
        assert client.stats['failures'] >= 1

    def test_server_error_raises_api_connection_error(self, client, mock_session):
        """Test 5xx status code raises APIConnectionError (retryable)."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.get.return_value = mock_response
        client._session = mock_session

        with pytest.raises(APIConnectionError):
            client.get_random_user()

    def test_client_error_raises_api_error(self, client, mock_session):
        """Test 4xx status code raises APIError (non-retryable)."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_session.get.return_value = mock_response
        client._session = mock_session

        with pytest.raises(APIError) as exc_info:
            client.get_random_user()

        assert exc_info.value.status_code == 400
        assert client.stats['failures'] >= 1

    def test_invalid_json_raises_api_error(self, client, mock_session):
        """Test invalid JSON raises APIError."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.text = "not json"
        mock_session.get.return_value = mock_response
        client._session = mock_session

        with pytest.raises(APIError):
            client.get_random_user()

    # ============================================================
    # Context Manager Tests
    # ============================================================

    def test_context_manager_closes_session(self):
        """Test context manager properly closes session."""
        with patch('src.producers.api_client.get_settings') as mock_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30
            mock_settings.return_value = mock_config

            with patch('src.producers.api_client.requests.Session') as mock_session_class:
                mock_session = MagicMock()
                mock_session_class.return_value = mock_session

                with RandomUserAPIClient() as client:
                    client._get_session()  # Initialize session

                mock_session.close.assert_called_once()

    def test_close_clears_session(self, client):
        """Test close() clears the session."""
        with patch('src.producers.api_client.requests.Session') as mock_class:
            mock_session = MagicMock()
            mock_class.return_value = mock_session

            client._get_session()
            assert client._session is not None

            client.close()
            assert client._session is None
            mock_session.close.assert_called_once()

    def test_close_idempotent(self, client):
        """Test multiple close() calls are safe."""
        client.close()
        client.close()
        client.close()
        # Should not raise any errors


class TestAPIClientRetryBehavior:
    """Tests for API Client retry behavior."""

    @pytest.fixture
    def client_with_mock_settings(self):
        """Create a client with mock settings."""
        with patch('src.producers.api_client.get_settings') as mock_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30
            mock_settings.return_value = mock_config
            yield RandomUserAPIClient()

    def test_retry_on_connection_error(self, client_with_mock_settings, sample_api_response):
        """Test retries on connection error."""
        client = client_with_mock_settings

        with patch('src.producers.api_client.requests.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            # First two attempts fail, third succeeds
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = sample_api_response

            mock_session.get.side_effect = [
                requests.ConnectionError("First failure"),
                requests.ConnectionError("Second failure"),
                mock_response
            ]

            client._session = mock_session
            result = client.get_random_user()

            assert result == sample_api_response
            assert mock_session.get.call_count == 3

    def test_retry_exhaustion_raises_error(self, client_with_mock_settings):
        """Test error raised after retries exhausted."""
        client = client_with_mock_settings

        with patch('src.producers.api_client.requests.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            mock_session.get.side_effect = requests.ConnectionError("Persistent failure")
            client._session = mock_session

            with pytest.raises(APIConnectionError):
                client.get_random_user()

            # Should have retried max_retries times
            assert mock_session.get.call_count >= 1


class TestAPIClientHeaders:
    """Tests for API Client header configuration."""

    def test_session_has_correct_headers(self):
        """Test session has correct headers configured."""
        with patch('src.producers.api_client.get_settings') as mock_settings:
            mock_config = MagicMock()
            mock_config.api.base_url = "https://randomuser.me/api/"
            mock_config.api.timeout_seconds = 30
            mock_settings.return_value = mock_config

            with patch('src.producers.api_client.requests.Session') as mock_session_class:
                mock_session = MagicMock()
                mock_session.headers = MagicMock()  # Use MagicMock for headers
                mock_session_class.return_value = mock_session

                client = RandomUserAPIClient()
                client._get_session()

                # Verify headers.update was called
                mock_session.headers.update.assert_called()
                call_args = mock_session.headers.update.call_args[0][0]
                assert 'Accept' in call_args
                assert call_args['Accept'] == 'application/json'
