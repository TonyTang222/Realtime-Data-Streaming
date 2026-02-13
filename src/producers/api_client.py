"""API Client with Exponential Backoff Retry."""

import logging
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import APIConfig, get_settings
from src.exceptions.custom_exceptions import (
    APIConnectionError,
    APIError,
    APIRateLimitError,
    APITimeoutError,
)
from src.utils.retry import exponential_backoff_retry

logger = logging.getLogger(__name__)


class RandomUserAPIClient:
    """
    randomuser.me API Client with retry support.

    Features:
        - Connection pooling via requests.Session
        - Configurable timeout
        - Automatic retry with exponential backoff
        - Proper error handling and classification

    Usage with context manager::

        with RandomUserAPIClient() as client:
            data = client.get_random_user()
    """

    def __init__(self, config: Optional[APIConfig] = None):
        """Initialize API client.

        Args:
            config: API configuration (defaults from environment variables)
        """
        self.config = config or get_settings().api
        self._session: Optional[requests.Session] = None

        # Stats
        self._stats = {"requests": 0, "successes": 0, "retries": 0, "failures": 0}

    @property
    def stats(self) -> Dict[str, int]:
        """Get statistics."""
        return self._stats.copy()

    def _get_session(self) -> requests.Session:
        """Get or create a requests Session with connection pooling and retry."""
        if self._session is None:
            self._session = requests.Session()

            # Set shared headers
            self._session.headers.update(
                {"Accept": "application/json", "User-Agent": "StreamingPipeline/1.0"}
            )

            # Configure retry adapter for low-level network retries
            retry_strategy = Retry(
                total=2,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)

        return self._session

    @exponential_backoff_retry(
        max_retries=3,
        base_delay=1.0,
        max_delay=30.0,
        retryable_exceptions=(
            APIConnectionError,
            APITimeoutError,
            requests.ConnectionError,
            requests.Timeout,
        ),
    )
    def get_random_user(self) -> Dict[str, Any]:
        """Fetch random user data from the API.

        Returns:
            API response dict containing ``results`` and ``info`` keys.

        Raises:
            APIConnectionError: Connection failure.
            APITimeoutError: Request timed out.
            APIRateLimitError: Rate limit exceeded.
            APIError: Other API errors.
        """
        self._stats["requests"] += 1
        session = self._get_session()

        try:
            response = session.get(
                self.config.base_url, timeout=self.config.timeout_seconds
            )

            # Handle HTTP errors
            if response.status_code == 429:
                # Rate limit
                retry_after = response.headers.get("Retry-After")
                self._stats["failures"] += 1
                raise APIRateLimitError(
                    "API rate limit exceeded",
                    retry_after=int(retry_after) if retry_after else None,
                )

            if response.status_code >= 500:
                # Server error - retryable
                self._stats["retries"] += 1
                raise APIConnectionError(
                    f"Server error: {response.status_code}",
                    status_code=response.status_code,
                    response_body=response.text[:500],
                )

            if response.status_code >= 400:
                # Client error - non-retryable
                self._stats["failures"] += 1
                raise APIError(
                    f"Client error: {response.status_code}",
                    status_code=response.status_code,
                    response_body=response.text[:500],
                )

            # Parse JSON
            try:
                data = response.json()
            except ValueError as e:
                self._stats["failures"] += 1
                raise APIError(
                    f"Invalid JSON response: {e}", response_body=response.text[:500]
                )

            self._stats["successes"] += 1
            logger.debug("API request successful", extra={"url": self.config.base_url})

            return data

        except requests.ConnectionError as e:
            self._stats["retries"] += 1
            raise APIConnectionError(f"Connection failed: {e}")

        except requests.Timeout as e:
            self._stats["retries"] += 1
            raise APITimeoutError(
                f"Request timed out after {self.config.timeout_seconds}s: {e}"
            )

        except requests.RequestException as e:
            self._stats["failures"] += 1
            raise APIError(f"Request failed: {e}")

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
            logger.debug("API client session closed")

    def __enter__(self) -> "RandomUserAPIClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit."""
        self.close()
        return False

    def __del__(self):
        """Destructor - ensure resource cleanup."""
        self.close()
