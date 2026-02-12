"""
Unit Tests for Retry Logic
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.retry import exponential_backoff_retry


class TestExponentialBackoffRetry:
    """Tests for the exponential_backoff_retry decorator."""

    def test_successful_call_no_retry(self):
        """Test successful call does not retry."""
        mock_func = MagicMock(return_value="success")

        @exponential_backoff_retry(max_retries=3, base_delay=0.01)
        def decorated():
            return mock_func()

        result = decorated()

        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_on_exception(self):
        """Test retries on exception."""
        call_count = 0

        @exponential_backoff_retry(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ValueError,)
        )
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        result = flaky_func()

        assert result == "success"
        assert call_count == 3  # 1 initial + 2 retries

    def test_max_retries_exceeded_raises_exception(self):
        """Test exception raised after max retries exceeded."""
        call_count = 0

        @exponential_backoff_retry(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ValueError,)
        )
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            always_fails()

        assert call_count == 3  # 1 initial + 2 retries

    def test_non_retryable_exception_not_retried(self):
        """Test non-retryable exception is not retried."""
        call_count = 0

        @exponential_backoff_retry(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(ValueError,)
        )
        def raises_type_error():
            nonlocal call_count
            call_count += 1
            raise TypeError("Not retryable")

        with pytest.raises(TypeError):
            raises_type_error()

        assert call_count == 1  # No retry

    def test_on_retry_callback_called(self):
        """Test on_retry callback is called."""
        callback = MagicMock()
        call_count = 0

        @exponential_backoff_retry(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ValueError,),
            on_retry=callback
        )
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Retry me")
            return "success"

        flaky_func()

        assert callback.call_count == 2  # Called once per retry

    def test_on_failure_callback_called(self):
        """Test on_failure callback is called on final failure."""
        failure_callback = MagicMock()

        @exponential_backoff_retry(
            max_retries=1,
            base_delay=0.01,
            retryable_exceptions=(ValueError,),
            on_failure=failure_callback
        )
        def always_fails():
            raise ValueError("Failed")

        with pytest.raises(ValueError):
            always_fails()

        assert failure_callback.call_count == 1

    def test_preserves_function_return_value(self):
        """Test return value is preserved."""
        @exponential_backoff_retry(max_retries=1, base_delay=0.01)
        def returns_dict():
            return {"key": "value", "number": 42}

        result = returns_dict()

        assert result == {"key": "value", "number": 42}

    def test_preserves_function_arguments(self):
        """Test function arguments are preserved."""
        @exponential_backoff_retry(max_retries=1, base_delay=0.01)
        def add_numbers(a, b, c=0):
            return a + b + c

        result = add_numbers(1, 2, c=3)

        assert result == 6


