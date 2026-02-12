"""Retry Logic with Exponential Backoff"""

import time
import random
import functools
import logging
from typing import Tuple, Type, Callable, Optional, TypeVar

from src.exceptions.custom_exceptions import is_retryable

logger = logging.getLogger(__name__)

# Type variable for generic return type
T = TypeVar('T')


def exponential_backoff_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
    on_failure: Optional[Callable[[Exception, int], None]] = None
) -> Callable:
    """
    Decorator for exponential backoff retry logic.

    Usage::

        @exponential_backoff_retry(
            max_retries=3,
            base_delay=1.0,
            retryable_exceptions=(ConnectionError, TimeoutError)
        )
        def fetch_data():
            response = requests.get(url)
            return response.json()

    Args:
        max_retries: Maximum number of retries (excludes the initial attempt).
        base_delay: Base delay in seconds.
        max_delay: Maximum delay in seconds (caps exponential growth).
        exponential_base: Exponential base (typically 2).
        jitter: Whether to add random jitter to the delay.
        retryable_exceptions: Exception types that trigger a retry.
        on_retry: Callback invoked on each retry.
        on_failure: Callback invoked on final failure.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception: Optional[Exception] = None

            # Try max_retries + 1 times (initial + retries)
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except retryable_exceptions as e:
                    last_exception = e

                    # Last attempt
                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries + 1} attempts",
                            extra={
                                "function": func.__name__,
                                "attempt": attempt + 1,
                                "max_retries": max_retries,
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            }
                        )
                        if on_failure:
                            on_failure(e, attempt + 1)
                        raise

                    # Calculate delay
                    delay = _calculate_delay(
                        attempt=attempt,
                        base_delay=base_delay,
                        max_delay=max_delay,
                        exponential_base=exponential_base,
                        jitter=jitter
                    )

                    # Log retry info
                    logger.warning(
                        f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                        f"after {delay:.2f}s due to {type(e).__name__}: {str(e)}",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "delay_seconds": delay,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        }
                    )

                    # Invoke on_retry callback
                    if on_retry:
                        on_retry(e, attempt + 1)

                    # Wait before retrying
                    time.sleep(delay)

            # Unreachable in practice; satisfies the type checker
            raise last_exception  # type: ignore

        return wrapper
    return decorator


def _calculate_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    exponential_base: float,
    jitter: bool
) -> float:
    """Calculate delay: min(base_delay * (exponential_base ** attempt), max_delay)."""
    # Base exponential delay
    delay = base_delay * (exponential_base ** attempt)

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter (0-50% random increase)
    if jitter:
        jitter_factor = 1 + random.uniform(0, 0.5)
        delay = delay * jitter_factor

    return delay
