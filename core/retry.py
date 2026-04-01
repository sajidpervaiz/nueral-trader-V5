"""
Retry Policy with exponential backoff and jitter.
"""

import asyncio
import random
import time
from typing import Callable, Optional, Type, List
from loguru import logger


class RetryPolicy:
    """
    Production-grade retry policy with:
    - Exponential backoff with jitter
    - Max retry attempts
    - Configurable base and max delay
    - Exception filtering
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 10.0,
        exponential_backoff: bool = True,
        jitter: bool = True,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_backoff = exponential_backoff
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or [Exception]

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        delay = self.base_delay

        if self.exponential_backoff:
            delay = self.base_delay * (2 ** (attempt - 1))

        delay = min(delay, self.max_delay)

        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)

        return delay

    def is_retryable(self, exception: Exception) -> bool:
        """Check if exception is retryable."""
        return any(
            isinstance(exception, exc_type)
            for exc_type in self.retryable_exceptions
        )


def with_retry(policy: Optional[RetryPolicy] = None):
    """Decorator for retry logic."""
    if policy is None:
        policy = RetryPolicy()

    def decorator(func: Callable):
        async def async_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, policy.max_attempts + 1):
                try:
                    return await func(*args, **kwargs)

                except Exception as e:
                    last_exception = e

                    if not policy.is_retryable(e):
                        logger.warning(f"Non-retryable exception: {e}")
                        raise

                    if attempt < policy.max_attempts:
                        delay = policy._calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt}/{policy.max_attempts} failed: {e}. "
                            f"Retrying in {delay:.2f}s"
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"All {policy.max_attempts} attempts failed. Last error: {e}"
                        )

            raise last_exception

        def sync_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, policy.max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_exception = e

                    if not policy.is_retryable(e):
                        logger.warning(f"Non-retryable exception: {e}")
                        raise

                    if attempt < policy.max_attempts:
                        delay = policy._calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt}/{policy.max_attempts} failed: {e}. "
                            f"Retrying in {delay:.2f}s"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {policy.max_attempts} attempts failed. Last error: {e}"
                        )

            raise last_exception

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
