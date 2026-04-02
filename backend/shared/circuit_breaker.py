"""
Circuit Breaker pattern implementation for API resilience.

Prevents cascading failures by "opening" the circuit when an API
is failing repeatedly, giving it time to recover.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Callable, TypeVar, Any
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation, requests pass through
    OPEN = "open"          # Failing, requests are rejected immediately
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker that tracks failures and opens circuit when threshold is reached.

    States:
    - CLOSED: Normal operation, failures are counted
    - OPEN: Circuit is tripped, requests fail fast
    - HALF_OPEN: Testing recovery, allows limited requests through
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def can_proceed(self) -> bool:
        """Check if a request can proceed through the circuit."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    logger.info(f"Circuit breaker '{self.name}': transitioning OPEN -> HALF_OPEN")
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                # Allow limited calls in half-open state
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False

            return False

    async def record_success(self) -> None:
        """Record a successful call."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                logger.info(f"Circuit breaker '{self.name}': HALF_OPEN -> CLOSED (recovered)")
                self._state = CircuitState.CLOSED
                self._failure_count = 0
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                if self._failure_count > 0:
                    self._failure_count = max(0, self._failure_count - 1)

    async def record_failure(self) -> None:
        """Record a failed call."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(f"Circuit breaker '{self.name}': HALF_OPEN -> OPEN (still failing)")
                self._state = CircuitState.OPEN

            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    logger.warning(
                        f"Circuit breaker '{self.name}': CLOSED -> OPEN "
                        f"(failures={self._failure_count}/{self.failure_threshold})"
                    )
                    self._state = CircuitState.OPEN

    def get_state(self) -> str:
        """Get current state as string."""
        return self._state.value


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open and request cannot proceed."""

    def __init__(self, circuit_name: str, message: str = None):
        self.circuit_name = circuit_name
        self.message = message or f"Circuit breaker '{circuit_name}' is open"
        super().__init__(self.message)


def circuit_breaker_protected(
    breaker: CircuitBreaker,
    fallback: Callable[..., T] = None,
    fallback_return: Any = None,
):
    """
    Decorator that protects a function with a circuit breaker.

    Usage:
        @circuit_breaker_protected(my_breaker)
        async def call_api():
            return await http_client.get(url)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            # Check if circuit allows request
            if not await breaker.can_proceed():
                logger.warning(f"Circuit breaker '{breaker.name}' is OPEN, failing fast")
                if fallback:
                    logger.info(f"Circuit breaker '{breaker.name}': using fallback")
                    return fallback(*args, **kwargs)
                raise CircuitBreakerError(breaker.name)

            try:
                result = await func(*args, **kwargs)
                await breaker.record_success()
                return result
            except Exception as e:
                await breaker.record_failure()
                raise

        return wrapper
    return decorator


# Global circuit breakers for each upstream API
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str, **kwargs) -> CircuitBreaker:
    """Get or create a circuit breaker for an API."""
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name=name, **kwargs)
    return _circuit_breakers[name]


def get_all_breakers() -> dict[str, CircuitBreaker]:
    """Get all registered circuit breakers."""
    return _circuit_breakers.copy()
