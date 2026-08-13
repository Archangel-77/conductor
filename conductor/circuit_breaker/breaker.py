"""Circuit breaker state machine for Conductor.

Implements the per-task-type breaker (``CircuitBreaker``), its
configuration (``CircuitBreakerConfig``), the state enum
(``CircuitState``), and a registry (``CircuitBreakerRegistry``) that
hands out one breaker per task type.

The breaker is **worker-side and in-memory**: each worker tracks its own
consecutive failures, so state is not shared across workers (a DB-backed
registry is future work).  The clock is injectable (``now=``) so tests
can exercise timeout transitions deterministically.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

from conductor.exceptions import CircuitBreakerError

logger = logging.getLogger("conductor.circuit_breaker.breaker")


class CircuitState(str, Enum):
    """Possible states of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Configuration for a circuit breaker.

    Attributes:
        threshold: Number of *consecutive* failures that trips the circuit
            from ``CLOSED`` to ``OPEN``.
        timeout: Seconds the circuit stays ``OPEN`` before it becomes
            ``HALF_OPEN`` and allows probe requests.
        half_open_attempts: Number of probe requests allowed while
            ``HALF_OPEN``; if they all fail the circuit re-opens.
    """

    threshold: int = 5
    timeout: float = 60.0
    half_open_attempts: int = 2

    def __post_init__(self) -> None:
        """Validate the configuration values."""
        self.validate()

    def validate(self) -> None:
        """Validate the configuration values.

        Raises:
            CircuitBreakerError: If any value is out of range.
        """
        if not isinstance(self.threshold, int) or self.threshold < 1:
            raise CircuitBreakerError("threshold must be an int >= 1")
        if self.timeout <= 0:
            raise CircuitBreakerError("timeout must be > 0")
        if not isinstance(self.half_open_attempts, int) or self.half_open_attempts < 1:
            raise CircuitBreakerError("half_open_attempts must be an int >= 1")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return {
            "threshold": self.threshold,
            "timeout": self.timeout,
            "half_open_attempts": self.half_open_attempts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CircuitBreakerConfig:
        """Deserialize from a dictionary produced by ``to_dict()``."""
        return cls(
            threshold=data.get("threshold", 5),
            timeout=data.get("timeout", 60.0),
            half_open_attempts=data.get("half_open_attempts", 2),
        )


class CircuitBreaker:
    """Per-task-type circuit breaker state machine.

    State transitions:

    - ``CLOSED`` → ``OPEN`` after ``threshold`` consecutive failures.
    - ``OPEN`` → ``HALF_OPEN`` after ``timeout`` seconds.
    - ``HALF_OPEN`` → ``CLOSED`` when a probe succeeds, or back to
      ``OPEN`` when all probe attempts fail.

    Args:
        task_type: The task type this breaker protects.
        config: The breaker configuration (defaults applied if ``None``).
        now: Injectable clock returning seconds (defaults to
            ``time.monotonic``) — used to make timeout transitions
            deterministic in tests.
    """

    def __init__(
        self,
        task_type: str,
        config: Optional[CircuitBreakerConfig] = None,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._task_type = task_type
        self._config = config or CircuitBreakerConfig()
        self._now = now
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: Optional[float] = None
        self._half_open_remaining = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def task_type(self) -> str:
        """The task type this breaker protects."""
        return self._task_type

    @property
    def config(self) -> CircuitBreakerConfig:
        """The configuration for this breaker."""
        return self._config

    @property
    def state(self) -> CircuitState:
        """The current circuit state."""
        return self._state

    @property
    def consecutive_failures(self) -> int:
        """The number of consecutive failures recorded."""
        return self._consecutive_failures

    @property
    def is_open(self) -> bool:
        """``True`` when the circuit is strictly ``OPEN`` (denies all)."""
        return self._state is CircuitState.OPEN

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    def allow_request(self) -> bool:
        """Return ``True`` if a request for this task type may proceed.

        - ``CLOSED`` — always allowed.
        - ``OPEN`` — denied until the timeout elapses, at which point the
          circuit becomes ``HALF_OPEN`` and the first probe is allowed.
        - ``HALF_OPEN`` — allowed while probe attempts remain.
        """
        now = self._now()
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            if self._opened_at is None or now - self._opened_at < self._config.timeout:
                return False
            # Timeout elapsed: enter the half-open probe window.
            self._state = CircuitState.HALF_OPEN
            self._half_open_remaining = self._config.half_open_attempts
            self._half_open_remaining -= 1
            logger.debug(
                "Circuit for '%s' is now half-open (%d probe(s) allowed).",
                self._task_type,
                self._half_open_remaining,
            )
            return True
        # HALF_OPEN
        if self._half_open_remaining > 0:
            self._half_open_remaining -= 1
            return True
        return False

    def record_success(self) -> None:
        """Record a successful request.

        Resets the consecutive-failure counter and closes the circuit
        (from ``HALF_OPEN`` or ``OPEN``).
        """
        if self._state is not CircuitState.CLOSED:
            logger.debug(
                "Circuit for '%s' recovered; closing.",
                self._task_type,
            )
            self._state = CircuitState.CLOSED
            self._half_open_remaining = 0
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        """Record a failed request.

        May trip the circuit ``CLOSED → OPEN`` (when the consecutive
        failures reach the threshold) or re-open it from ``HALF_OPEN``
        (when all probe attempts have failed).
        """
        now = self._now()
        if self._state is CircuitState.CLOSED:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._config.threshold:
                self._state = CircuitState.OPEN
                self._opened_at = now
                logger.warning(
                    "Circuit for '%s' is now OPEN after %d consecutive failures.",
                    self._task_type,
                    self._consecutive_failures,
                )
        elif self._state is CircuitState.HALF_OPEN:
            if self._half_open_remaining <= 0:
                self._state = CircuitState.OPEN
                self._opened_at = now
                self._consecutive_failures = 0
                logger.warning(
                    "Circuit for '%s' re-opened after half-open probes failed.",
                    self._task_type,
                )
        # OPEN: no-op (requests are not executed while open).

    def reset(self) -> None:
        """Reset this breaker to ``CLOSED`` with no recorded failures."""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = None
        self._half_open_remaining = 0

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a plain-dictionary snapshot of the breaker state."""
        return {
            "task_type": self._task_type,
            "state": self._state.value,
            "consecutive_failures": self._consecutive_failures,
            "opened_at": self._opened_at,
            "half_open_remaining": self._half_open_remaining,
        }


class CircuitBreakerRegistry:
    """Holds one :class:`CircuitBreaker` per task type.

    Args:
        enabled: Whether the breaker is active.  When ``False``,
            :meth:`get` returns ``None`` and no state is tracked.
        default_config: Config applied to every task type unless
            overridden.
        overrides: Per-task-type config overrides.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        default_config: Optional[CircuitBreakerConfig] = None,
        overrides: Optional[dict[str, CircuitBreakerConfig]] = None,
    ) -> None:
        self._enabled = enabled
        self._default_config = default_config or CircuitBreakerConfig()
        self._overrides = dict(overrides or {})
        self._breakers: dict[str, CircuitBreaker] = {}

    @property
    def enabled(self) -> bool:
        """``True`` when circuit-breaking is active."""
        return self._enabled

    def get(self, task_type: str) -> Optional[CircuitBreaker]:
        """Return the breaker for *task_type* (``None`` when disabled).

        Lazily creates the breaker on first access.
        """
        if not self._enabled:
            return None
        breaker = self._breakers.get(task_type)
        if breaker is None:
            config = self._overrides.get(task_type, self._default_config)
            breaker = CircuitBreaker(task_type, config)
            self._breakers[task_type] = breaker
        return breaker

    def open_types(self) -> list[str]:
        """Return task types whose circuit is not ``CLOSED``.

        Includes ``OPEN`` and ``HALF_OPEN`` circuits.
        """
        return [
            task_type
            for task_type, breaker in self._breakers.items()
            if breaker.state is not CircuitState.CLOSED
        ]

    def snapshot(self) -> list[dict[str, Any]]:
        """Return a snapshot of every breaker (newest first)."""
        return [breaker.snapshot() for breaker in self._breakers.values()]

    def reset(self) -> None:
        """Reset every breaker to ``CLOSED``."""
        for breaker in self._breakers.values():
            breaker.reset()
