"""
Unit tests for the circuit breaker state machine.

Pure in-memory tests — no database required.  An injectable clock makes
timeout (``OPEN`` → ``HALF_OPEN``) transitions deterministic.
"""

# pylint: disable=missing-class-docstring,missing-function-docstring

from __future__ import annotations

import pytest

from conductor.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitState,
)
from conductor.exceptions import CircuitBreakerError


class FakeClock:
    """Deterministic clock for testing timeout transitions."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def make_breaker(
    config: CircuitBreakerConfig,
    clock: FakeClock,
) -> CircuitBreaker:
    """Build a breaker for ``api.call`` with the given clock."""
    return CircuitBreaker("api.call", config, now=clock)


def current_state(breaker: CircuitBreaker) -> CircuitState:
    """Return ``breaker.state`` via a call.

    Reading the state through a function breaks the type checker's
    narrowing, so a later assertion on a different state is not flagged
    as non-overlapping (the checker otherwise keeps the first asserted
    enum member for the whole test method).
    """
    return breaker.state


# ===================================================================
# CircuitState
# ===================================================================


class TestCircuitState:

    def test_values(self) -> None:
        assert CircuitState.CLOSED.value == "closed"
        assert CircuitState.OPEN.value == "open"
        assert CircuitState.HALF_OPEN.value == "half_open"

    def test_str(self) -> None:
        assert str(CircuitState.OPEN) == "open"


# ===================================================================
# CircuitBreakerConfig
# ===================================================================


class TestCircuitBreakerConfig:

    def test_defaults(self) -> None:
        cfg = CircuitBreakerConfig()
        assert cfg.threshold == 5
        assert cfg.timeout == 60.0
        assert cfg.half_open_attempts == 2

    def test_custom_values(self) -> None:
        cfg = CircuitBreakerConfig(threshold=3, timeout=5.5, half_open_attempts=4)
        assert cfg.threshold == 3
        assert cfg.timeout == 5.5
        assert cfg.half_open_attempts == 4

    def test_invalid_threshold(self) -> None:
        with pytest.raises(CircuitBreakerError, match="threshold"):
            CircuitBreakerConfig(threshold=0)

    def test_invalid_timeout(self) -> None:
        with pytest.raises(CircuitBreakerError, match="timeout"):
            CircuitBreakerConfig(timeout=0)

    def test_invalid_half_open_attempts(self) -> None:
        with pytest.raises(CircuitBreakerError, match="half_open_attempts"):
            CircuitBreakerConfig(half_open_attempts=0)

    def test_to_from_dict_round_trip(self) -> None:
        cfg = CircuitBreakerConfig(threshold=2, timeout=5.0, half_open_attempts=3)
        assert CircuitBreakerConfig.from_dict(cfg.to_dict()) == cfg

    def test_from_dict_defaults(self) -> None:
        cfg = CircuitBreakerConfig.from_dict({})
        assert cfg.threshold == 5
        assert cfg.timeout == 60.0
        assert cfg.half_open_attempts == 2


# ===================================================================
# CircuitBreaker state machine
# ===================================================================


class TestCircuitBreakerStateMachine:

    def test_closed_allows_requests(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=3), clock)
        assert current_state(b) is CircuitState.CLOSED
        assert b.allow_request() is True
        assert b.allow_request() is True

    def test_opens_after_threshold_failures(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=3), clock)
        b.record_failure()
        b.record_failure()
        assert current_state(b) is CircuitState.CLOSED
        b.record_failure()
        assert current_state(b) is CircuitState.OPEN
        assert b.is_open is True

    def test_success_resets_failure_count(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=3), clock)
        b.record_failure()
        b.record_failure()
        b.record_success()
        assert b.consecutive_failures == 0
        assert current_state(b) is CircuitState.CLOSED
        # Failures are no longer consecutive.
        b.record_failure()
        b.record_failure()
        assert current_state(b) is CircuitState.CLOSED

    def test_open_denies_until_timeout(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=1, timeout=10.0), clock)
        b.record_failure()
        assert current_state(b) is CircuitState.OPEN
        assert b.allow_request() is False
        clock.advance(5)
        assert b.allow_request() is False  # still within the timeout
        clock.advance(6)  # now t=11 > timeout
        assert b.allow_request() is True  # half-open probe allowed
        assert current_state(b) is CircuitState.HALF_OPEN

    def test_half_open_probe_success_closes(self) -> None:
        clock = FakeClock()
        b = make_breaker(
            CircuitBreakerConfig(threshold=1, timeout=10.0, half_open_attempts=2),
            clock,
        )
        b.record_failure()  # open at t=0
        clock.advance(11)
        assert b.allow_request() is True  # probe 1
        b.record_success()
        assert current_state(b) is CircuitState.CLOSED
        assert b.consecutive_failures == 0

    def test_half_open_all_probes_fail_reopens(self) -> None:
        clock = FakeClock()
        b = make_breaker(
            CircuitBreakerConfig(threshold=1, timeout=10.0, half_open_attempts=2),
            clock,
        )
        b.record_failure()
        clock.advance(11)
        assert b.allow_request() is True  # probe 1 (2 -> 1 remaining)
        b.record_failure()
        assert current_state(b) is CircuitState.HALF_OPEN  # still one probe left
        assert b.allow_request() is True  # probe 2 (1 -> 0 remaining)
        b.record_failure()  # all probes failed -> re-open
        assert current_state(b) is CircuitState.OPEN
        assert b.allow_request() is False

    def test_half_open_denies_when_attempts_exhausted(self) -> None:
        clock = FakeClock()
        b = make_breaker(
            CircuitBreakerConfig(threshold=1, timeout=10.0, half_open_attempts=1),
            clock,
        )
        b.record_failure()
        clock.advance(11)
        assert b.allow_request() is True  # the only probe
        assert b.allow_request() is False  # no attempts remain

    def test_record_failure_while_open_is_noop(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=1, timeout=10.0), clock)
        b.record_failure()
        assert current_state(b) is CircuitState.OPEN
        b.record_failure()  # defensive no-op
        assert current_state(b) is CircuitState.OPEN
        assert b.consecutive_failures == 1

    def test_reset(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=1, timeout=10.0), clock)
        b.record_failure()
        assert b.is_open is True
        b.reset()
        assert current_state(b) is CircuitState.CLOSED
        assert b.consecutive_failures == 0
        assert b.allow_request() is True

    def test_snapshot(self) -> None:
        clock = FakeClock()
        b = make_breaker(CircuitBreakerConfig(threshold=1, timeout=10.0), clock)
        b.record_failure()
        snap = b.snapshot()
        assert snap["task_type"] == "api.call"
        assert snap["state"] == "open"
        assert snap["consecutive_failures"] == 1
        assert snap["opened_at"] == 0.0
        assert snap["half_open_remaining"] == 0


# ===================================================================
# CircuitBreakerRegistry
# ===================================================================


class TestCircuitBreakerRegistry:

    def test_disabled_returns_none(self) -> None:
        reg = CircuitBreakerRegistry(enabled=False)
        assert reg.get("api.call") is None
        assert reg.enabled is False
        assert reg.open_types() == []
        assert reg.snapshot() == []

    def test_default_config_shared(self) -> None:
        reg = CircuitBreakerRegistry(enabled=True)
        b1 = reg.get("a")
        b2 = reg.get("b")
        assert b1 is not None and b2 is not None
        assert b1 is not b2
        assert b1.config == CircuitBreakerConfig()

    def test_same_type_returns_same_breaker(self) -> None:
        reg = CircuitBreakerRegistry(enabled=True)
        assert reg.get("a") is reg.get("a")

    def test_overrides(self) -> None:
        reg = CircuitBreakerRegistry(
            enabled=True,
            default_config=CircuitBreakerConfig(threshold=10),
            overrides={"a": CircuitBreakerConfig(threshold=2)},
        )
        breaker_a = reg.get("a")
        breaker_b = reg.get("b")
        assert breaker_a is not None and breaker_b is not None
        assert breaker_a.config.threshold == 2
        assert breaker_b.config.threshold == 10

    def test_open_types(self) -> None:
        reg = CircuitBreakerRegistry(
            enabled=True,
            default_config=CircuitBreakerConfig(
                threshold=1,
                timeout=10.0,
                half_open_attempts=1,
            ),
        )
        breaker = reg.get("a")
        assert breaker is not None
        breaker.record_failure()
        reg.get("b")
        assert reg.open_types() == ["a"]

    def test_snapshot_and_reset(self) -> None:
        reg = CircuitBreakerRegistry(
            enabled=True,
            default_config=CircuitBreakerConfig(
                threshold=1,
                timeout=10.0,
                half_open_attempts=1,
            ),
        )
        breaker = reg.get("a")
        assert breaker is not None
        breaker.record_failure()
        assert len(reg.snapshot()) == 1
        assert reg.open_types() == ["a"]
        reg.reset()
        assert reg.open_types() == []
