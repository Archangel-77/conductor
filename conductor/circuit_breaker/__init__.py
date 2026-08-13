"""Circuit breaker for Conductor.

Provides a per-task-type circuit breaker that protects workers from
repeatedly executing a handler that keeps failing.  A breaker tracks
*consecutive* failures: after ``threshold`` consecutive failures the
circuit trips ``CLOSED → OPEN`` and the worker skips execution of that
task type (tasks stay pending).  After ``timeout`` seconds the circuit
becomes ``HALF_OPEN`` and lets a limited number of probe executions
through; a successful probe closes the circuit, all probes failing
re-opens it.

Typical usage::

    from conductor.circuit_breaker import CircuitBreaker, CircuitBreakerConfig

    breaker = CircuitBreaker("api.call", CircuitBreakerConfig(threshold=5, timeout=60))
    if breaker.allow_request():
        result = await handler(payload)
        breaker.record_success() if ok else breaker.record_failure()
"""

from __future__ import annotations

from conductor.circuit_breaker.breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitState,
)

__all__: list[str] = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "CircuitState",
]
