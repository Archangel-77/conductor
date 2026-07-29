"""
Unit tests for backoff strategy classes and the ``calculate_backoff_delay``
function.

All tests run fast with no external dependencies.
"""

from __future__ import annotations

import pytest

from conductor.core.models import (
    BackoffStrategyType,
    ExponentialBackoff,
    FixedBackoff,
    LinearBackoff,
)
from conductor.core.worker import calculate_backoff_delay

# ===================================================================
# ExponentialBackoff
# ===================================================================


class TestExponentialBackoff:
    """Verify the exponential backoff formula:
    ``delay = initial_delay * 2 ** (attempt - 1)``, capped at ``max_delay``.
    """

    def test_sequence_with_defaults(self) -> None:
        """With initial_delay=1.0: attempt 0→1, 1→2, 2→4, 3→8, 4→16."""
        strategy = ExponentialBackoff()
        assert strategy.calculate_delay(0) == 1.0
        assert strategy.calculate_delay(1) == 2.0
        assert strategy.calculate_delay(2) == 4.0
        assert strategy.calculate_delay(3) == 8.0
        assert strategy.calculate_delay(4) == 16.0

    def test_custom_initial_delay(self) -> None:
        """With initial_delay=0.5: attempt 0→0.5, 1→1.0, 2→2.0."""
        strategy = ExponentialBackoff(initial_delay=0.5)
        assert strategy.calculate_delay(0) == 0.5
        assert strategy.calculate_delay(1) == 1.0
        assert strategy.calculate_delay(2) == 2.0

    def test_max_delay_capping(self) -> None:
        """Delay should never exceed ``max_delay``."""
        strategy = ExponentialBackoff(initial_delay=1.0, max_delay=10.0)
        # Without cap: attempt 4 = 16.0, attempt 5 = 32.0
        assert strategy.calculate_delay(4) == 10.0  # capped
        assert strategy.calculate_delay(5) == 10.0  # capped

    def test_default_max_delay(self) -> None:
        """Default ``max_delay`` is 3600.0."""
        strategy = ExponentialBackoff()
        assert strategy.max_delay == 3600.0


# ===================================================================
# LinearBackoff
# ===================================================================


class TestLinearBackoff:
    """Verify the linear backoff formula:
    ``delay = initial_delay + (initial_delay * (attempt - 1))``,
    capped at ``max_delay``.
    """

    def test_sequence_with_defaults(self) -> None:
        """With initial_delay=1.0: attempt 0→1, 1→2, 2→3, 3→4, 4→5."""
        strategy = LinearBackoff()
        assert strategy.calculate_delay(0) == 1.0
        assert strategy.calculate_delay(1) == 2.0
        assert strategy.calculate_delay(2) == 3.0
        assert strategy.calculate_delay(3) == 4.0
        assert strategy.calculate_delay(4) == 5.0

    def test_custom_initial_delay(self) -> None:
        """With initial_delay=5.0: attempt 0→5, 1→10, 2→15, 3→20."""
        strategy = LinearBackoff(initial_delay=5.0)
        assert strategy.calculate_delay(0) == 5.0
        assert strategy.calculate_delay(1) == 10.0
        assert strategy.calculate_delay(2) == 15.0
        assert strategy.calculate_delay(3) == 20.0

    def test_max_delay_capping(self) -> None:
        """Delay should never exceed ``max_delay``."""
        strategy = LinearBackoff(initial_delay=10.0, max_delay=25.0)
        # Without cap: attempt 2 = 30.0, attempt 3 = 40.0
        assert strategy.calculate_delay(2) == 25.0  # capped
        assert strategy.calculate_delay(3) == 25.0  # capped

    def test_default_max_delay(self) -> None:
        """Default ``max_delay`` is 3600.0."""
        strategy = LinearBackoff()
        assert strategy.max_delay == 3600.0


# ===================================================================
# FixedBackoff
# ===================================================================


class TestFixedBackoff:
    """Verify fixed backoff always returns ``initial_delay``
    regardless of attempt number, capped at ``max_delay``.
    """

    def test_always_returns_initial_delay(self) -> None:
        """All attempt numbers return the same delay."""
        strategy = FixedBackoff(initial_delay=5.0)
        assert strategy.calculate_delay(1) == 5.0
        assert strategy.calculate_delay(2) == 5.0
        assert strategy.calculate_delay(10) == 5.0
        assert strategy.calculate_delay(100) == 5.0

    def test_capped_by_max_delay(self) -> None:
        """If ``initial_delay > max_delay``, the result is capped."""
        strategy = FixedBackoff(initial_delay=10.0, max_delay=5.0)
        assert strategy.calculate_delay(1) == 5.0

    def test_default_max_delay(self) -> None:
        """Default ``max_delay`` is 3600.0."""
        strategy = FixedBackoff()
        assert strategy.max_delay == 3600.0


# ===================================================================
# calculate_backoff_delay (module-level function)
# ===================================================================


class TestCalculateBackoffDelay:
    """Verify the module-level ``calculate_backoff_delay`` helper."""

    def test_exponential_strategy(self) -> None:
        """``strategy="exponential"`` matches ``ExponentialBackoff``."""
        result = calculate_backoff_delay(
            attempt=3,
            strategy="exponential",
            initial_delay=1.0,
            max_delay=3600.0,
        )
        assert result == 4.0  # 1.0 * 2 ** (3-1)

    def test_linear_strategy(self) -> None:
        """``strategy="linear"`` matches ``LinearBackoff``."""
        result = calculate_backoff_delay(
            attempt=3,
            strategy="linear",
            initial_delay=5.0,
            max_delay=3600.0,
        )
        assert result == 15.0  # 5 + 5 * (3-1)

    def test_fixed_strategy(self) -> None:
        """``strategy="fixed"`` matches ``FixedBackoff``."""
        result = calculate_backoff_delay(
            attempt=10,
            strategy="fixed",
            initial_delay=3.0,
            max_delay=3600.0,
        )
        assert result == 3.0

    def test_capping(self) -> None:
        """All strategies cap at ``max_delay``."""
        result = calculate_backoff_delay(
            attempt=100,
            strategy="exponential",
            initial_delay=1.0,
            max_delay=30.0,
        )
        assert result == 30.0

    def test_invalid_strategy(self) -> None:
        """Unknown strategy raises ``ValueError``."""
        with pytest.raises(ValueError, match="Unknown backoff strategy"):
            calculate_backoff_delay(
                attempt=1,
                strategy="invalid",
                initial_delay=1.0,
                max_delay=3600.0,
            )


# ===================================================================
# BackoffStrategyType enum
# ===================================================================


class TestBackoffStrategyTypeValues:
    """Verify enum values and string representation."""

    def test_values(self) -> None:
        assert BackoffStrategyType.EXPONENTIAL.value == "exponential"
        assert BackoffStrategyType.LINEAR.value == "linear"
        assert BackoffStrategyType.FIXED.value == "fixed"

    def test_str_representation(self) -> None:
        assert str(BackoffStrategyType.EXPONENTIAL) == "exponential"
        assert str(BackoffStrategyType.LINEAR) == "linear"
        assert str(BackoffStrategyType.FIXED) == "fixed"
