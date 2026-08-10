"""
Unit tests for cron helpers (``conductor.core.queue._next_cron_run``).

No database required.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from conductor.core.queue import _next_cron_run


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """Build a timezone-aware UTC datetime."""
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


class TestNextCronRun:

    def test_every_minute(self) -> None:
        """``* * * * *`` fires every minute."""
        base = _utc(2026, 1, 1, 0, 0)
        assert _next_cron_run("* * * * *", base) == _utc(2026, 1, 1, 0, 1)

    def test_strictly_after_base(self) -> None:
        """A fire at exactly the base time is not returned (strictly after)."""
        base = _utc(2026, 1, 1, 0, 30)
        assert _next_cron_run("30 * * * *", base) == _utc(2026, 1, 1, 1, 30)

    def test_daily_at_0200(self) -> None:
        """``0 2 * * *`` fires daily at 02:00 UTC."""
        base = _utc(2026, 1, 1, 0, 0)
        assert _next_cron_run("0 2 * * *", base) == _utc(2026, 1, 1, 2, 0)

    def test_skip_missed_occurrences(self) -> None:
        """If the base time is past today's fire, the next fire is tomorrow."""
        base = _utc(2026, 1, 1, 3, 0)
        assert _next_cron_run("0 2 * * *", base) == _utc(2026, 1, 2, 2, 0)

    def test_advances_across_midnight(self) -> None:
        """``0 0 * * *`` after 23:59 advances to the next day."""
        base = _utc(2026, 1, 1, 23, 59)
        assert _next_cron_run("0 0 * * *", base) == _utc(2026, 1, 2, 0, 0)

    def test_returns_utc_aware(self) -> None:
        """The computed next run is timezone-aware (UTC)."""
        nxt = _next_cron_run("* * * * *", _utc(2026, 1, 1))
        assert nxt.tzinfo is not None
        assert nxt.utcoffset() == timezone.utc.utcoffset(nxt)

    def test_invalid_expression_raises(self) -> None:
        """An unparseable expression raises ValueError."""
        with pytest.raises(ValueError, match="cron"):
            _next_cron_run("not a cron", _utc(2026, 1, 1))
