"""Handlers used by the Docker Compose CI smoke test.

Mounted into the compose worker via ``tests/docker/compose.ci.yml`` and
loaded through ``CONDUCTOR_HANDLERS_MODULE`` so the "tasks execute" check
can run end-to-end through the real worker container.
"""

from __future__ import annotations

from typing import Any, Optional

from conductor.core.worker import Worker


def register(worker: Worker) -> None:
    """Register the compose smoke-test handler on *worker*."""

    @worker.task("qa_echo")
    async def qa_echo(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        return {"echo": payload}
