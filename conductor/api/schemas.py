"""Pydantic response models for the Conductor web dashboard API.

The dashboard endpoints return plain dictionaries; these models document
the stable response shapes consumed by the React frontend in
``conductor/web``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class TaskListResponse(BaseModel):
    """Paginated task listing response."""

    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


class WorkerListResponse(BaseModel):
    """Paginated worker listing response."""

    items: list[dict[str, Any]]
    limit: int
    offset: int


class DlqListResponse(BaseModel):
    """Paginated dead-letter queue listing response."""

    items: list[dict[str, Any]]
    limit: int
    offset: int


class CancelResponse(BaseModel):
    """Response for cancelling a task."""

    status: str


class RetryResponse(BaseModel):
    """Response for retrying a dead-lettered task."""

    task_id: str


class DiscardResponse(BaseModel):
    """Response for discarding a dead-lettered task."""

    status: str
    task_id: str
