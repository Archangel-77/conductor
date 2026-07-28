"""
Custom exception classes for Conductor.

All exceptions inherit from a base ``ConductorException`` so callers can
catch a single type when needed.
"""


class ConductorException(Exception):
    """Base exception for all Conductor errors."""


class DatabaseError(ConductorException):
    """Raised when a database operation fails."""


class WorkerError(ConductorException):
    """Raised when a worker encounters an unrecoverable error."""


class TaskError(ConductorException):
    """Raised when a task operation (submit, fetch, update) fails."""


class RetryPolicyError(ConductorException):
    """Raised when a retry policy is invalid or cannot be applied."""


class ConductorConnectionError(ConductorException):
    """Raised when a database connection cannot be established."""
