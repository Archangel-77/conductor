"""
Dead Letter Queue package.

Handles tasks that have exhausted their retry attempts.
"""

from conductor.dlq.dead_letter_queue import DeadLetterQueue

__all__: list[str] = [
    "DeadLetterQueue",
]
