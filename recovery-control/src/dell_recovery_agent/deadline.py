from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from cra_dell_recovery.errors import AuthorityDeadlineExceeded


@dataclass(frozen=True)
class AuthorityOperationDeadline:
    """Admission-to-commit budget for authority-sensitive DB operations.

    This is deliberately independent from SQLite's five-second writer-lock
    bound.  The shorter deadline prevents a transaction that waited behind a
    writer from restoring an authority decision after the suspect boundary.
    """

    admitted_at: float
    deadline_seconds: float
    monotonic: Callable[[], float] = time.monotonic

    @classmethod
    def start(
        cls,
        *,
        deadline_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> AuthorityOperationDeadline:
        if deadline_seconds <= 0:
            raise ValueError("critical DB deadline must be positive")
        return cls(monotonic(), deadline_seconds, monotonic)

    @property
    def elapsed_seconds(self) -> float:
        return max(0.0, self.monotonic() - self.admitted_at)

    def ensure_valid(self) -> None:
        if self.elapsed_seconds >= self.deadline_seconds:
            raise AuthorityDeadlineExceeded(
                f"DB_DEADLINE_AUTHORITY_COUPLING_EXCEEDED:{self.elapsed_seconds:.6f}>={self.deadline_seconds:.6f}"
            )
