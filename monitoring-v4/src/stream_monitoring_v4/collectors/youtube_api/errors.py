from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RemoteFailure(RuntimeError):
    kind: str
    http_status: int = 0
    error_reason: str = ""

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.kind)


class PaginationLimit(RuntimeError):
    pass
