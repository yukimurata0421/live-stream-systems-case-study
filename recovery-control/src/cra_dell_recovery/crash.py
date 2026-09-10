from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum


class CrashPoint(StrEnum):
    CENTRAL_BEFORE_COMMIT = "CENTRAL_BEFORE_COMMIT"
    CENTRAL_AFTER_COMMIT_BEFORE_SEND = "CENTRAL_AFTER_COMMIT_BEFORE_SEND"
    CENTRAL_AFTER_SEND_BEFORE_RECEIPT = "CENTRAL_AFTER_SEND_BEFORE_RECEIPT"
    DELL_AFTER_ACCEPT_COMMIT = "DELL_AFTER_ACCEPT_COMMIT"
    DELL_AFTER_EXECUTION_STARTED_COMMIT = "DELL_AFTER_EXECUTION_STARTED_COMMIT"
    DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS = "DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS"
    CRA_AFTER_RESTORE = "CRA_AFTER_RESTORE"


class InjectedCrash(RuntimeError):
    pass


CrashInjector = Callable[[CrashPoint], None]


def no_crash(_: CrashPoint) -> None:
    return


def crash_at(expected: CrashPoint) -> CrashInjector:
    def inject(actual: CrashPoint) -> None:
        if actual == expected:
            raise InjectedCrash(actual)

    return inject
