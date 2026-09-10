from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


def effect_scope_violations(events: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    """Independent oracle: no SUT ledger or admission code is imported."""

    attempts = Counter(str(event["effect_scope_id"]) for event in events if int(event.get("physical_attempt_count") or 0) > 0)
    return tuple(
        {
            "invariant": "same_scope_physical_attempt_count<=1",
            "effect_scope_id": scope_id,
            "observed": count,
            "expected_maximum": 1,
        }
        for scope_id, count in sorted(attempts.items())
        if count > 1
    )
