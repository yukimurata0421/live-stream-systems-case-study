from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def independent_p2_expected(case: Mapping[str, Any]) -> str:
    """Independent fail-closed oracle; imports no production evaluator."""

    kind = str(case["kind"])
    if kind == "ACK_SET":
        if not bool(case.get("all_required_present")) or not bool(case.get("in_flight_confirmed")):
            return "BLOCK"
        if int(case.get("in_flight_count") or 0) != 0:
            return "REJECT"
        return "ACK"
    if not bool(case.get("snapshot_available")) or not bool(case.get("snapshot_fresh")):
        return "BLOCK"
    if bool(case.get("maintenance_active")):
        if not bool(case.get("target_match")) or not bool(case.get("generation_match")):
            return "REJECT"
        if not bool(case.get("planned")):
            return "BLOCK"
        if not bool(case.get("authorization_present")):
            return "REJECT"
        if bool(case.get("authorization_replayed")) or bool(case.get("authorization_expired")):
            return "REJECT"
        return "ALLOW"
    return "ALLOW" if not bool(case.get("planned")) else "REJECT"


def p2_violations(case: Mapping[str, Any]) -> list[str]:
    expected = independent_p2_expected(case)
    actual = str(case.get("actual") or "")
    return [] if actual == expected else [f"{case['control_id']}:{actual}!={expected}"]
