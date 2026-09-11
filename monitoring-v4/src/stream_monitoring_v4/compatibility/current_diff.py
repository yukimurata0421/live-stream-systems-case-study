from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from stream_contracts.monitoring_v4.current import DomainCurrent


@dataclass(frozen=True)
class CurrentDifference:
    domain: str
    field: str
    expected: Any
    actual: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "field": self.field,
            "expected": self.expected,
            "actual": self.actual,
        }


@dataclass(frozen=True)
class CurrentDiffReport:
    compared_domains: tuple[str, ...]
    missing_expected_domains: tuple[str, ...]
    missing_actual_domains: tuple[str, ...]
    differences: tuple[CurrentDifference, ...]

    @property
    def equivalent(self) -> bool:
        return not self.missing_expected_domains and not self.missing_actual_domains and not self.differences

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "monitoring_v4.current_diff.v1",
            "equivalent": self.equivalent,
            "compared_domains": list(self.compared_domains),
            "missing_expected_domains": list(self.missing_expected_domains),
            "missing_actual_domains": list(self.missing_actual_domains),
            "differences": [item.to_dict() for item in self.differences],
        }


def compare_current_projection(
    actual: Iterable[DomainCurrent],
    expected: Mapping[str, Mapping[str, Any]],
    *,
    fields: tuple[str, ...] = ("state", "reason_codes", "observed_at"),
) -> CurrentDiffReport:
    """Compare v4 current with an explicitly normalized, fixed-time v3 projection.

    The caller must normalize v3 values first. This function never reads or
    rewrites v3 state and does not silently translate missing fields.
    """

    actual_by_domain = {item.domain: item.to_dict() for item in actual}
    expected_domains = set(expected)
    actual_domains = set(actual_by_domain)
    compared = sorted(expected_domains & actual_domains)
    differences: list[CurrentDifference] = []
    for domain in compared:
        expected_item = expected[domain]
        actual_item = actual_by_domain[domain]
        for field in fields:
            if field not in expected_item:
                differences.append(CurrentDifference(domain, field, "<missing>", actual_item.get(field)))
                continue
            expected_value = expected_item[field]
            actual_value = actual_item.get(field)
            if field in {"reason_codes", "source_observation_ids"}:
                expected_value = sorted(str(item) for item in expected_value)
                actual_value = sorted(str(item) for item in actual_value or [])
            if expected_value != actual_value:
                differences.append(CurrentDifference(domain, field, expected_value, actual_value))
    return CurrentDiffReport(
        compared_domains=tuple(compared),
        missing_expected_domains=tuple(sorted(actual_domains - expected_domains)),
        missing_actual_domains=tuple(sorted(expected_domains - actual_domains)),
        differences=tuple(differences),
    )
