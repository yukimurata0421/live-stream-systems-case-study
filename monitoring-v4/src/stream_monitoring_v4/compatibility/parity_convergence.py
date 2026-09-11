from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from stream_contracts.monitoring_v4.time import unix_ts


PARITY_CONVERGENCE_POLICY_REVISION = "monitoring-v4-parity-convergence-r2"
_CURRENT_STATES = frozenset({"good", "bad", "unknown"})


@dataclass(frozen=True)
class SnapshotConvergencePolicy:
    """Narrow allowlist for asynchronously sampled views of one producer path."""

    domain: str
    expected_source: str
    actual_sources: tuple[str, ...]
    maximum_absolute_skew_sec: int
    convergence_timeout_sec: int
    evidence_basis: str

    def __post_init__(self) -> None:
        if not self.domain or not self.expected_source or not self.actual_sources:
            raise ValueError("snapshot convergence identity is required")
        if self.maximum_absolute_skew_sec <= 0 or self.convergence_timeout_sec <= 0:
            raise ValueError("snapshot convergence bounds must be positive")
        if tuple(sorted(set(self.actual_sources))) != self.actual_sources:
            raise ValueError("actual_sources must be sorted and unique")
        if not self.evidence_basis.strip():
            raise ValueError("snapshot convergence evidence basis is required")


# This is deliberately not inferred from every source TTL. A pair is added only
# after its producer relationship and a real bounded convergence sequence have
# been established. Different producer paths remain contract differences.
SNAPSHOT_CONVERGENCE_POLICIES: dict[tuple[str, str], SnapshotConvergencePolicy] = {
    (
        "delivery",
        "subsystems_status.local_delivery",
    ): SnapshotConvergencePolicy(
        domain="delivery",
        expected_source="subsystems_status.local_delivery",
        actual_sources=("runtime_delivery_watchdog",),
        maximum_absolute_skew_sec=180,
        convergence_timeout_sec=360,
        evidence_basis=(
            "the legacy aggregate and v4 current read the local-delivery watchdog; "
            "the 2026-08-17 transition fixture observed 42s maximum skew and 310s "
            "maximum lagging-view catch-up during repeated link transitions"
        ),
    ),
    (
        "youtube_input_quality",
        "operational_reliability_burn_status.raw_current",
    ): SnapshotConvergencePolicy(
        domain="youtube_input_quality",
        expected_source="operational_reliability_burn_status.raw_current",
        actual_sources=("youtube_input_quality_oauth",),
        maximum_absolute_skew_sec=600,
        convergence_timeout_sec=600,
        evidence_basis=(
            "both views originate from the OAuth input-quality producer; its declared "
            "freshness boundary is 600s, while the 2026-08-17 fixture observed 352s "
            "maximum skew and 457s maximum lagging-view catch-up"
        ),
    ),
}

_ROLLOUT_SOURCE_PAIRS: dict[tuple[str, str], tuple[str, ...]] = {
    ("delivery", "subsystems_status.local_delivery"): (
        "runtime_delivery_watchdog",
    ),
    ("rendering", "map_runtime_status"): ("map_runtime",),
}


@dataclass(frozen=True)
class SnapshotConvergenceCandidate:
    policy: SnapshotConvergencePolicy
    expected_ts: int
    actual_ts: int
    skew_sec: int

    @property
    def lagging_side(self) -> str:
        return "actual" if self.skew_sec > 0 else "expected"

    @property
    def ahead_ts(self) -> int:
        return max(self.expected_ts, self.actual_ts)


def selected_actual_sources(payload: Mapping[str, Any]) -> tuple[tuple[str, ...], str]:
    """Extract reducer-selected source identity without accepting malformed payloads."""

    selected = payload.get("selected")
    if not isinstance(selected, Sequence) or isinstance(selected, (str, bytes)):
        return (), "selected_sources_missing"
    sources: list[str] = []
    for raw in selected:
        if not isinstance(raw, Mapping):
            return (), "selected_source_not_object"
        source = raw.get("source")
        if not isinstance(source, str) or not source.strip():
            return (), "selected_source_invalid"
        sources.append(source.strip())
    if not sources:
        return (), "selected_sources_empty"
    if len(sources) != len(set(sources)):
        return (), "selected_sources_duplicate"
    return tuple(sorted(sources)), ""


def decoded_actual_sources(value: Any) -> tuple[str, ...] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return None
    sources: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            return None
        sources.append(raw.strip())
    if not sources or len(sources) != len(set(sources)):
        return None
    normalized = tuple(sorted(sources))
    return normalized if list(normalized) == sources else None


def snapshot_convergence_candidate(
    domain: str,
    detail: Mapping[str, Any],
) -> SnapshotConvergenceCandidate | None:
    """Validate a candidate from its complete, versioned parity detail."""

    if detail.get("match") is not False:
        return None
    expected_state = detail.get("expected_state")
    actual_state = detail.get("actual_state")
    if (
        expected_state not in _CURRENT_STATES
        or actual_state not in _CURRENT_STATES
        or expected_state == actual_state
    ):
        return None
    if detail.get("normalization_error") not in {"", None}:
        return None
    expected_source = detail.get("expected_source")
    if not isinstance(expected_source, str):
        return None
    policy = SNAPSHOT_CONVERGENCE_POLICIES.get((domain, expected_source))
    if policy is None:
        return None
    actual_sources = decoded_actual_sources(detail.get("actual_sources"))
    if actual_sources != policy.actual_sources:
        return None
    try:
        expected_ts = unix_ts(str(detail.get("expected_observed_at", "")))
        actual_ts = unix_ts(str(detail.get("actual_observed_at", "")))
    except ValueError:
        return None
    skew_sec = expected_ts - actual_ts
    if skew_sec == 0 or abs(skew_sec) > policy.maximum_absolute_skew_sec:
        return None
    declared_skew = detail.get("snapshot_skew_sec")
    if type(declared_skew) is not int or declared_skew != skew_sec:
        return None
    return SnapshotConvergenceCandidate(policy, expected_ts, actual_ts, skew_sec)


def rollout_convergence_candidate_detail_valid(
    domain: str,
    detail: Mapping[str, Any],
) -> bool:
    """Keep rollout reconciliation inside an exact legacy/v4 source pair."""

    if detail.get("match") is not False:
        return False
    expected_state = detail.get("expected_state")
    actual_state = detail.get("actual_state")
    if (
        expected_state not in _CURRENT_STATES
        or actual_state not in _CURRENT_STATES
        or expected_state == actual_state
    ):
        return False
    if detail.get("normalization_error") not in {"", None}:
        return False
    expected_source = detail.get("expected_source")
    if not isinstance(expected_source, str):
        return False
    required_sources = _ROLLOUT_SOURCE_PAIRS.get((domain, expected_source))
    if required_sources is None:
        return False
    if decoded_actual_sources(detail.get("actual_sources")) != required_sources:
        return False
    try:
        unix_ts(str(detail.get("expected_observed_at", "")))
        unix_ts(str(detail.get("actual_observed_at", "")))
    except ValueError:
        return False
    return True


def snapshot_candidate_converged(
    candidate: SnapshotConvergenceCandidate,
    equivalent_detail: Mapping[str, Any],
) -> bool:
    """Require the lagging view to pass the original ahead timestamp and agree."""

    if equivalent_detail.get("match") is not True:
        return False
    if equivalent_detail.get("classification") != "equivalent":
        return False
    expected_state = equivalent_detail.get("expected_state")
    if expected_state not in _CURRENT_STATES or equivalent_detail.get("actual_state") != expected_state:
        return False
    if equivalent_detail.get("normalization_error") not in {"", None}:
        return False
    if equivalent_detail.get("expected_source") != candidate.policy.expected_source:
        return False
    if decoded_actual_sources(equivalent_detail.get("actual_sources")) != candidate.policy.actual_sources:
        return False
    try:
        expected_ts = unix_ts(str(equivalent_detail.get("expected_observed_at", "")))
        actual_ts = unix_ts(str(equivalent_detail.get("actual_observed_at", "")))
    except ValueError:
        return False
    caught_up_ts = actual_ts if candidate.lagging_side == "actual" else expected_ts
    return (
        candidate.ahead_ts < caught_up_ts
        <= candidate.ahead_ts + candidate.policy.convergence_timeout_sec
    )
