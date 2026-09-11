from __future__ import annotations

from collections.abc import Sequence

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import unix_ts, utc_text

from .source_policy import DomainPolicy


REDUCER_REVISION = "monitoring-v4-current-reducer-r3.3"


def _latest_by_source(items: Sequence[ObservationEnvelope]) -> dict[str, ObservationEnvelope]:
    latest: dict[str, ObservationEnvelope] = {}
    for item in items:
        previous = latest.get(item.source)
        if previous is None or (unix_ts(item.observed_at), unix_ts(item.received_at), item.observation_id) > (
            unix_ts(previous.observed_at),
            unix_ts(previous.received_at),
            previous.observation_id,
        ):
            latest[item.source] = item
    return latest


def reduce_domain(
    observations: Sequence[ObservationEnvelope],
    policy: DomainPolicy,
    *,
    now_ts: int,
    max_future_sec: int = 0,
) -> DomainCurrent:
    latest = _latest_by_source([item for item in observations if item.domain == policy.domain])
    eligible: list[tuple[int, int, ObservationEnvelope]] = []
    ignored: list[dict[str, str]] = []
    diagnostics: list[dict[str, str]] = []
    for item in latest.values():
        rule = policy.rule(item.source)
        if rule is None:
            ignored.append({"source": item.source, "reason": "source_not_in_policy"})
            continue
        if item.evidence_role not in rule.roles:
            ignored.append({"source": item.source, "reason": "role_not_current"})
            continue
        observed_ts = unix_ts(item.observed_at)
        if observed_ts > now_ts + max(0, int(max_future_sec)):
            ignored.append({"source": item.source, "reason": "future_timestamp"})
            continue
        freshness_limit = min(rule.ttl_sec, item.freshness_limit_sec)
        if now_ts - observed_ts > freshness_limit:
            ignored.append({"source": item.source, "reason": "stale"})
            continue
        if not rule.current_authority:
            diagnostics.append(
                {
                    "source": item.source,
                    "producer_id": rule.producer_key,
                    "status": item.status,
                    "reason_code": item.reason_code,
                    "evidence_role": item.evidence_role,
                    "observed_at": item.observed_at,
                    "observation_id": item.observation_id,
                    "diagnostic_kind": rule.diagnostic_kind,
                }
            )
            ignored.append(
                {
                    "source": item.source,
                    "reason": (
                        "projection_not_current_authority"
                        if rule.diagnostic_kind == "same_producer_projection"
                        else "correlated_not_current_authority"
                    ),
                }
            )
            continue
        if item.evidence_role not in policy.allowed_roles:
            ignored.append({"source": item.source, "reason": "role_not_current"})
            continue
        eligible.append((rule.priority, freshness_limit, item))

    diagnostics.sort(key=lambda item: (item["source"], item["observation_id"]))

    if not eligible:
        reduced_at = utc_text(now_ts)
        return DomainCurrent.create(
            domain=policy.domain,
            state="unknown",
            reason_codes=("missing_current_evidence",),
            source_observation_ids=(),
            observed_at=reduced_at,
            reduced_at=reduced_at,
            valid_until=reduced_at,
            policy_revision=policy.revision,
            reducer_revision=REDUCER_REVISION,
            payload={
                "selected": [],
                "diagnostics": diagnostics,
                "same_producer_projection_disagreement": False,
                "same_producer_api_disagreement": False,
                "ignored": sorted(ignored, key=lambda item: item["source"]),
            },
        )

    top_priority = max(priority for priority, _ttl, _item in eligible)
    selected = [(ttl, item) for priority, ttl, item in eligible if priority == top_priority]
    statuses = {item.status for _ttl, item in selected}
    if "bad" in statuses and "good" in statuses:
        state = "unknown"
        reasons = ("source_disagreement",)
    elif "bad" in statuses:
        state = "bad"
        reasons = ("current_bad_evidence",)
    elif "good" in statuses:
        state = "good"
        reasons = ("current_good_evidence",)
    elif statuses == {"not_applicable"}:
        state = "unknown"
        reasons = ("current_evidence_not_applicable",)
    else:
        state = "unknown"
        reasons = ("current_evidence_unknown",)

    observed_ts = max(unix_ts(item.observed_at) for _ttl, item in selected)
    valid_until_ts = min(unix_ts(item.observed_at) + ttl for ttl, item in selected)
    selected_sorted = sorted(selected, key=lambda pair: (pair[1].source, pair[1].observation_id))
    selected_status_by_producer = {
        policy.rule(item.source).producer_key: item.status
        for _ttl, item in selected_sorted
        if policy.rule(item.source) is not None
    }
    same_producer_projection_disagreement = any(
        item["producer_id"] in selected_status_by_producer
        and item["status"] != selected_status_by_producer[item["producer_id"]]
        and item["diagnostic_kind"] == "same_producer_projection"
        for item in diagnostics
    )
    same_producer_api_disagreement = any(
        item["producer_id"] in selected_status_by_producer
        and item["status"] != selected_status_by_producer[item["producer_id"]]
        and item["diagnostic_kind"] == "same_producer_api_probe"
        for item in diagnostics
    )
    context: dict[str, bool] = {}
    if any(item.payload.get("maintenance") is True for _ttl, item in selected_sorted):
        context["maintenance"] = True
    if any(item.payload.get("planned_rollout") is True for _ttl, item in selected_sorted):
        context["planned_rollout"] = True
    return DomainCurrent.create(
        domain=policy.domain,
        state=state,
        reason_codes=reasons,
        source_observation_ids=[item.observation_id for _ttl, item in selected_sorted],
        observed_at=utc_text(observed_ts),
        reduced_at=utc_text(now_ts),
        valid_until=utc_text(valid_until_ts),
        policy_revision=policy.revision,
        reducer_revision=REDUCER_REVISION,
        payload={
            **context,
            "priority": top_priority,
            "selected": [
                {
                    "source": item.source,
                    "status": item.status,
                    "evidence_role": item.evidence_role,
                    "observed_at": item.observed_at,
                    "payload": dict(item.payload),
                }
                for _ttl, item in selected_sorted
            ],
            "diagnostics": diagnostics,
            "same_producer_projection_disagreement": same_producer_projection_disagreement,
            "same_producer_api_disagreement": same_producer_api_disagreement,
            "ignored": sorted(ignored, key=lambda item: item["source"]),
        },
    )
