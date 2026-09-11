#!/usr/bin/env python3
"""Observation-boundary historical replay harness for one audited window."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import unix_ts
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES
from stream_monitoring_v4.incidents.service import IncidentService
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.repository import MonitoringRepository


FIXTURE_SCHEMA = "monitoring_v4.historical_replay_window.v1"
_LEAK = re.compile(r"(?:https?://|\b(?:10|127|192\.168|172\.(?:1[6-9]|2\d|3[01]))(?:\.\d{1,3}){2}\b|/home/|/var/|token|secret|password|credential|notification_(?:subject|content))", re.IGNORECASE)


class ReplayHarnessError(RuntimeError):
    pass


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def fixture_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_fixture(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    required = {"schema", "fixture_id", "replay_boundary", "provenance", "observations", "historical_expected", "synthetic_actions", "unknown"}
    if not isinstance(value, dict) or set(value) != required:
        raise ReplayHarnessError("fixture shape does not match historical replay v1")
    if value["schema"] != FIXTURE_SCHEMA or value["replay_boundary"] != "persisted_observation":
        raise ReplayHarnessError("fixture is not an observation-boundary replay fixture")
    if not isinstance(value["observations"], list) or len(value["observations"]) != 6:
        raise ReplayHarnessError("fixture must contain exactly six audited observations")
    return value


def assert_sanitized_fixture(value: dict[str, Any]) -> None:
    raw = canonical_json(value)
    if _LEAK.search(raw):
        raise ReplayHarnessError("fixture contains prohibited sensitive value")
    for observation in value["observations"]:
        if observation["provenance"] != "historical_observed":
            raise ReplayHarnessError("observation provenance is not historical_observed")
        for forbidden in ("observation_id", "snapshot_id", "episode_id", "transition_id", "intent_id", "source_event_id", "source_generation", "subject", "content"):
            if forbidden in observation:
                raise ReplayHarnessError(f"raw identifier leaked: {forbidden}")
        if not observation["alias"].startswith("obs-"):
            raise ReplayHarnessError("observation alias is not deterministic scoped alias")


def _event_from_fixture(item: dict[str, Any]) -> ObservationEnvelope:
    return ObservationEnvelope.create(
        domain=item["domain"], source=item["source"],
        source_event_id=item["source_event_alias"], source_generation=item["source_generation_alias"],
        evidence_role=item["evidence_role"], status=item["state"], reason_code=item["reason_code"],
        observed_at=item["observed_at"], received_at=item["received_at"],
        freshness_limit_sec=int(item["freshness_limit_sec"]), producer_revision=item["producer_revision"],
        payload=item["payload"],
    )


def _public_current(item: Any) -> dict[str, Any]:
    return {"domain": item.domain, "state": item.state, "observed_at": item.observed_at, "reduced_at": item.reduced_at, "reason_codes": list(item.reason_codes)}


def _episodes(repository: MonitoringRepository) -> list[dict[str, Any]]:
    with repository.connection(read_only=True) as connection:
        rows = connection.execute("SELECT domain, status, severity, opened_at, closed_at FROM incident_episodes ORDER BY domain, opened_ts").fetchall()
    return [dict(row) for row in rows]


def _public_transitions(repository: MonitoringRepository) -> list[dict[str, Any]]:
    return [{"domain": item.domain, "phase": item.phase, "severity": item.severity, "occurred_at": item.occurred_at} for item in repository.transitions()]


def _public_intents(repository: MonitoringRepository) -> list[dict[str, Any]]:
    return [{"route": item.route, "phase": item.phase, "severity": item.severity, "created_at": item.created_at} for item in repository.intents()]


def _reduce_and_process(repository: MonitoringRepository, at: str) -> list[dict[str, Any]]:
    current = CurrentReducerService(repository, DEFAULT_POLICIES).reduce(("delivery", "rendering"), now_ts=unix_ts(at))
    service = IncidentService(repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy())
    for item in current:
        service.process(item, now_at=at)
    return [_public_current(item) for item in current]


def _semantic_difference(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    expected_transitions = sorted(expected["transitions"], key=canonical_json)
    actual_transitions = sorted(actual["transitions"], key=canonical_json)
    matched = (
        expected["episodes"] == len(actual["episodes"])
        and expected["logical_intents"] == len(actual["intents"])
        and expected_transitions == actual_transitions
        and all(item["state"] == "good" for item in actual["canonical_current"])
    )
    return {
        "classification": "exact_match" if matched else "unexpected_semantic_divergence",
        "expected": {"episodes": expected["episodes"], "transitions": expected_transitions, "logical_intents": expected["logical_intents"]},
        "actual": {"episodes": len(actual["episodes"]), "transitions": actual_transitions, "logical_intents": len(actual["intents"])},
    }


def _source_identity() -> dict[str, str]:
    def call(args: list[str]) -> str:
        completed = subprocess.run(args, cwd=Path(__file__).resolve().parents[1], text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False)
        return completed.stdout.strip() if completed.returncode == 0 else "unavailable"
    return {"git_head": call(["git", "rev-parse", "HEAD"]), "tracked_dirty_diff_sha256": hashlib.sha256(call(["git", "diff", "--binary", "--"]).encode()).hexdigest()}


def _run_sequence(
    fixture: dict[str, Any],
    *,
    dropped_aliases: set[str] | None = None,
) -> tuple[MonitoringRepository, Path, list[dict[str, Any]], dict[str, Any]]:
    """Replay only persisted observations; dropping is a caller-labelled synthetic action."""
    assert_sanitized_fixture(fixture)
    root = Path(tempfile.mkdtemp(prefix="stream-v4-historical-replay-"))
    database = root / "replay.sqlite3"
    timeline: list[dict[str, Any]] = []
    repository = MonitoringRepository(database)
    repository.initialize(applied_at=fixture["provenance"]["historical_start"])
    dropped = dropped_aliases or set()
    observations = list(fixture["observations"])
    checkpoints = ("2026-08-12T16:49:20Z", "2026-08-12T16:50:20Z", "2026-08-12T16:51:20Z")
    cursor = 0
    for checkpoint in checkpoints:
        while cursor < len(observations) and observations[cursor]["observed_at"] <= checkpoint:
            item = observations[cursor]
            if item["alias"] in dropped:
                timeline.append({"provenance": "synthetic", "action": "drop_observation", "alias": item["alias"]})
            else:
                accepted = repository.append_observation(_event_from_fixture(item))
                timeline.append({"provenance": "historical_observed", "action": "append_observation", "alias": item["alias"], "accepted": accepted})
            cursor += 1
        timeline.append({"provenance": "historical_derived", "action": "reduce_and_process", "at": checkpoint, "current": _reduce_and_process(repository, checkpoint)})
    result = {
        "canonical_current": [_public_current(repository.current(domain)) for domain in ("delivery", "rendering")],
        "episodes": _episodes(repository), "transitions": _public_transitions(repository), "intents": _public_intents(repository),
    }
    return repository, root, timeline, result


def _delayed_old_bad(item: dict[str, Any]) -> tuple[ObservationEnvelope, dict[str, Any]]:
    delayed_received_at = "2026-08-12T16:51:21Z"
    synthetic = {
        **item,
        "source_event_alias": f"synthetic-delayed-{item['source_event_alias']}",
        "source_generation_alias": f"synthetic-delayed-{item['source_generation_alias']}",
        "received_at": delayed_received_at,
    }
    event = _event_from_fixture(synthetic)
    action = {
        "provenance": "synthetic",
        "action": "append_new_identity_delayed_old_bad",
        "id": "synthetic-delayed-old-bad-1",
        "historical_semantics_alias": item["alias"],
        "observed_at": item["observed_at"],
        "received_at": delayed_received_at,
        "new_identity": True,
    }
    return event, action


def replay_fixture(fixture: dict[str, Any], *, mode: str) -> dict[str, Any]:
    if mode not in {"baseline", "duplicate_old_bad", "delayed_old_bad", "recovery_drop"}:
        raise ReplayHarnessError(f"unsupported replay mode: {mode}")
    repository, root, timeline, baseline = _run_sequence(fixture)
    try:
        comparison = _semantic_difference(fixture["historical_expected"], baseline)
        result: dict[str, Any] = {"timeline": timeline, "baseline": baseline, "semantic_difference": comparison, "perturbation": None}
        if comparison["classification"] != "exact_match" or mode == "baseline":
            return result
        if mode == "recovery_drop":
            dropped = next(item for item in fixture["observations"] if item["domain"] == "delivery" and item["state"] == "good" and item["observed_at"] == "2026-08-12T16:50:58Z")
            drop_repository, drop_root, drop_timeline, after = _run_sequence(fixture, dropped_aliases={dropped["alias"]})
            try:
                delivery_episode = next(item for item in after["episodes"] if item["domain"] == "delivery")
                delivery_recovery = [item for item in after["transitions"] if item["domain"] == "delivery" and item["phase"] == "recovered"]
                delivery_recovery_intents = [item for item in after["intents"] if item["phase"] == "recovered" and item["route"] in {"discord", "slack"}]
                invariants = {
                    "recovery_transition_not_generated": not delivery_recovery,
                    "delivery_episode_not_closed": delivery_episode["status"] == "active",
                    "delivery_not_promoted_to_good": next(item for item in after["canonical_current"] if item["domain"] == "delivery")["state"] != "good",
                    "recovery_intent_count_reduced": len(delivery_recovery_intents) == 1,
                }
                result["timeline"] = timeline + [{"provenance": "synthetic", "action": "begin_recovery_drop_sequence", "alias": dropped["alias"]}] + drop_timeline
                result["perturbation"] = {
                    "classification": "pass" if all(invariants.values()) else "sut_monitoring_failure",
                    "suppression": "recovery_evidence_absent_fail_closed",
                    "dropped_observation": dropped["alias"], "result": after, "invariants": invariants,
                }
                return result
            finally:
                shutil.rmtree(drop_root, ignore_errors=True)
        old_bad = next(item for item in fixture["observations"] if item["domain"] == "delivery" and item["state"] == "bad")
        timeline.append({"provenance": "synthetic", "action": "component_restart", "id": "synthetic-component-restart-1"})
        repository = MonitoringRepository(root / "replay.sqlite3")
        timeline.append({"provenance": "synthetic", "action": "repository_reopen", "id": "synthetic-repository-reopen-1"})
        if mode == "duplicate_old_bad":
            accepted = repository.append_observation(_event_from_fixture(old_bad))
            action = {"provenance": "synthetic", "action": "replay_old_bad", "alias": old_bad["alias"], "accepted": accepted}
            suppression = "duplicate_observation_id" if not accepted else "unclassified"
            authority_blocked = not accepted
        else:
            delayed, action = _delayed_old_bad(old_bad)
            accepted = repository.append_observation(delayed)
            suppression = "older_observed_at_canonical_non_regression"
            authority_blocked = accepted
        timeline.append(action)
        after_current = _reduce_and_process(repository, "2026-08-12T16:51:21Z")
        after = {"canonical_current": [_public_current(repository.current(domain)) for domain in ("delivery", "rendering")], "episodes": _episodes(repository), "transitions": _public_transitions(repository), "intents": _public_intents(repository)}
        delivery_current = next(item for item in after["canonical_current"] if item["domain"] == "delivery")
        invariants = {
            "canonical_current_did_not_regress": all(item["state"] == "good" for item in after["canonical_current"]),
            "observation_accepted_but_not_authoritative": authority_blocked and delivery_current["observed_at"] > old_bad["observed_at"],
            "episodes_not_reopened": after["episodes"] == baseline["episodes"],
            "transitions_not_added": after["transitions"] == baseline["transitions"],
            "intents_not_added": after["intents"] == baseline["intents"],
        }
        result["perturbation"] = {"classification": "pass" if all(invariants.values()) else "sut_monitoring_failure", "suppression": suppression, "append_accepted": accepted, "after_reduce_current": after_current, "result": after, "invariants": invariants}
        return result
    finally:
        shutil.rmtree(root, ignore_errors=True)


def build_artifact(fixture_path: Path, *, mode: str) -> dict[str, Any]:
    fixture = load_fixture(fixture_path)
    result = replay_fixture(fixture, mode=mode)
    return {
        "artifact_schema": "monitoring_v4.historical_replay_artifact.v1",
        "source_identity": _source_identity(),
        "historical_window_id": fixture["provenance"]["historical_window_id"],
        "dump_sha256": fixture["provenance"]["dump_sha256"],
        "fixture_sha256": fixture_sha256(fixture_path),
        "replay_boundary": fixture["replay_boundary"],
        "mode": mode,
        "historical_facts": fixture["historical_expected"],
        "synthetic_actions": [item for item in result["timeline"] if item["provenance"] == "synthetic"],
        "result": result,
        "classification": result["semantic_difference"]["classification"] if result["semantic_difference"]["classification"] != "exact_match" else (result["perturbation"] or {"classification": "baseline_pass"})["classification"],
        "reproduction_command": f"PYTHONPATH=src python3 tests/historical_replay_harness.py --fixture {fixture_path} --mode {mode} --artifact OUTPUT_ARTIFACT.json",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--mode", choices=("baseline", "duplicate_old_bad", "delayed_old_bad", "recovery_drop"), required=True)
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    artifact = build_artifact(args.fixture, mode=args.mode)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(artifact, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return 0 if artifact["classification"] in {"baseline_pass", "pass"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
