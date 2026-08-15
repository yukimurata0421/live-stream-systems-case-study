#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stream_core.cli_support.sli_report import POLICIES, SliReportContext, build_sli_report
from stream_core.common.json_io import iter_jsonl
from stream_core.operational_reliability.evidence_store import (
    RETENTION_DAYS,
    backfill_same_url_events,
    compact_duplicate_daily_checkpoints,
    counts,
    prune,
    upsert_rollups,
)


JST = timezone(timedelta(hours=9))
BURN_WINDOWS = "15m,1h,6h,24h,7d,30d"


def git_revision(root: Path) -> dict:
    def run(*args: str) -> str:
        completed = subprocess.run(
            ["git", *args], cwd=root, text=True, capture_output=True, check=False, timeout=10
        )
        return completed.stdout.strip() if completed.returncode == 0 else ""

    head = run("rev-parse", "HEAD")
    tag = run("describe", "--tags", "--exact-match", "HEAD")
    dirty = bool(run("status", "--porcelain", "--untracked-files=no"))
    expected = os.environ.get("STREAM_V3_DEPLOYED_REVISION", "").strip()
    return {
        "head": head,
        "exact_tag": tag,
        "worktree_clean": not dirty,
        "expected_revision": expected,
        "matches_expected_revision": bool(expected and expected in {head, tag}),
        "deployment_revision_valid": bool(head and tag and not dirty and (not expected or expected in {head, tag})),
    }


def measurement(window: dict, family: str) -> dict:
    item = window.get(family, {})
    if family == "youtube_availability":
        return item.get("selected", {})
    if family == "same_url_preservation":
        return item.get("raw_metric", {})
    return item if isinstance(item, dict) else {}


def burn_rate(item: dict, target_pct: float) -> float | None:
    if item.get("measurement_status") != "valid" or item.get("sli_pct") is None:
        return None
    budget_fraction = (100.0 - target_pct) / 100.0
    if budget_fraction <= 0:
        return None
    return max(0.0, (100.0 - float(item["sli_pct"])) / 100.0 / budget_fraction)


def multi_window_burn_alerts(report: dict) -> list[dict]:
    alerts: list[dict] = []
    windows = report.get("windows", {})
    for family in ("youtube_availability", "upload_ceiling", "youtube_input_quality", "audio_correctness"):
        target = float(POLICIES[family]["target_pct"])
        rates = {
            label: burn_rate(measurement(windows.get(label, {}), family), target)
            for label in ("15m", "1h", "6h")
        }
        severity = ""
        rule = ""
        if rates["15m"] is not None and rates["1h"] is not None and rates["15m"] >= 14.4 and rates["1h"] >= 14.4:
            severity = "critical"
            rule = "fast_15m_1h"
        elif rates["1h"] is not None and rates["6h"] is not None and rates["1h"] >= 6.0 and rates["6h"] >= 6.0:
            severity = "warning"
            rule = "slow_1h_6h"
        if severity:
            alerts.append(
                {
                    "family": family,
                    "severity": severity,
                    "rule": rule,
                    "target_pct": target,
                    "burn_rates": {key: round(value, 3) if value is not None else None for key, value in rates.items()},
                    "automatic_recovery": False,
                }
            )
    return alerts


def latest_gate_status(report: dict) -> dict:
    result: dict[str, dict] = {}
    for family in POLICIES:
        official = str(POLICIES[family]["official_window"])
        if official == "sampled_checks":
            continue
        item = measurement(report.get("windows", {}).get(official, {}), family)
        result[family] = {
            key: item.get(key)
            for key in (
                "compliance_status",
                "measurement_status",
                "measurement_unknown_reasons",
                "sli_pct",
                "coverage_pct",
                "source_freshness_pct",
                "source_disagreement",
            )
        }
    return result


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    temporary.replace(path)


def run(args: argparse.Namespace) -> dict:
    state_root = args.state_root
    log_file = args.watchdog_events_file or state_root / "logs" / "youtube_watchdog.jsonl"
    db_path = args.database or state_root / "operational_reliability.sqlite3"
    now_ts = int(args.now or time.time())
    closed_hour_end = now_ts // 3600 * 3600
    report_end = now_ts // 60 * 60 if args.quick else closed_hour_end
    revision = git_revision(args.repo_root)
    revision_value = str(revision.get("exact_tag") or revision.get("head") or "")

    inserted_ledger = 0
    compacted_ledger = 0
    if not args.quick:
        compacted_ledger = compact_duplicate_daily_checkpoints(db_path)
        inserted_ledger = backfill_same_url_events(
            db_path,
            iter_jsonl(log_file),
            revision=revision_value,
        )
    report = build_sli_report(
        SliReportContext(
            youtube_watchdog_events_file=log_file,
            prometheus_url=args.prometheus_url,
            operational_reliability_db_file=None if args.quick else db_path,
            viewer_synthetic_events_file=state_root / "logs" / "viewer_synthetic_status.jsonl",
            subsystems_status_events_file=state_root / "logs" / "subsystems_status.jsonl",
        ),
        windows=BURN_WINDOWS,
        end_time=str(report_end),
        prometheus_url=args.prometheus_url,
    )
    if args.quick:
        input_feedback = measurement(report.get("windows", {}).get("1h", {}), "youtube_input_quality")
        payload = {
            "schema_version": 1,
            "checked_at_utc": datetime.fromtimestamp(report_end, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "status": "ok" if revision["deployment_revision_valid"] else "unknown",
            "revision": revision,
            "fast_feedback": {
                "youtube_input_quality": {
                    key: input_feedback.get(key)
                    for key in (
                        "measurement_status",
                        "measurement_unknown_reasons",
                        "target_met_on_observed_samples",
                        "sli_pct",
                        "coverage_pct",
                        "source_freshness_pct",
                        "source_disagreement",
                    )
                }
            },
            "multi_window_burn_alerts": multi_window_burn_alerts(report),
            "no_automatic_recovery": True,
            "formal_sli": False,
        }
        write_json_atomic(args.output, payload)
        return payload
    hourly_rows = upsert_rollups(
        db_path,
        report,
        granularity="hourly",
        bucket_start_ts=closed_hour_end - 3600,
        bucket_end_ts=closed_hour_end,
        window_label="1h",
        revision=revision_value,
    )

    now_jst = datetime.fromtimestamp(closed_hour_end, JST)
    day_end_jst = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_ts = int(day_end_jst.timestamp())
    daily_report = build_sli_report(
        SliReportContext(
            youtube_watchdog_events_file=log_file,
            prometheus_url=args.prometheus_url,
            operational_reliability_db_file=db_path,
            viewer_synthetic_events_file=state_root / "logs" / "viewer_synthetic_status.jsonl",
            subsystems_status_events_file=state_root / "logs" / "subsystems_status.jsonl",
        ),
        windows="24h",
        end_time=str(day_end_ts),
        prometheus_url=args.prometheus_url,
    )
    daily_rows = upsert_rollups(
        db_path,
        daily_report,
        granularity="daily_jst",
        bucket_start_ts=day_end_ts - 86400,
        bucket_end_ts=day_end_ts,
        window_label="24h",
        revision=revision_value,
    )
    removed = prune(db_path, now_ts=closed_hour_end, retention_days=args.retention_days)
    payload = {
        "schema_version": 1,
        "checked_at_utc": datetime.fromtimestamp(closed_hour_end, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "status": "ok" if revision["deployment_revision_valid"] else "unknown",
        "retention_days": args.retention_days,
        "database": str(db_path),
        "revision": revision,
        "ledger_backfill_inserted": inserted_ledger,
        "ledger_duplicate_daily_checkpoints_compacted": compacted_ledger,
        "hourly_rollup_rows": hourly_rows,
        "daily_rollup_rows": daily_rows,
        "pruned_rows": removed,
        "row_counts": counts(db_path),
        "formal_gates": latest_gate_status(report),
        "multi_window_burn_alerts": multi_window_burn_alerts(report),
        "no_automatic_recovery": True,
    }
    write_json_atomic(args.output, payload)
    return payload


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Persist Same URL and SLI reliability evidence")
    value.add_argument("--repo-root", type=Path, default=ROOT)
    value.add_argument(
        "--state-root",
        type=Path,
        default=Path(os.environ.get("STREAM_RUNTIME_STATE_DIR", ROOT / ".state" / "arena-monitor")),
    )
    value.add_argument("--database", type=Path)
    value.add_argument("--watchdog-events-file", type=Path)
    value.add_argument(
        "--prometheus-url",
        default=os.environ.get("STREAM_V3_PROMETHEUS_URL", "http://127.0.0.1:9090"),
    )
    value.add_argument("--retention-days", type=int, default=RETENTION_DAYS)
    value.add_argument("--now", type=int, default=0)
    value.add_argument("--quick", action="store_true")
    value.add_argument("--output", type=Path)
    return value


def main() -> int:
    args = parser().parse_args()
    if args.output is None:
        args.output = args.state_root / "operational_reliability_status.json"
    try:
        payload = run(args)
    except Exception as exc:
        print(f"operational-reliability-rollup: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
