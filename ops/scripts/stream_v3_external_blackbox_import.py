#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


METRIC_TYPE = "monitoring.googleapis.com/uptime_check/check_passed"
DEFAULT_TARGETS = (
    "public_status=yukimurata0421.dev",
    "youtube_public_video=www.youtube.com",
)


def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_ts(value: object) -> int:
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return 0


def parse_target(value: str) -> tuple[str, str]:
    name, separator, host = value.partition("=")
    if not separator or not name.strip() or not host.strip():
        raise ValueError(f"invalid target {value!r}; expected name=host")
    return name.strip(), host.strip().lower()


def access_token(gcloud_bin: str, *, timeout_sec: float) -> str:
    try:
        completed = subprocess.run(
            [gcloud_bin, "auth", "application-default", "print-access-token"],
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_sec,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"access_token:{type(exc).__name__}") from exc
    token = completed.stdout.strip()
    if completed.returncode != 0 or not token:
        detail = completed.stderr.strip().splitlines()[-1][:200] if completed.stderr.strip() else "empty token"
        raise RuntimeError(f"access_token:{detail}")
    return token


def fetch_time_series(
    *,
    project: str,
    host: str,
    token: str,
    start_ts: int,
    end_ts: int,
    timeout_sec: float,
) -> dict:
    query = urllib.parse.urlencode(
        {
            "filter": (
                f'metric.type="{METRIC_TYPE}" '
                f'AND resource.type="uptime_url" '
                f'AND resource.labels.host="{host}"'
            ),
            "interval.startTime": iso_utc(start_ts),
            "interval.endTime": iso_utc(end_ts),
            "view": "FULL",
            "pageSize": 100,
        }
    )
    url = f"https://monitoring.googleapis.com/v3/projects/{urllib.parse.quote(project, safe='')}/timeSeries?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "stream-v3-arena-gcp-blackbox-import/1.0",
            "X-Goog-User-Project": project,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read(512).decode("utf-8", errors="replace")
        except OSError:
            detail = ""
        raise RuntimeError(f"monitoring_api:http_{exc.code}:{detail[:200]}") from exc
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"monitoring_api:{type(exc).__name__}:{str(exc)[:160]}") from exc


def latest_location_samples(payload: dict) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    series_list = payload.get("timeSeries") if isinstance(payload.get("timeSeries"), list) else []
    for series in series_list:
        if not isinstance(series, dict):
            continue
        metric = series.get("metric") if isinstance(series.get("metric"), dict) else {}
        labels = metric.get("labels") if isinstance(metric.get("labels"), dict) else {}
        location = str(labels.get("checker_location") or "unknown")
        points = series.get("points") if isinstance(series.get("points"), list) else []
        for point in points:
            if not isinstance(point, dict):
                continue
            interval = point.get("interval") if isinstance(point.get("interval"), dict) else {}
            end_ts = parse_ts(interval.get("endTime"))
            value = point.get("value") if isinstance(point.get("value"), dict) else {}
            passed = value.get("boolValue")
            if end_ts <= 0 or not isinstance(passed, bool):
                continue
            previous = latest.get(location)
            if previous is None or end_ts > int(previous["sample_ts"]):
                latest[location] = {"sample_ts": end_ts, "passed": passed}
    return latest


def assess_target(
    payload: dict,
    *,
    host: str,
    now_ts: int,
    stale_sec: int,
    minimum_locations: int,
    minimum_pass_ratio: float,
) -> dict:
    samples = latest_location_samples(payload)
    fresh = {
        location: sample
        for location, sample in samples.items()
        if 0 <= now_ts - int(sample["sample_ts"]) <= stale_sec
    }
    passed_locations = sum(1 for sample in fresh.values() if sample["passed"] is True)
    pass_ratio = passed_locations / len(fresh) if fresh else None
    latest_ts = max((int(sample["sample_ts"]) for sample in samples.values()), default=0)
    if len(fresh) < minimum_locations:
        status = "unknown"
        reason = "minimum_fresh_checker_locations_not_met"
    elif pass_ratio is not None and pass_ratio >= minimum_pass_ratio:
        status = "ok"
        reason = "external_checker_quorum_passed"
    elif passed_locations > 0:
        status = "unknown"
        reason = "external_checker_source_disagreement"
    else:
        status = "failed"
        reason = "all_external_checker_locations_failed"
    return {
        "status": status,
        "reason": reason,
        "host": host,
        "latest_sample_at_utc": iso_utc(latest_ts) if latest_ts else None,
        "sample_age_seconds": max(0, now_ts - latest_ts) if latest_ts else None,
        "observed_locations": len(samples),
        "fresh_locations": len(fresh),
        "passed_locations": passed_locations,
        "pass_ratio": round(pass_ratio, 6) if pass_ratio is not None else None,
        "minimum_locations": minimum_locations,
        "minimum_pass_ratio": minimum_pass_ratio,
        "locations": {
            location: {
                "passed": sample["passed"],
                "sample_at_utc": iso_utc(int(sample["sample_ts"])),
                "sample_age_seconds": max(0, now_ts - int(sample["sample_ts"])),
            }
            for location, sample in sorted(samples.items())
        },
    }


def build_status(
    target_payloads: dict[str, tuple[str, dict]],
    *,
    project: str,
    now_ts: int,
    stale_sec: int,
    minimum_locations: int,
    minimum_pass_ratio: float,
) -> dict:
    targets = {
        name: assess_target(
            payload,
            host=host,
            now_ts=now_ts,
            stale_sec=stale_sec,
            minimum_locations=minimum_locations,
            minimum_pass_ratio=minimum_pass_ratio,
        )
        for name, (host, payload) in target_payloads.items()
    }
    statuses = {str(target.get("status")) for target in targets.values()}
    if "failed" in statuses:
        status = "failed"
        reason = "one_or_more_external_targets_failed"
    elif targets and statuses == {"ok"}:
        status = "ok"
        reason = "all_external_targets_passed"
    else:
        status = "unknown"
        reason = "external_target_evidence_incomplete"
    evidence_timestamps = [
        parse_ts(target.get("latest_sample_at_utc"))
        for target in targets.values()
        if parse_ts(target.get("latest_sample_at_utc")) > 0
    ]
    evidence_ts = min(evidence_timestamps) if len(evidence_timestamps) == len(targets) and targets else 0
    return {
        "schema_version": 2,
        "checked_at_utc": iso_utc(now_ts),
        "evidence_at_utc": iso_utc(evidence_ts) if evidence_ts else None,
        "evidence_age_seconds": max(0, now_ts - evidence_ts) if evidence_ts else None,
        "status": status,
        "reason": reason,
        "project": project,
        "execution_boundary": "google_cloud_monitoring_public_uptime_checkers",
        "targets": targets,
        "formal_sli": False,
        "do_not_merge_with_internal_availability_sli": True,
    }


def add_status_continuity(status: dict, previous: dict, *, now_ts: int, max_gap_sec: int = 900) -> dict:
    current_status = str(status.get("status") or "unknown")
    previous_status = str(previous.get("status") or "")
    previous_checked_ts = parse_ts(previous.get("checked_at_utc"))
    same_contiguous_status = (
        previous_status == current_status
        and previous_checked_ts > 0
        and 0 <= now_ts - previous_checked_ts <= max_gap_sec
    )
    try:
        previous_count = max(0, int(previous.get("consecutive_status_samples", 0) or 0))
    except (TypeError, ValueError):
        previous_count = 0
    if same_contiguous_status and previous_count > 0:
        since_ts = parse_ts(previous.get("status_since_utc")) or previous_checked_ts
        consecutive = previous_count + 1
    else:
        since_ts = now_ts
        consecutive = 1
    status["status_since_utc"] = iso_utc(since_ts)
    status["status_duration_seconds"] = max(0, now_ts - since_ts)
    status["consecutive_status_samples"] = consecutive
    return status


def read_previous_status(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.environ.get("STREAM_V3_GCP_MONITORING_PROJECT", ""))
    parser.add_argument("--target", action="append", default=[])
    parser.add_argument("--gcloud-bin", default="/usr/bin/gcloud")
    parser.add_argument("--timeout-sec", type=float, default=20.0)
    parser.add_argument("--lookback-sec", type=int, default=1200)
    parser.add_argument("--stale-sec", type=int, default=600)
    parser.add_argument("--minimum-locations", type=int, default=3)
    parser.add_argument("--minimum-pass-ratio", type=float, default=2.0 / 3.0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--now", type=int, default=0)
    args = parser.parse_args()
    if not args.project.strip():
        parser.error("--project or STREAM_V3_GCP_MONITORING_PROJECT is required")
    now_ts = args.now or int(datetime.now(timezone.utc).timestamp())
    try:
        targets = dict(parse_target(value) for value in (args.target or DEFAULT_TARGETS))
        token = access_token(args.gcloud_bin, timeout_sec=args.timeout_sec)
        payloads = {
            name: (
                host,
                fetch_time_series(
                    project=args.project,
                    host=host,
                    token=token,
                    start_ts=now_ts - args.lookback_sec,
                    end_ts=now_ts,
                    timeout_sec=args.timeout_sec,
                ),
            )
            for name, host in targets.items()
        }
        status = build_status(
            payloads,
            project=args.project,
            now_ts=now_ts,
            stale_sec=args.stale_sec,
            minimum_locations=args.minimum_locations,
            minimum_pass_ratio=args.minimum_pass_ratio,
        )
    except (RuntimeError, ValueError) as exc:
        status = {
            "schema_version": 2,
            "checked_at_utc": iso_utc(now_ts),
            "evidence_at_utc": None,
            "status": "unknown",
            "reason": "external_evidence_import_unavailable",
            "error": str(exc)[:240],
            "execution_boundary": "google_cloud_monitoring_public_uptime_checkers",
            "formal_sli": False,
            "do_not_merge_with_internal_availability_sli": True,
        }
    status = add_status_continuity(status, read_previous_status(args.output), now_ts=now_ts)
    write_atomic(args.output, status)
    print(json.dumps(status, ensure_ascii=False, separators=(",", ":")))
    return 1 if status["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
