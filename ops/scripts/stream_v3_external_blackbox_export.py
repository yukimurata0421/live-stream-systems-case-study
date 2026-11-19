"""Export regional uptime-check evidence to stdout without writing runtime state."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os

from stream_v3_external_blackbox_import import access_token, fetch_time_series, iso_utc


METRICS = ("check_passed", "http_status", "error_code", "content_mismatch", "request_latency")


def timestamp(value: str) -> int:
    try:
        point = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an ISO 8601 timestamp") from exc
    if point.tzinfo is None:
        raise argparse.ArgumentTypeError("include Z or a UTC offset")
    return int(point.timestamp())


def export_window(*, project: str, host: str, check_id: str | None, token: str,
                  start_ts: int, end_ts: int, timeout_sec: float) -> dict:
    samples = []
    errors = {}
    for name in METRICS:
        try:
            data = fetch_time_series(project=project, host=host, check_id=check_id, token=token,
                                     start_ts=start_ts, end_ts=end_ts, timeout_sec=timeout_sec,
                                     metric_type=f"monitoring.googleapis.com/uptime_check/{name}")
        except RuntimeError as exc:
            errors[name] = str(exc)[:240]
            continue
        for series in data["timeSeries"]:
            labels = series["metric"]["labels"]
            for point in series.get("points", []):
                samples.append({"metric": name, "check_id": labels.get("check_id"),
                                "location": labels.get("checker_location"),
                                "end_time": point["interval"]["endTime"], "value": point["value"]})
    return {"project": project, "host": host, "check_id_filter": check_id,
            "start_utc": iso_utc(start_ts), "end_utc": iso_utc(end_ts), "errors": errors,
            "points": sorted(samples, key=lambda p: (p["end_time"], p["check_id"] or "",
                                                     p["location"] or "", p["metric"]))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=os.environ.get("STREAM_V3_GCP_MONITORING_PROJECT"))
    parser.add_argument("--host", required=True)
    parser.add_argument("--check-id")
    parser.add_argument("--start", type=timestamp, required=True)
    parser.add_argument("--end", type=timestamp, required=True)
    parser.add_argument("--gcloud-bin", default="/usr/bin/gcloud")
    args = parser.parse_args()
    if not args.project or args.start >= args.end:
        parser.error("supply a project and a start earlier than the end")
    try:
        token = access_token(args.gcloud_bin, timeout_sec=20)
    except RuntimeError as exc:
        parser.exit(1, f"{exc}\n")
    result = export_window(project=args.project, host=args.host, check_id=args.check_id,
                           start_ts=args.start, end_ts=args.end, token=token, timeout_sec=20)
    print(json.dumps(result, indent=2))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
