from __future__ import annotations

import importlib.util
import gzip
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops" / "scripts" / "report_youtube_api_cost.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("report_youtube_api_cost", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load report_youtube_api_cost module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class YouTubeApiCostReportTests(unittest.TestCase):
    def test_default_log_file_follows_state_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            with mock.patch.dict(
                "os.environ",
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                },
                clear=False,
            ):
                mod = _load_module()
            self.assertEqual(
                mod.DEFAULT_LOG_FILE,
                state_dir / "logs" / "youtube_api_calls.jsonl",
            )

    def test_default_target_day_is_previous_closed_day(self) -> None:
        mod = _load_module()
        now_local = datetime(2026, 5, 5, 10, 0, tzinfo=timezone.utc)
        target = mod.resolve_target_day(now_local, "", include_open_day=False)
        self.assertEqual(target.isoformat(), "2026-05-04")

    def test_open_day_window_applies_lag(self) -> None:
        mod = _load_module()
        now_ts = int(datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc).timestamp())
        w = mod.build_window(
            now_utc_ts=now_ts,
            tz_name="UTC",
            day_arg="2026-05-05",
            include_open_day=True,
            lag_sec=120,
        )
        self.assertTrue(w.open_day)
        self.assertEqual(w.effective_end_ts, now_ts - 120)

    def test_main_aggregates_for_explicit_day(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "youtube_api_calls.jsonl"
            target_day = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
            start = datetime.fromisoformat(f"{target_day}T00:00:00+00:00")

            events = [
                {
                    "ts_utc": (start + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                    "source": "resolve_live_video_id",
                    "method": "search.list",
                    "cost_units": 100,
                    "status": "ok",
                    "quota_exceeded": False,
                },
                {
                    "ts_utc": (start + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
                    "source": "check_data_api",
                    "method": "videos.list",
                    "cost_units": 1,
                    "status": "http_error",
                    "quota_exceeded": True,
                },
                {
                    "ts_utc": (start - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                    "source": "outside",
                    "method": "search.list",
                    "cost_units": 100,
                    "status": "ok",
                    "quota_exceeded": False,
                },
            ]
            with log_file.open("w", encoding="utf-8") as fh:
                for item in events:
                    fh.write(json.dumps(item, ensure_ascii=False))
                    fh.write("\n")
                fh.write("not a json line\n")

            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(log_file),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()

            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("status"), "degraded")
            self.assertEqual(payload.get("reason"), "telemetry_coverage_degraded")
            self.assertEqual(payload.get("totals", {}).get("calls"), 2)
            self.assertEqual(payload.get("totals", {}).get("units"), 101)
            self.assertEqual(payload.get("totals", {}).get("quota_exceeded_events"), 1)
            self.assertEqual(payload.get("by_method", {}).get("search.list", {}).get("calls"), 1)
            self.assertEqual(payload.get("by_method", {}).get("videos.list", {}).get("calls"), 1)
            self.assertEqual(payload.get("ingest", {}).get("parse_errors"), 1)
            self.assertFalse(bool(payload.get("ingest", {}).get("coverage_ok", True)))

    def test_main_reads_rotated_jsonl_logs(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "youtube_api_calls.jsonl"
            target_day = "2026-05-01"
            start = datetime.fromisoformat(f"{target_day}T00:00:00+00:00")
            records = [
                (
                    log_file.with_name(log_file.name + ".2.gz"),
                    {
                        "ts_utc": (start + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                        "source": "youtube_live_api_get",
                        "method": "liveBroadcasts.list",
                        "cost_units": 1,
                        "status": "ok",
                        "quota_exceeded": False,
                    },
                ),
                (
                    log_file.with_name(log_file.name + ".1"),
                    {
                        "ts_utc": (start + timedelta(hours=12)).isoformat().replace("+00:00", "Z"),
                        "source": "youtube_live_api_get",
                        "method": "liveStreams.list",
                        "cost_units": 1,
                        "status": "ok",
                        "quota_exceeded": False,
                    },
                ),
                (
                    log_file,
                    {
                        "ts_utc": (start + timedelta(hours=23, minutes=59)).isoformat().replace("+00:00", "Z"),
                        "source": "check_data_api",
                        "method": "videos.list",
                        "cost_units": 1,
                        "status": "ok",
                        "quota_exceeded": False,
                    },
                ),
            ]
            for path, record in records:
                if path.name.endswith(".gz"):
                    with gzip.open(path, "wt", encoding="utf-8") as fh:
                        fh.write(json.dumps(record) + "\n")
                else:
                    path.write_text(json.dumps(record) + "\n", encoding="utf-8")
            log_file.with_name(log_file.name + ".lock").write_text("not json\n", encoding="utf-8")

            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(log_file),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
                "--coverage-start-gap-mode",
                "warn",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()

            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("totals", {}).get("calls"), 3)
            self.assertEqual(payload.get("totals", {}).get("units"), 3)
            self.assertEqual(payload.get("ingest", {}).get("parse_errors"), 0)
            self.assertEqual(len(payload.get("ingest", {}).get("log_files", [])), 3)
            self.assertEqual(payload.get("ingest", {}).get("coverage_gap_start_sec"), 3600)

    def test_missing_log_returns_degraded(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "not_found.jsonl"
            target_day = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(missing),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("status"), "degraded")
            self.assertEqual(payload.get("reason"), "telemetry_missing")
            self.assertFalse(bool(payload.get("ingest", {}).get("coverage_ok", True)))

    def test_out_of_window_records_do_not_satisfy_coverage(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "youtube_api_calls.jsonl"
            target_day = "2026-05-01"
            start = datetime.fromisoformat(f"{target_day}T00:00:00+00:00")
            out_of_window_event = {
                "ts_utc": (start - timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
                "source": "outside",
                "method": "videos.list",
                "cost_units": 1,
                "status": "ok",
                "quota_exceeded": False,
            }
            log_file.write_text(json.dumps(out_of_window_event) + "\n", encoding="utf-8")

            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(log_file),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("status"), "degraded")
            self.assertEqual(payload.get("reason"), "telemetry_coverage_degraded")
            self.assertEqual(payload.get("ingest", {}).get("coverage_reason"), "no in-window telemetry timestamps")

    def test_detects_end_gap_when_telemetry_stops_mid_window(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "youtube_api_calls.jsonl"
            target_day = "2026-05-01"
            start = datetime.fromisoformat(f"{target_day}T00:00:00+00:00")
            events = [
                {
                    "ts_utc": (start + timedelta(seconds=60)).isoformat().replace("+00:00", "Z"),
                    "source": "youtube_live_api_get",
                    "method": "liveBroadcasts.list",
                    "cost_units": 1,
                    "status": "ok",
                    "quota_exceeded": False,
                }
            ]
            with log_file.open("w", encoding="utf-8") as fh:
                for item in events:
                    fh.write(json.dumps(item) + "\n")

            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(log_file),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
                "--coverage-gap-grace-sec",
                "300",
                "--coverage-end-gap-grace-sec",
                "300",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("status"), "degraded")
            self.assertEqual(payload.get("reason"), "telemetry_coverage_degraded")
            self.assertIn("coverage gap at window end", str(payload.get("ingest", {}).get("coverage_reason", "")))

    def test_start_gap_warn_mode_keeps_status_ok(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "youtube_api_calls.jsonl"
            target_day = "2026-05-01"
            start = datetime.fromisoformat(f"{target_day}T00:00:00+00:00")
            # First telemetry is intentionally late to trigger start-gap warning.
            events = [
                {
                    "ts_utc": (start + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                    "source": "youtube_live_api_get",
                    "method": "liveBroadcasts.list",
                    "cost_units": 1,
                    "status": "ok",
                    "quota_exceeded": False,
                },
                {
                    "ts_utc": (start + timedelta(hours=23, minutes=59)).isoformat().replace("+00:00", "Z"),
                    "source": "youtube_live_api_get",
                    "method": "liveStreams.list",
                    "cost_units": 1,
                    "status": "ok",
                    "quota_exceeded": False,
                },
            ]
            with log_file.open("w", encoding="utf-8") as fh:
                for item in events:
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")

            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(log_file),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
                "--coverage-gap-grace-sec",
                "300",
                "--coverage-start-gap-mode",
                "warn",
                "--coverage-end-gap-grace-sec",
                "300",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("status"), "ok")
            self.assertTrue(bool(payload.get("ingest", {}).get("coverage_ok", False)))
            warnings = payload.get("ingest", {}).get("coverage_warnings", [])
            self.assertTrue(any("coverage gap at day start" in str(x) for x in warnings))
            self.assertEqual(payload.get("ingest", {}).get("coverage_window_sec"), 86400)
            self.assertEqual(payload.get("ingest", {}).get("coverage_observed_sec"), 22 * 3600 + 59 * 60)
            self.assertAlmostEqual(
                payload.get("ingest", {}).get("coverage_gap_start_ratio"),
                1 / 24,
                places=6,
            )
            self.assertAlmostEqual(
                payload.get("ingest", {}).get("coverage_observed_ratio"),
                (22 * 3600 + 59 * 60) / 86400,
                places=6,
            )

    def test_start_gap_warn_mode_still_degrades_on_end_gap(self) -> None:
        mod = _load_module()
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "youtube_api_calls.jsonl"
            target_day = "2026-05-01"
            start = datetime.fromisoformat(f"{target_day}T00:00:00+00:00")
            events = [
                {
                    "ts_utc": (start + timedelta(hours=8)).isoformat().replace("+00:00", "Z"),
                    "source": "youtube_live_api_get",
                    "method": "liveBroadcasts.list",
                    "cost_units": 1,
                    "status": "ok",
                    "quota_exceeded": False,
                },
                {
                    "ts_utc": (start + timedelta(hours=21, minutes=30)).isoformat().replace("+00:00", "Z"),
                    "source": "youtube_live_api_get",
                    "method": "liveStreams.list",
                    "cost_units": 1,
                    "status": "ok",
                    "quota_exceeded": False,
                },
            ]
            with log_file.open("w", encoding="utf-8") as fh:
                for item in events:
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")

            buf = io.StringIO()
            argv = [
                "report_youtube_api_cost.py",
                "--log-file",
                str(log_file),
                "--tz",
                "UTC",
                "--day",
                target_day,
                "--allow-near-boundary",
                "--allow-just-closed-day",
                "--coverage-gap-grace-sec",
                "300",
                "--coverage-start-gap-mode",
                "warn",
                "--coverage-end-gap-grace-sec",
                "300",
            ]
            with mock.patch("sys.argv", argv):
                with redirect_stdout(buf):
                    rc = mod.main()
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload.get("status"), "degraded")
            self.assertIn(
                "coverage gap at window end",
                str(payload.get("ingest", {}).get("coverage_reason", "")),
            )
            warnings = payload.get("ingest", {}).get("coverage_warnings", [])
            self.assertTrue(any("coverage gap at day start" in str(x) for x in warnings))


if __name__ == "__main__":
    unittest.main()
