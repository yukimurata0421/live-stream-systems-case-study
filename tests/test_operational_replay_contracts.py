from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

from stream_core.commands import health as health_command  # type: ignore
from stream_core.common.json_io import iter_jsonl  # type: ignore
from stream_core.common.timeutil import parse_utc_ts  # type: ignore
from stream_core.diagnostics import ingest_contract, needrestart_contract, suite as diagnostics_suite  # type: ignore
from stream_core.notifications import incidents as notify_incidents  # type: ignore


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def write_ok_visual_reports(root: Path, *, ts_utc: str = "1970-01-01T00:16:00Z") -> tuple[Path, Path]:
    overlay = root / "stream1090_report.jsonl"
    upstream = root / "upstream_stream1090_report.jsonl"
    common = {
        "ts_utc": ts_utc,
        "judgment": "report_only_ok",
        "warnings": [],
        "checks": {"position_change_count": 1, "messages_delta": 10},
        "baseline": {"warn_rate": 0.0, "alert": False},
    }
    write_jsonl(overlay, [{**common, "target": "overlay_stream1090"}])
    write_jsonl(upstream, [{**common, "target": "upstream_readsb_tar1090_stream1090"}])
    return overlay, upstream


def base_observe_payload() -> dict:
    return {
        "api_report_judgment": "ok",
        "fast_mode_judgment": "ok_none",
        "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
        "remote_warning_restart_judgment": "ok_single_or_none",
        "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
        "rtmps_ssl_tls_judgment": "ok_none",
        "public_probe_judgment": "ok_none",
        "checks": {
            "current_fail": False,
            "historical_degraded": False,
            "youtube_current_degraded": False,
            "youtube_observability_current_fail": False,
            "fast_mode_current_active": False,
            "youtube_current_status": "ok",
            "youtube_current_judgment": "ok",
            "youtube_stats_stale": False,
            "pulse_pass": True,
        },
    }


class NotificationReplayContractTests(unittest.TestCase):
    def collect(self, root: Path, payload: dict, *, stats: dict | None = None, now_ts: int = 1000) -> list[dict]:
        overlay, upstream = write_ok_visual_reports(root)
        stats_file = root / "youtube_watchdog_stats.json"
        if stats is not None:
            stats_file.write_text(json.dumps(stats), encoding="utf-8")
        return notify_incidents.collect_notification_incidents(
            observe_payload=lambda _hours: (0, payload, ""),
            stream1090_report_events_file=overlay,
            upstream_report_events_file=upstream,
            youtube_watchdog_stats_file=stats_file if stats is not None else None,
            now_ts=now_ts,
            report_stale_sec=1800,
        )

    def test_recovered_history_replay_does_not_emit_current_rtmps_or_public_probe_notifications(self) -> None:
        payload = {
            **base_observe_payload(),
            "rtmps_ssl_tls_judgment": "investigate_rtmps_ssl_tls_repeated",
            "rtmps_ssl_tls_count_24h": 12,
            "public_probe_judgment": "observe_public_probe_noise_clustered",
            "public_probe_degraded_count_24h": 10,
            "checks": {**base_observe_payload()["checks"], "historical_degraded": True},
        }
        with tempfile.TemporaryDirectory() as td:
            incidents = self.collect(Path(td), payload)

        ids = {item["id"] for item in incidents}
        self.assertNotIn("stream:current_fail", ids)
        self.assertNotIn("rtmps:ssl_tls_specific_event", ids)
        self.assertNotIn("public_probe:429_or_bot_confirmation_repeated", ids)

    def test_current_failure_replay_adds_rtmps_and_public_probe_context(self) -> None:
        payload = {
            **base_observe_payload(),
            "rtmps_ssl_tls_judgment": "investigate_rtmps_ssl_tls_immediate",
            "rtmps_ssl_tls_count_1h": 1,
            "rtmps_ssl_tls_count_24h": 3,
            "stream_engine_ffmpeg_ssl_tls_count_24h": 3,
            "fast_recovery_ssl_tls_count_24h": 0,
            "public_probe_judgment": "observe_public_probe_noise_clustered",
            "public_probe_degraded_count_1h": 2,
            "public_probe_degraded_count_24h": 4,
            "public_probe_authoritative_live_ok_count_24h": 4,
            "checks": {
                **base_observe_payload()["checks"],
                "current_fail": True,
                "youtube_current_status": "warn",
                "youtube_current_judgment": "ng",
            },
        }
        with tempfile.TemporaryDirectory() as td:
            incidents = self.collect(Path(td), payload)

        ids = {item["id"] for item in incidents}
        self.assertIn("stream:current_fail", ids)
        self.assertIn("rtmps:ssl_tls_specific_event", ids)
        self.assertIn("public_probe:429_or_bot_confirmation_repeated", ids)

    def test_encoder_gap_replay_requires_fresh_current_stats(self) -> None:
        payload = {
            **base_observe_payload(),
            "encoder_gap_enable_auto_stop_false_judgment": "observe_encoder_gap_viewer_state",
            "encoder_gap_enable_auto_stop_false_sample_count_24h": 3,
            "encoder_gap_enable_auto_stop_false_duration_sec_24h": 345,
        }
        stale_stats = {
            "ts_utc": "1970-01-01T00:00:00Z",
            "oauth_enable_auto_stop": False,
            "api_live_state": "live",
            "stream_active": False,
            "ingest_connected": False,
            "local_ok": False,
            "ffmpeg_pid": 0,
        }
        current_gap_stats = {
            **stale_stats,
            "ts_utc": "1970-01-01T00:16:40Z",
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stale = self.collect(root, payload, stats=stale_stats, now_ts=1000)
            current = self.collect(root, payload, stats=current_gap_stats, now_ts=1000)

        self.assertNotIn("youtube:enable_auto_stop_false_encoder_gap", {item["id"] for item in stale})
        self.assertIn("youtube:enable_auto_stop_false_encoder_gap", {item["id"] for item in current})

    def test_visual_report_recovery_evidence_uses_latest_ok_sample(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            overlay, upstream = write_ok_visual_reports(root, ts_utc="1970-01-01T00:20:00Z")
            recovered_ts, evidence = notify_incidents.recovery_observation_for_incident(
                "stream1090:overlay_report",
                1300,
                stream1090_report_events_file=overlay,
                upstream_report_events_file=upstream,
            )

        self.assertEqual(recovered_ts, 1200)
        self.assertIn("judgment=report_only_ok", evidence)
        self.assertIn("messages_delta=10", evidence)


class HealthSummaryReplayContractTests(unittest.TestCase):
    def test_health_summary_text_keeps_ffmpeg_attempt_episode_cluster_counts_separate(self) -> None:
        payload = {
            "pass": True,
            "remote_warning_restart_count_1h": 0,
            "remote_warning_restart_count_24h": 0,
            "remote_warning_restart_judgment": "ok_single_or_none",
            "public_probe_degraded_count_1h": 0,
            "public_probe_degraded_count_24h": 0,
            "public_probe_authoritative_live_ok_count_24h": 0,
            "public_probe_judgment": "ok_none",
            "fast_mode_episode_count_24h": 0,
            "fast_mode_active_duration_sec_24h": 0,
            "fast_mode_api_units_estimated_24h": 0,
            "fast_mode_judgment": "ok_none",
            "api_report_judgment": "ok",
            "ffmpeg_tcp_send_mbps_24h_p50": 3.2,
            "ffmpeg_tcp_send_mbps_24h_p95": 4.4,
            "ffmpeg_tcp_send_mbps_24h_max": 4.9,
            "ffmpeg_tcp_send_mbps_24h_over_5mbps_duration_sec": 0,
            "ffmpeg_tcp_send_budget_judgment": "ok_within_budget",
            "encoder_gap_enable_auto_stop_false_duration_sec_24h": 0,
            "stream_engine_ffmpeg_restart_attempts_24h": 12,
            "stream_engine_ffmpeg_restart_retry_episodes_24h": 2,
            "stream_engine_ffmpeg_restart_incident_clusters_24h": 2,
            "stream_engine_ffmpeg_restart_incident_root_causes_24h": {"rtmps_tls_connect_cluster": 2},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
            "stream_engine_ffmpeg_exit_224_count_24h": 0,
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "rtmps_ssl_tls_count_1h": 0,
            "rtmps_ssl_tls_count_24h": 12,
            "rtmps_ssl_tls_judgment": "investigate_rtmps_ssl_tls_repeated",
            "checks": {"current_fail": False, "historical_degraded": True},
        }

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = health_command.health_summary(
                observe=lambda _hours: (0, payload, ""),
                windows="24",
                json_output=False,
            )
        text = buf.getvalue()

        self.assertEqual(rc, 0)
        self.assertIn("ffmpeg_restart_attempts_24h=12", text)
        self.assertIn("ffmpeg_restart_episodes_24h=2", text)
        self.assertIn("ffmpeg_restart_clusters_24h=2", text)
        self.assertIn("ffmpeg_restart_root_causes_24h={'rtmps_tls_connect_cluster': 2}", text)

    def test_health_summary_json_preserves_per_window_returncode_and_error(self) -> None:
        def observe(hours: int) -> tuple[int, dict, str]:
            if hours == 1:
                return 0, {"pass": True, "checks": {"current_fail": False}}, ""
            return 2, {}, "observe failed"

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = health_command.health_summary(observe=observe, windows="1,8", json_output=True)
        payload = json.loads(buf.getvalue())

        self.assertEqual(rc, 2)
        self.assertEqual(payload["windows"][0]["returncode"], 0)
        self.assertEqual(payload["windows"][1]["returncode"], 2)
        self.assertEqual(payload["windows"][1]["error"], "observe failed")


class RemoteWarningReplayContractTests(unittest.TestCase):
    def context(self, log_dir: Path) -> health_command.HealthContext:
        return health_command.HealthContext(
            observe_stream_health_script=Path("observe_stream_health.py"),
            log_base_dir=log_dir,
            fast_recovery_events_file=log_dir / "fast_recovery_events.jsonl",
            youtube_watchdog_events_file=log_dir / "youtube_watchdog.jsonl",
            run=lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="{}", stderr=""),
            iter_jsonl=iter_jsonl,
            parse_utc_ts=parse_utc_ts,
        )

    def test_remote_warning_compare_pairs_restart_with_before_and_after_watchdog_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log_dir = Path(td)
            write_jsonl(
                log_dir / "youtube_watchdog.jsonl",
                [
                    {"ts_utc": "1970-01-01T00:09:30Z", "status": "ok", "oauth_stream_status": "active", "oauth_stream_health_status": "good"},
                    {"ts_utc": "1970-01-01T00:10:30Z", "status": "warn", "oauth_stream_status": "active", "oauth_stream_health_status": "noData"},
                ],
            )
            write_jsonl(
                log_dir / "fast_recovery_events.jsonl",
                [
                    {
                        "ts_utc": "1970-01-01T00:10:00Z",
                        "kind": "restart",
                        "trigger": "remote_warning",
                        "message": "youtube pre-loss warning",
                        "metrics": {"bytes_sent_delta": 10, "lastsnd_ms": 100, "notsent": 0, "unacked": 0},
                        "youtube_hint": {"oauth_stream_status": "active", "oauth_stream_health_status": "noData"},
                    },
                    {"ts_utc": "1970-01-01T00:11:00Z", "kind": "restart", "trigger": "tcp_stall"},
                ],
            )

            payload = health_command.remote_warning_comparison_payload(
                self.context(log_dir),
                hours=1,
                limit=5,
                now_ts=1200,
            )

        self.assertEqual(payload["remote_warning_restart_count"], 1)
        event = payload["events"][0]
        self.assertEqual(event["youtube_watchdog_before"]["status"], "ok")
        self.assertEqual(event["youtube_watchdog_after"]["status"], "warn")
        self.assertEqual(event["youtube_hint"]["oauth_stream_health_status"], "noData")

    def test_remote_warning_compare_sorts_newest_first_and_applies_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log_dir = Path(td)
            write_jsonl(
                log_dir / "youtube_watchdog.jsonl",
                [{"ts_utc": "1970-01-01T00:01:00Z", "status": "ok"}],
            )
            write_jsonl(
                log_dir / "fast_recovery_events.jsonl",
                [
                    {"ts_utc": "1970-01-01T00:05:00Z", "kind": "restart", "trigger": "remote_warning", "message": "old"},
                    {"ts_utc": "1970-01-01T00:15:00Z", "kind": "restart", "trigger": "remote_warning", "message": "new"},
                ],
            )

            payload = health_command.remote_warning_comparison_payload(
                self.context(log_dir),
                hours=1,
                limit=1,
                now_ts=1200,
            )

        self.assertEqual(payload["remote_warning_restart_count"], 2)
        self.assertEqual(payload["shown"], 1)
        self.assertEqual(payload["events"][0]["message"], "new")


class DiagnosticsReplayContractTests(unittest.TestCase):
    def test_ingest_contract_keeps_legacy_rtmp_warn_nonfatal_but_placeholder_fatal(self) -> None:
        legacy_status = ingest_contract.stream_ingest_endpoint_status(
            lambda _path: {"RTMP_URL": "rtmp://a.rtmp.youtube.com/live2/REPLACE_WITH_TEST_STREAM_KEY", "STREAM_KEY": "REPLACE_WITH_TEST_STREAM_KEY"}
        )
        placeholder_status = ingest_contract.stream_ingest_endpoint_status(
            lambda _path: {"RTMP_URL": "rtmps://a.rtmps.youtube.com:443/live2/YOUR_STREAM_KEY", "STREAM_KEY": "YOUR_STREAM_KEY"}
        )
        legacy_result = ingest_contract.ingest_result(legacy_status)
        placeholder_result = ingest_contract.ingest_result(placeholder_status)

        self.assertTrue(legacy_result.ok)
        self.assertEqual(legacy_result.severity, "warn")
        self.assertFalse(legacy_result.fatal)
        self.assertFalse(placeholder_result.ok)
        self.assertEqual(placeholder_result.severity, "fail")
        self.assertTrue(placeholder_result.fatal)

    def test_needrestart_contract_requires_override_units_and_disable_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            good = Path(td) / "good.conf"
            bad = Path(td) / "bad.conf"
            good.write_text(
                "$nrconf{override_rc}{qr(^adsb-streamnew-youtube-stream.service$)} = 0;\n"
                "$nrconf{override_rc}{qr(^adsb-streamnew-auto-dj.service$)} = 0;\n",
                encoding="utf-8",
            )
            bad.write_text("adsb-streamnew-youtube-stream.service\n", encoding="utf-8")

            good_status = needrestart_contract.needrestart_contract_status(good)
            bad_status = needrestart_contract.needrestart_contract_status(bad)

        self.assertTrue(good_status["ok"])
        self.assertFalse(bad_status["ok"])
        self.assertFalse(bad_status["has_override_rc"])
        self.assertFalse(bad_status["disables_restart"])

    def test_contract_payload_lists_only_fatal_checks(self) -> None:
        warn_ingest = ingest_contract.ingest_result(
            ingest_contract.stream_ingest_endpoint_status(
                lambda _path: {"RTMP_URL": "rtmp://a.rtmp.youtube.com/live2/REPLACE_WITH_TEST_STREAM_KEY", "STREAM_KEY": "REPLACE_WITH_TEST_STREAM_KEY"}
            )
        )
        failed_needrestart = needrestart_contract.needrestart_result_from_status(
            {"ok": False, "path": "/tmp/missing", "reason": "missing", "has_stream_units": False, "has_override_rc": False, "disables_restart": False}
        )
        payload = diagnostics_suite.contract_payload([warn_ingest, failed_needrestart])

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["warn_count"], 1)
        self.assertEqual(payload["fatal_checks"], ["needrestart:stream_override"])


if __name__ == "__main__":
    unittest.main()
