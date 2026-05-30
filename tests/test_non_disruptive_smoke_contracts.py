from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(SRC / "stream_core"))

import cli as stream_cli  # type: ignore
from stream_core.cli_support import parser as stream_parser  # type: ignore
from stream_core.cli_support import router as stream_router  # type: ignore
from stream_core.cli_support.objective_sli import ObjectiveSliContext, objective_sli  # type: ignore
from stream_core.commands.stream1090_report import Stream1090ReportContext, stream1090_report  # type: ignore
from stream_v2.local_runtime import LocalRuntimeConfig, build_local_env  # type: ignore


def run_stream_v2_cli(args: list[str], *, timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    current = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(SRC) if not current else f"{SRC}{os.pathsep}{current}"
    return subprocess.run(
        [sys.executable, "-m", "stream_v2", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


class IsolatedLocalSmokeCliContractTests(unittest.TestCase):
    def test_local_smoke_dry_run_cli_is_isolated_and_non_mutating(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td) / "state"
            cp = run_stream_v2_cli(
                [
                    "local-smoke",
                    "--state-root",
                    str(state_root),
                    "--dry-run",
                    "--duration-sec",
                    "0",
                    "--no-browser",
                    "--display",
                    ":151",
                    "--overlay-port",
                    "18151",
                    "--pulse-sink",
                    "stream_v2_ci_sink",
                    "--stream1090-url",
                    "http://127.0.0.1:65535/stream1090/",
                ]
            )

            self.assertEqual(cp.returncode, 0, cp.stderr)
            payload = json.loads(cp.stdout)
            env = payload["env"]

        self.assertEqual(payload["mode"], "local_test")
        self.assertEqual(payload["safety"]["test_mode"], True)
        self.assertEqual(payload["safety"]["systemd_mutation"], "not used")
        self.assertEqual(payload["safety"]["production_root"], "read-only, not invoked")
        self.assertEqual(env["TEST_MODE"], "1")
        self.assertEqual(env["TEST_OUTPUT"], "null")
        self.assertEqual(env["STREAM_KEY"], "LOCAL_TEST_ONLY")
        self.assertEqual(env["RTMP_URL"], "rtmps://a.rtmps.youtube.com:443/live2/LOCAL_TEST_ONLY")
        self.assertEqual(env["AUTO_START_BROWSER"], "0")
        self.assertEqual(env["DISPLAY_NAME"], ":151")
        self.assertEqual(env["OVERLAY_PORT"], "18151")
        self.assertEqual(env["PULSE_SINK"], "stream_v2_ci_sink")
        self.assertEqual(env["PULSE_SOURCE"], "stream_v2_ci_sink.monitor")
        self.assertEqual(env["REQUIRE_SYSTEMD_LAUNCH"], "0")
        self.assertEqual(env["ALLOW_DIRECT_STREAM_SH"], "1")
        self.assertEqual(env["HEALTH_GATE_ABORT_ON_FOREIGN"], "0")
        self.assertEqual(env["TAKEOVER_ENABLED"], "0")
        self.assertIn(str(state_root.resolve()), env["STREAM_RUNTIME_STATE_DIR"])
        self.assertIn(str(state_root.resolve()), env["OVERLAY_DIR"])
        self.assertIn(str(state_root.resolve()), env["NOW_PLAYING_FILE"])
        self.assertNotEqual(env["DISPLAY_NAME"], ":99")
        self.assertNotEqual(env["PULSE_SINK"], "stream_sink")
        self.assertNotIn("local-smoke: starting", cp.stdout + cp.stderr)

    def test_local_env_write_cli_creates_private_test_mode_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td) / "state"
            env_file = Path(td) / "safe-local.env"
            cp = run_stream_v2_cli(
                [
                    "local-env",
                    "--state-root",
                    str(state_root),
                    "--env-file",
                    str(env_file),
                    "--write",
                    "--no-browser",
                    "--display",
                    ":152",
                    "--overlay-port",
                    "18152",
                    "--pulse-sink",
                    "stream_v2_ci_sink2",
                    "--pretty",
                ]
            )

            self.assertEqual(cp.returncode, 0, cp.stderr)
            payload = json.loads(cp.stdout)
            text = env_file.read_text(encoding="utf-8")
            mode = env_file.stat().st_mode & 0o777

        self.assertEqual(payload["written_env_file"], str(env_file.resolve()))
        self.assertEqual(mode, 0o600)
        self.assertIn("TEST_MODE=1", text)
        self.assertIn("STREAM_KEY=LOCAL_TEST_ONLY", text)
        self.assertIn("RTMP_URL=rtmps://a.rtmps.youtube.com:443/live2/LOCAL_TEST_ONLY", text)
        self.assertIn("REQUIRE_SYSTEMD_LAUNCH=0", text)
        self.assertIn("ALLOW_DIRECT_STREAM_SH=1", text)
        self.assertIn("TAKEOVER_ENABLED=0", text)
        self.assertIn("PULSE_SINK=stream_v2_ci_sink2", text)
        self.assertNotIn("YOUR_STREAM_KEY", text)

    def test_local_runtime_env_keeps_all_mutable_paths_under_isolated_state_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td) / "state"
            config = LocalRuntimeConfig(
                repo_root=ROOT,
                state_root=state_root,
                display=":153",
                overlay_port=18153,
                pulse_sink="stream_v2_contract_sink",
                output="file",
                output_file=state_root / "capture" / "out.mkv",
                start_browser=False,
            )

            env = build_local_env(config)

        isolated_keys = (
            "STREAM_RUNTIME_STATE_DIR",
            "STREAM_RUNTIME_LOG_DIR",
            "TEST_OUTPUT_FILE",
            "OVERLAY_DIR",
            "OVERLAY_SERVER_LOG_FILE",
            "BROWSER_PROFILE_DIR",
            "XVFB_LOG_FILE",
            "BROWSER_LOG_FILE",
            "STREAM_LOCK_DIR",
            "RUNTIME_STATE_FILE",
            "EVENT_LOG_FILE",
            "RESTART_REASON_FILE",
            "NOW_PLAYING_FILE",
            "NOW_PLAYING_SNAPSHOT_FILE",
            "PLAY_HISTORY_JSONL_FILE",
        )
        for key in isolated_keys:
            self.assertIn(str(state_root.resolve()), env[key], key)
        self.assertEqual(env["TEST_MODE"], "1")
        self.assertEqual(env["TEST_OUTPUT"], "file")
        self.assertEqual(env["AUTO_START_BROWSER"], "0")
        self.assertEqual(env["DISPLAY_NAME"], ":153")
        self.assertEqual(env["PULSE_SINK"], "stream_v2_contract_sink")
        self.assertEqual(env["RTMP_URL"], "rtmps://a.rtmps.youtube.com:443/live2/LOCAL_TEST_ONLY")


class ReadOnlyRoutineCommandContractTests(unittest.TestCase):
    def test_stream_new_read_only_commands_are_not_classified_as_systemd_mutation(self) -> None:
        read_only_commands = (
            "status",
            "logs",
            "history",
            "api-usage",
            "health-summary",
            "objective-sli",
            "memory-status",
            "resource-memory",
            "subsystems-status",
            "recovery-orchestrator",
            "shadow-once",
            "shadow-sli",
            "oauth-status",
            "remote-warning-compare",
            "stream1090-report",
            "upstream-report",
            "notify-status",
            "doctor",
            "contract-check",
        )
        for command in read_only_commands:
            self.assertFalse(stream_cli.command_requires_mutating_systemd(command, ""), command)

        self.assertFalse(stream_cli.command_requires_mutating_systemd("m", "status"))
        self.assertFalse(stream_cli.command_requires_mutating_systemd("m", "s"))

    def test_stream_new_mutating_commands_still_require_explicit_guard(self) -> None:
        for command in ("install", "start", "stop", "restart", "enable", "watch"):
            self.assertTrue(stream_cli.command_requires_mutating_systemd(command, ""), command)

        for action in ("on", "off", "pause", "resume", "start", "stop", "enter", "exit"):
            self.assertTrue(stream_cli.command_requires_mutating_systemd("maintenance", action), action)
            self.assertTrue(stream_cli.command_requires_mutating_systemd("m", action), action)

    def test_router_preserves_no_record_and_dry_run_for_safe_runtime_checks(self) -> None:
        calls: list[tuple[str, tuple, dict]] = []

        def record(name: str):
            def _fn(*args, **kwargs) -> int:
                calls.append((name, args, kwargs))
                return 0

            return _fn

        router = stream_router.CliRouter(
            maintenance_top_level_actions=stream_cli.MAINTENANCE_TOP_LEVEL_ACTIONS,
            maintenance_command_aliases=stream_cli.MAINTENANCE_COMMAND_ALIASES,
            guard_mutating_command=lambda command, action: 0,
            install=record("install"),
            start=record("start"),
            stop=record("stop"),
            restart=record("restart"),
            maintenance=record("maintenance"),
            enable=record("enable"),
            watch=record("watch"),
            status=record("status"),
            logs=record("logs"),
            history=record("history"),
            api_usage=record("api_usage"),
            health_summary=record("health_summary"),
            objective_sli=record("objective_sli"),
            memory_status=record("memory_status"),
            resource_memory=record("resource_memory"),
            subsystems_status=record("subsystems_status"),
            recovery_orchestrator=record("recovery_orchestrator"),
            shadow_once=record("shadow_once"),
            shadow_sli=record("shadow_sli"),
            oauth_status=record("oauth_status"),
            remote_warning_compare=record("remote_warning_compare"),
            stream1090_report=record("stream1090_report"),
            upstream_report=record("upstream_report"),
            notify_status=record("notify_status"),
            doctor=record("doctor"),
            contract_check=record("contract_check"),
        )

        def dispatch(argv: list[str]) -> tuple[str, tuple, dict]:
            args = stream_parser.build_parser().parse_args(argv)
            calls.clear()
            rc = stream_router.dispatch(args, router)
            self.assertEqual(rc, 0)
            return calls[-1]

        self.assertEqual(dispatch(["objective-sli", "--json", "--no-record"])[2], {"json_output": True, "record": False})
        self.assertEqual(dispatch(["memory-status", "--json", "--no-record"])[2], {"json_output": True, "record": False})
        self.assertEqual(dispatch(["resource-memory", "--json", "--no-record"])[2], {"json_output": True, "record": False})
        self.assertEqual(dispatch(["subsystems-status", "--json", "--no-record"])[2], {"json_output": True, "record": False})
        self.assertEqual(dispatch(["recovery-orchestrator", "--json", "--no-record"])[2], {"json_output": True, "record": False})
        self.assertEqual(dispatch(["shadow-once", "--json", "--no-record"])[2], {"json_output": True, "record": False})
        self.assertEqual(dispatch(["notify-status", "--dry-run"])[2], {"dry_run": True, "force_test": False})

        stream_report = dispatch(["stream1090-report", "--json", "--no-record", "--base-url", "http://127.0.0.1:18080"])
        self.assertEqual(stream_report[0], "stream1090_report")
        self.assertFalse(stream_report[2]["record"])
        self.assertTrue(stream_report[2]["json_output"])

        upstream_report = dispatch(["upstream-report", "--json", "--no-record", "--upstream-url", "http://127.0.0.1/stream1090/"])
        self.assertEqual(upstream_report[0], "upstream_report")
        self.assertFalse(upstream_report[2]["record"])
        self.assertTrue(upstream_report[2]["json_output"])


class NoRecordReportContractTests(unittest.TestCase):
    def test_objective_sli_no_record_prints_payload_without_snapshot_or_history_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            logs = root / "logs"
            logs.mkdir()
            ctx = ObjectiveSliContext(
                log_base_dir=logs,
                youtube_watchdog_events_file=logs / "youtube_watchdog.jsonl",
                fast_recovery_events_file=logs / "fast_recovery_events.jsonl",
                stream_engine_events_file=logs / "stream_engine_events.jsonl",
                stream1090_report_events_file=logs / "stream1090_report.jsonl",
                upstream_report_events_file=logs / "upstream_stream1090_report.jsonl",
                notify_events_file=logs / "stream_notify_events.jsonl",
                memory_status_events_file=logs / "memory_status.jsonl",
                objective_sli_file=root / "objective_sli.json",
                objective_sli_events_file=logs / "objective_sli.jsonl",
            )

            with mock.patch("builtins.print") as printed:
                rc = objective_sli(ctx, json_output=True, record=False)
            payload = json.loads(str(printed.call_args.args[0]))

            self.assertFalse(ctx.objective_sli_file.exists())
            self.assertFalse(ctx.objective_sli_events_file.exists())

        self.assertEqual(rc, 0)
        self.assertEqual(payload["source"], "stream-new objective-sli")
        self.assertEqual(payload["window_policy"]["api_quota_day_timezone"], "America/Los_Angeles")

    def test_stream1090_report_no_record_keeps_visual_probe_report_only_and_skips_history_append(self) -> None:
        append_calls: list[tuple[Path, dict]] = []

        def payload_func(**kwargs) -> dict:
            return {
                "mode": "report_only",
                "affects_restart": False,
                "affects_stream_restart": False,
                "target": "overlay_stream1090",
                "base_url": kwargs["base_url"],
                "map_path": "/stream1090/",
                "checks": {"sample_sec": kwargs["sample_sec"]},
                "visual_probe": {"enabled": kwargs["visual"], "judgment": "visual_probe_ok"},
                "warnings": [],
                "judgment": "report_only_ok",
            }

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ctx = Stream1090ReportContext(
                stream1090_report_events_file=root / "stream1090_report.jsonl",
                upstream_report_events_file=root / "upstream_stream1090_report.jsonl",
                stream1090_visual_dir=root / "visual",
                run=lambda *_args, **_kwargs: subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
                append_jsonl=lambda path, payload: append_calls.append((path, payload)),
                iter_jsonl=lambda _path: [],
                parse_utc_ts=lambda _text: 0,
                default_upstream_url=lambda: "http://127.0.0.1/stream1090/",
            )

            with mock.patch("builtins.print") as printed:
                rc = stream1090_report(
                    ctx,
                    payload_func=payload_func,
                    base_url="http://127.0.0.1:18080",
                    sample_sec=0,
                    timeout=1,
                    visual=True,
                    record=False,
                    json_output=True,
                )
            payload = json.loads(str(printed.call_args.args[0]))

            self.assertFalse(ctx.stream1090_report_events_file.exists())

        self.assertEqual(rc, 0)
        self.assertFalse(append_calls)
        self.assertEqual(payload["mode"], "report_only")
        self.assertFalse(payload["affects_stream_restart"])
        self.assertEqual(payload["visual_probe"]["judgment"], "visual_probe_ok")


if __name__ == "__main__":
    unittest.main()
