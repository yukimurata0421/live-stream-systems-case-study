from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "stream_core"))
sys.path.insert(0, str(ROOT / "src" / "watchers"))

from engine import connectivity  # type: ignore
import stream_engine  # type: ignore
import fast_recovery  # type: ignore
from fast_recovery_core import executor  # type: ignore
from stream_core.notifications import incidents as notification_incidents  # type: ignore
from stream_core.notifications import connectivity_correlation  # type: ignore
from stream_core.notifications import status_loop as notification_status_loop  # type: ignore


class _Socket:
    def __init__(self, *, connect_error: OSError | None = None) -> None:
        self.connect_error = connect_error
        self.target = None

    def settimeout(self, _timeout: float) -> None:
        return None

    def connect(self, target) -> None:
        self.target = target
        if self.connect_error is not None:
            raise self.connect_error

    def close(self) -> None:
        return None


class ConnectivityProbeTests(unittest.TestCase):
    def test_endpoint_uses_rtmps_default_port_without_exposing_stream_key(self) -> None:
        endpoint = connectivity.endpoint_from_rtmp_url("rtmps://a.example.test/live2/secret")

        self.assertEqual(endpoint, connectivity.IngestEndpoint(host="a.example.test", port=443))

    def test_probe_distinguishes_dns_failure_from_tcp_failure(self) -> None:
        endpoint = connectivity.IngestEndpoint("a.example.test", 443)
        dns_failure = subprocess.CompletedProcess(["getent"], 2, "", "not found")

        result = connectivity.probe_ingest_connectivity(
            endpoint,
            run_command=lambda *_args, **_kwargs: dns_failure,
        )

        self.assertFalse(result.dns_ok)
        self.assertFalse(result.tcp_ok)

        tcp_failure = connectivity.probe_ingest_connectivity(
            endpoint,
            run_command=lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["getent"], 0, "203.0.113.10 STREAM test\n", ""
            ),
            socket_factory=lambda *_args: _Socket(connect_error=OSError("unreachable")),
        )
        self.assertTrue(tcp_failure.dns_ok)
        self.assertFalse(tcp_failure.tcp_ok)

    def test_probe_accepts_first_reachable_resolved_address(self) -> None:
        sockets = [_Socket(connect_error=OSError("first failed")), _Socket()]
        result = connectivity.probe_ingest_connectivity(
            connectivity.IngestEndpoint("a.example.test", 443),
            run_command=lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["getent"],
                0,
                "203.0.113.10 STREAM test\n203.0.113.11 STREAM test\n",
                "",
            ),
            socket_factory=lambda *_args: sockets.pop(0),
        )

        self.assertTrue(result.ready)


class StreamEngineConnectivityTests(unittest.TestCase):
    def _engine(self, root: str, **env_overrides: str) -> stream_engine.StreamEngine:
        env = {
            "BASE_DIR": root,
            "TEST_MODE": "0",
            "RTMP_URL": "rtmps://a.example.test:443/live2/key",
            "CONNECTIVITY_GATE_ENABLED": "1",
            "CONNECTIVITY_POLL_SEC": "2",
            "RENDER_SELF_RECOVERY_ENABLED": "1",
            "RENDER_SELF_RECOVERY_CONFIRMATIONS": "3",
            "RENDER_SELF_RECOVERY_GRACE_SEC": "0",
            "RENDER_SELF_RECOVERY_COOLDOWN_SEC": "60",
        }
        env.update(env_overrides)
        with mock.patch.dict(os.environ, env, clear=False):
            return stream_engine.StreamEngine(stream_engine.load_config())

    def test_ffmpeg_restart_gate_polls_and_releases_immediately_on_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            engine = self._engine(td)
            down = connectivity.ConnectivityResult(False, False, "DNS unavailable")
            ready = connectivity.ConnectivityResult(True, True, "ready")
            with (
                mock.patch.object(engine, "ingest_connectivity_probe", side_effect=[down, ready]),
                mock.patch.object(engine, "append_event") as append_event,
                mock.patch.object(engine, "write_runtime_snapshot"),
                mock.patch.object(engine, "log"),
                mock.patch.object(stream_engine.time, "sleep") as sleep,
            ):
                self.assertTrue(engine.wait_for_ingest_connectivity())

            sleep.assert_called_once_with(2.0)
            self.assertEqual(
                [call.args[0] for call in append_event.call_args_list],
                ["connectivity_wait_started", "connectivity_wait_recovered"],
            )

    def test_long_connectivity_outage_keeps_fixed_two_second_poll_without_child_launches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            engine = self._engine(td)
            down = connectivity.ConnectivityResult(False, False, "DNS unavailable")
            ready = connectivity.ConnectivityResult(True, True, "ready")
            with (
                mock.patch.object(
                    engine,
                    "ingest_connectivity_probe",
                    side_effect=[*[down] * 60, ready],
                ) as probe,
                mock.patch.object(engine, "append_event") as append_event,
                mock.patch.object(engine, "write_runtime_snapshot"),
                mock.patch.object(engine, "log"),
                mock.patch.object(stream_engine.time, "sleep") as sleep,
            ):
                self.assertTrue(engine.wait_for_ingest_connectivity())

            self.assertEqual(probe.call_count, 61)
            self.assertEqual(sleep.call_count, 60)
            self.assertTrue(all(call.args == (2.0,) for call in sleep.call_args_list))
            self.assertEqual(
                [call.args[0] for call in append_event.call_args_list],
                ["connectivity_wait_started", "connectivity_wait_recovered"],
            )

    def test_stale_render_restarts_only_browser_and_keeps_ffmpeg_child(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            engine = self._engine(td)
            ffmpeg = mock.Mock()
            ffmpeg.pid = 4321
            browser = mock.Mock()
            browser.pid = 1234
            engine.ffmpeg_proc = ffmpeg
            engine.browser_proc = browser
            ready = connectivity.ConnectivityResult(True, True, "ready")
            with (
                mock.patch.object(
                    engine,
                    "render_status_ready_probe",
                    return_value=(False, "browser map and ADS-B sample still warming up"),
                ),
                mock.patch.object(engine, "ingest_connectivity_probe", return_value=ready),
                mock.patch.object(engine, "ensure_browser_running", return_value=True) as restart_browser,
                mock.patch.object(engine, "append_event"),
                mock.patch.object(stream_engine.time, "monotonic", return_value=100.0),
            ):
                self.assertFalse(engine.recover_stale_render_heartbeat())
                self.assertFalse(engine.recover_stale_render_heartbeat())
                self.assertTrue(engine.recover_stale_render_heartbeat())

            restart_browser.assert_called_once_with(force=True, reason="render_heartbeat_stale")
            self.assertIs(engine.ffmpeg_proc, ffmpeg)
            ffmpeg.terminate.assert_not_called()

    def test_stale_render_does_not_restart_browser_while_connectivity_is_down(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            engine = self._engine(td, RENDER_SELF_RECOVERY_CONFIRMATIONS="1")
            down = connectivity.ConnectivityResult(False, False, "DNS unavailable")
            with (
                mock.patch.object(engine, "render_status_ready_probe", return_value=(False, "stale")),
                mock.patch.object(engine, "ingest_connectivity_probe", return_value=down),
                mock.patch.object(engine, "ensure_browser_running") as restart_browser,
                mock.patch.object(engine, "append_event"),
                mock.patch.object(stream_engine.time, "monotonic", return_value=100.0),
            ):
                self.assertFalse(engine.recover_stale_render_heartbeat())

            restart_browser.assert_not_called()


class FastRecoveryScopeTests(unittest.TestCase):
    def test_k8s_tcp_stall_targets_ffmpeg_child_not_deployment(self) -> None:
        with (
            mock.patch.object(fast_recovery, "k8s_supervisor_active", return_value=True),
            mock.patch.object(fast_recovery, "restart_ffmpeg_child", return_value=(True, "ok")) as child,
            mock.patch.object(fast_recovery, "restart_stream") as runtime,
        ):
            ok, detail, scope = fast_recovery.execute_recovery_action(
                reason_kind="tcp_stall",
                reason="confirmed transport stall",
                ffmpeg_pid=222,
            )

        self.assertTrue(ok)
        self.assertEqual(detail, "ok")
        self.assertEqual(scope, "ffmpeg_child")
        child.assert_called_once_with(222, "confirmed transport stall")
        runtime.assert_not_called()

    def test_network_down_is_recorded_without_any_restart_action(self) -> None:
        state: dict[str, object] = {}
        network = fast_recovery.recovery_decision.NetworkObservation(
            gateway="192.0.2.1",
            gateway_ok=False,
            public_ok_count=0,
            dns_ok=False,
            tcp_probe_ok=False,
            network_down=True,
        )
        with mock.patch.object(fast_recovery, "append_event") as append_event:
            fast_recovery.mark_connectivity_wait(
                state,
                now_ts=100,
                network=network,
                ffmpeg_pid=222,
            )
            fast_recovery.mark_connectivity_wait(
                state,
                now_ts=110,
                network=network,
                ffmpeg_pid=222,
            )

        self.assertTrue(state["connectivity_wait_active"])
        self.assertEqual(state["connectivity_wait_since_ts"], 100)
        append_event.assert_called_once()

    def test_child_executor_never_escalates_to_sigkill(self) -> None:
        sent: list[tuple[int, int]] = []
        ok, detail = executor.restart_ffmpeg_child(
            ffmpeg_pid=222,
            reason="test",
            log=lambda _message: None,
            send_signal=lambda pid, sig: sent.append((pid, sig)),
            process_exists=lambda _pid: True,
            wait_timeout_sec=0,
        )

        self.assertTrue(ok)
        self.assertIn("stream engine owns", detail)
        self.assertEqual(sent, [(222, executor.signal.SIGTERM)])


class ConnectivityIncidentCorrelationTests(unittest.TestCase):
    def test_root_network_incident_suppresses_only_derivative_current_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "fast_recovery_state.json"
            state_file.write_text(
                """{
                  "observed_ts": 1000,
                  "connectivity_wait_active": true,
                  "connectivity_wait_since_ts": 900,
                  "connectivity_gateway_present": true,
                  "connectivity_gateway_ok": false,
                  "connectivity_public_ok_count": 0,
                  "connectivity_dns_ok": false,
                  "connectivity_tcp_probe_ok": false
                }""",
                encoding="utf-8",
            )
            source = [
                {"id": "map:delivery_critical"},
                {"id": "viewer:synthetic_probe_failed"},
                {"id": "reliability:youtube_input_quality_fast_feedback"},
                {"id": "reliability:youtube_input_quality_multi_window_burn"},
                {"id": "external:blackbox_failed"},
            ]

            correlated = notification_incidents.correlate_connectivity_incidents(
                source,
                state_file=state_file,
                now_ts=1010,
            )

        ids = [item["id"] for item in correlated]
        self.assertEqual(ids[0], "network:delivery_connectivity_unavailable")
        self.assertNotIn("map:delivery_critical", ids)
        self.assertNotIn("viewer:synthetic_probe_failed", ids)
        self.assertNotIn("reliability:youtube_input_quality_fast_feedback", ids)
        self.assertIn("reliability:youtube_input_quality_multi_window_burn", ids)
        self.assertIn("external:blackbox_failed", ids)
        self.assertEqual(correlated[0]["repeat_sec"], 600)

    def test_stale_connectivity_state_does_not_suppress_current_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "fast_recovery_state.json"
            state_file.write_text(
                '{"observed_ts":100,"connectivity_wait_active":true}',
                encoding="utf-8",
            )
            source = [{"id": "map:delivery_critical"}]

            correlated = notification_incidents.correlate_connectivity_incidents(
                source,
                state_file=state_file,
                now_ts=1000,
            )

        self.assertEqual(correlated, source)

    def test_existing_derivative_is_deferred_instead_of_falsely_recovered(self) -> None:
        stored = {
            "first_seen_ts": 800,
            "last_bad_ts": 990,
            "last_incident": {"id": "map:delivery_critical"},
        }
        state: dict[str, object] = {}
        active = {"map:delivery_critical": stored}

        connectivity_correlation.reconcile_deferred_active(
            state=state,
            active_state=active,
            current_ids={connectivity_correlation.ROOT_INCIDENT_ID},
        )

        self.assertEqual(active, {})
        self.assertEqual(
            state[connectivity_correlation.DEFERRED_ACTIVE_STATE_KEY],
            {"map:delivery_critical": stored},
        )

    def test_deferred_derivative_is_restored_for_post_connectivity_evaluation(self) -> None:
        stored = {
            "first_seen_ts": 800,
            "last_bad_ts": 990,
            "last_incident": {"id": "viewer:visual_failure"},
        }
        state = {
            connectivity_correlation.DEFERRED_ACTIVE_STATE_KEY: {
                "viewer:visual_failure": stored,
            }
        }
        active: dict[str, object] = {}

        connectivity_correlation.reconcile_deferred_active(
            state=state,
            active_state=active,
            current_ids=set(),
        )

        self.assertEqual(active, {"viewer:visual_failure": stored})
        self.assertNotIn(connectivity_correlation.DEFERRED_ACTIVE_STATE_KEY, state)


class ConnectivityNotificationLifecycleTests(unittest.TestCase):
    def test_root_correlation_does_not_emit_false_derivative_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            saved_state = {
                "active": {
                    "map:delivery_critical": {
                        "first_seen_ts": 800,
                        "first_notified_ts": 800,
                        "last_bad_ts": 990,
                        "last_notified_ts": 990,
                        "last_incident": {
                            "id": "map:delivery_critical",
                            "summary": "map unavailable",
                        },
                    }
                }
            }
            current = [
                {
                    "id": connectivity_correlation.ROOT_INCIDENT_ID,
                    "severity": "critical",
                    "component": "local_delivery_connectivity",
                    "summary": "connectivity unavailable",
                    "evidence": "dns_ok=False rtmps_tcp_ok=False",
                    "recovery_type": "connectivity_restore_no_runtime_restart",
                    "follow_up": "check connectivity",
                    "observed_ts": 995,
                    "repeat_sec": 600,
                }
            ]
            rendered: list[tuple[str, list[str]]] = []
            recovered_probes: list[str] = []

            def load_state() -> dict:
                return copy.deepcopy(saved_state)

            def save_state(payload: dict) -> None:
                saved_state.clear()
                saved_state.update(copy.deepcopy(payload))

            def collect_incidents(**_kwargs) -> list[dict]:
                return copy.deepcopy(current)

            def recovery_observation(ident: str, now_ts: int) -> tuple[int, str]:
                recovered_probes.append(ident)
                return now_ts, "confirmed after connectivity reevaluation"

            def format_message(*, phase: str, incidents: list[dict], **_kwargs) -> str:
                rendered.append((phase, [str(item.get("id")) for item in incidents]))
                return json.dumps({"phase": phase})

            ctx = notification_status_loop.NotifyStatusContext(
                notify_events_file=root / "events.jsonl",
                notify_outbox_file=root / "outbox.jsonl",
                load_config=lambda: {
                    "enabled": False,
                    "webhook_url": "",
                    "username": "test",
                    "repeat_sec": 60,
                    "report_stale_sec": 1800,
                    "outbox_ttl_sec": 86400,
                    "outbox_max_pending": 50,
                    "outbox_flush_limit": 10,
                    "slack_enabled": False,
                    "slack_webhook_url": "",
                    "slack_username": "test",
                    "slack_min_active_sec": 1800,
                    "fast_recovery_event_recent_sec": 1800,
                    "fast_recovery_event_triggers": [],
                    "stream_engine_event_recent_sec": 1800,
                    "stream_watchdog_event_recent_sec": 1800,
                    "runtime_lifecycle_event_recent_sec": 86400,
                },
                load_state=load_state,
                save_state=save_state,
                collect_incidents=collect_incidents,
                recovery_observation_for_incident=recovery_observation,
                format_message=format_message,
                send_webhook=lambda *_args, **_kwargs: (True, "ok"),
                maintenance_notification_incident=lambda _now: None,
                fast_recovery_events_file=root / "fast.jsonl",
                stream_engine_events_file=root / "engine.jsonl",
                stream_watchdog_events_file=root / "watchdog.jsonl",
                runtime_state_base_dir=root,
            )

            self.assertEqual(notification_status_loop.notify_status(ctx=ctx, now_ts=1000), 0)
            self.assertEqual(
                rendered,
                [("detected", [connectivity_correlation.ROOT_INCIDENT_ID])],
            )
            self.assertEqual(recovered_probes, [])
            self.assertEqual(
                sorted(saved_state["active"]),
                [connectivity_correlation.ROOT_INCIDENT_ID],
            )
            self.assertIn(
                "map:delivery_critical",
                saved_state[connectivity_correlation.DEFERRED_ACTIVE_STATE_KEY],
            )

            rendered.clear()
            self.assertEqual(notification_status_loop.notify_status(ctx=ctx, now_ts=1010), 0)
            self.assertEqual(rendered, [])
            self.assertEqual(recovered_probes, [])

            current.clear()
            self.assertEqual(notification_status_loop.notify_status(ctx=ctx, now_ts=1020), 0)
            self.assertEqual(rendered[0][0], "recovered")
            self.assertEqual(
                set(rendered[0][1]),
                {
                    connectivity_correlation.ROOT_INCIDENT_ID,
                    "map:delivery_critical",
                },
            )
            self.assertEqual(
                set(recovered_probes),
                {
                    connectivity_correlation.ROOT_INCIDENT_ID,
                    "map:delivery_critical",
                },
            )
            self.assertEqual(saved_state["active"], {})
            self.assertNotIn(connectivity_correlation.DEFERRED_ACTIVE_STATE_KEY, saved_state)


if __name__ == "__main__":
    unittest.main()
