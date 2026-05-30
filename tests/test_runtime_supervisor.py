from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.supervisor import K8sSupervisor, SystemdSupervisor, build_runtime_supervisor
from stream_core.commands import service as service_command
from watchers.fast_recovery_core import executor as fast_recovery_executor
from watchers import stream_watchdog
from watchers.youtube_lifecycle import actions as youtube_lifecycle_actions


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


class RuntimeSupervisorTests(unittest.TestCase):
    def test_systemd_restart_requires_active_after_command(self) -> None:
        calls: list[list[str]] = []

        def run_systemctl(args: list[str], _check: bool) -> subprocess.CompletedProcess[str]:
            calls.append(args)
            if args[0] == "is-active":
                return cp(0, stdout="active\n")
            return cp(0)

        supervisor = SystemdSupervisor(run_systemctl=run_systemctl)
        result = supervisor.restart("adsb-streamnew-youtube-stream.service", reason="test")

        self.assertTrue(result.ok)
        self.assertEqual(result.command, ("systemctl", "restart", "adsb-streamnew-youtube-stream.service"))
        self.assertEqual(calls, [["restart", "adsb-streamnew-youtube-stream.service"], ["is-active", "adsb-streamnew-youtube-stream.service"]])

    def test_systemd_restart_fails_when_target_not_active_after_restart(self) -> None:
        def run_systemctl(args: list[str], _check: bool) -> subprocess.CompletedProcess[str]:
            if args[0] == "is-active":
                return cp(3, stdout="inactive\n")
            return cp(0)

        result = SystemdSupervisor(run_systemctl=run_systemctl).restart("dummy.service")

        self.assertFalse(result.ok)
        self.assertIn("not active", result.detail)

    def test_k8s_dry_run_restart_builds_rollout_restart(self) -> None:
        calls: list[list[str]] = []
        supervisor = K8sSupervisor(dry_run=True, run_command=lambda command: calls.append(command) or cp(0))

        result = supervisor.restart("deployment/stream-v3-runtime", reason="shadow-test")

        self.assertTrue(result.ok)
        self.assertTrue(result.dry_run)
        self.assertEqual(calls, [])
        self.assertEqual(
            result.command,
            ("kubectl", "-n", "stream-v3", "rollout", "restart", "deployment/stream-v3-runtime"),
        )
        self.assertEqual(result.detail, "reason=shadow-test")

    def test_k8s_status_parses_ready_deployment(self) -> None:
        payload = {
            "kind": "Deployment",
            "spec": {"replicas": 1},
            "status": {"readyReplicas": 1, "availableReplicas": 1},
        }

        supervisor = K8sSupervisor(run_command=lambda _command: cp(0, stdout=json.dumps(payload)))
        status = supervisor.status("deployment/stream-v3-runtime")

        self.assertTrue(status.active)
        self.assertIn("available=1", status.detail)

    def test_k8s_start_stop_scale_replicas(self) -> None:
        commands: list[list[str]] = []
        supervisor = K8sSupervisor(run_command=lambda command: commands.append(command) or cp(0))

        self.assertTrue(supervisor.start("deployment/stream-v3-runtime").ok)
        self.assertTrue(supervisor.stop("deployment/stream-v3-runtime").ok)

        self.assertEqual(commands[0], ["kubectl", "-n", "stream-v3", "scale", "deployment/stream-v3-runtime", "--replicas=1"])
        self.assertEqual(commands[1], ["kubectl", "-n", "stream-v3", "scale", "deployment/stream-v3-runtime", "--replicas=0"])

    def test_k8s_start_once_requires_cronjob_target(self) -> None:
        result = K8sSupervisor(dry_run=True).start_once("deployment/stream-v3-control")

        self.assertFalse(result.ok)
        self.assertIn("cronjob", result.detail)

    def test_factory_builds_k8s_dry_run_with_stream_v3_target_map(self) -> None:
        supervisor = build_runtime_supervisor(
            env={"STREAM_RUNTIME_SUPERVISOR": "k8s"},
            run_systemctl=lambda _args, _check: cp(0),
        )

        result = supervisor.restart("adsb-streamnew-youtube-stream.service", reason="test")

        self.assertTrue(result.ok)
        self.assertTrue(result.dry_run)
        self.assertEqual(result.command, ("kubectl", "-n", "stream-v3", "rollout", "restart", "deployment/stream-v3-runtime"))
        self.assertIn("mapped_target=deployment/stream-v3-runtime", result.detail)

    def test_factory_maps_report_timer_to_k8s_cronjob(self) -> None:
        supervisor = build_runtime_supervisor(
            env={"STREAM_RUNTIME_SUPERVISOR": "k8s"},
            run_systemctl=lambda _args, _check: cp(0),
        )

        result = supervisor.start_once("adsb-streamnew-youtube-api-cost-open-day-report.service", reason="manual report")

        self.assertTrue(result.ok)
        self.assertTrue(result.dry_run)
        self.assertEqual(result.command[:5], ("kubectl", "-n", "stream-v3", "create", "job"))
        self.assertIn("--from=cronjob/stream-v3-youtube-api-cost-open-day", result.command)

    def test_factory_maps_network_observer_to_runtime_workload(self) -> None:
        supervisor = build_runtime_supervisor(
            env={"STREAM_RUNTIME_SUPERVISOR": "k8s"},
            run_systemctl=lambda _args, _check: cp(0),
        )

        result = supervisor.restart("adsb-streamnew-network-observer.service", reason="observer refresh")

        self.assertTrue(result.ok)
        self.assertEqual(result.command, ("kubectl", "-n", "stream-v3", "rollout", "restart", "deployment/stream-v3-runtime"))

    def test_fast_recovery_executor_can_use_runtime_supervisor(self) -> None:
        supervisor = K8sSupervisor(dry_run=True, target_map={"stream.service": "deployment/stream-v3-runtime"})
        logs: list[str] = []

        ok, detail = fast_recovery_executor.restart_stream(
            stream_service="stream.service",
            reason="tcp stall",
            run_systemctl=lambda *_args, **_kwargs: cp(99, stderr="should not run"),
            log=logs.append,
            supervisor=supervisor,
        )

        self.assertTrue(ok)
        self.assertEqual(detail, "restart ok")

    def test_stream_watchdog_uses_k8s_supervisor_when_requested(self) -> None:
        with mock.patch.dict(os.environ, {"STREAM_RUNTIME_SUPERVISOR": "k8s", "STREAM_V3_CUTOVER_ENABLE": "0"}, clear=False):
            supervisor = stream_watchdog.runtime_supervisor_or_none()

        self.assertIsInstance(supervisor, K8sSupervisor)
        result = supervisor.restart("adsb-streamnew-youtube-stream.service", reason="watchdog-test")
        self.assertTrue(result.ok)
        self.assertTrue(result.dry_run)
        self.assertIn("deployment/stream-v3-runtime", result.command)

    def test_youtube_lifecycle_restart_can_use_runtime_supervisor_failure_detail(self) -> None:
        supervisor = K8sSupervisor(dry_run=False, run_command=lambda _cmd: cp(1, stderr="api refused"))
        writes: list[dict[str, str]] = []
        logs: list[str] = []

        ok, detail = youtube_lifecycle_actions.restart_stream(
            reason="remote ended",
            stream_service="deployment/stream-v3-runtime",
            write_restart_reason=lambda **kwargs: writes.append(kwargs),
            run_systemctl=lambda *_args, **_kwargs: cp(99, stderr="should not run"),
            log=logs.append,
            supervisor=supervisor,
        )

        self.assertFalse(ok)
        self.assertIn("api refused", detail)
        self.assertEqual(writes[0]["reason"], "remote ended")

    def test_service_start_can_use_k8s_workloads_in_dry_run(self) -> None:
        ctx = SimpleNamespace(
            supervisor_mode="k8s",
            runtime_supervisor=K8sSupervisor(dry_run=True),
            k8s_workloads=("deployment/stream-v3-runtime", "deployment/stream-v3-control"),
            guard_start_safety=lambda: 0,
        )

        self.assertEqual(service_command.start(ctx), 0)

    def test_service_restart_stops_on_k8s_supervisor_failure(self) -> None:
        ctx = SimpleNamespace(
            supervisor_mode="k8s",
            runtime_supervisor=K8sSupervisor(dry_run=False, run_command=lambda _cmd: cp(1, stderr="api refused")),
            k8s_workloads=("deployment/stream-v3-runtime",),
            guard_start_safety=lambda: 0,
        )

        self.assertEqual(service_command.restart(ctx), 1)


if __name__ == "__main__":
    unittest.main()
