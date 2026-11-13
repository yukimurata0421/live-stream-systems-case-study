from __future__ import annotations

import copy
import configparser
import importlib.util
import io
import json
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops/host-maintenance/bin/nvidia_driver_smoke.py"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


smoke = load("nvidia_smoke_test", SCRIPT)
fixture = load("map_smoke_fixture", ROOT / "tests/test_stream_v3_map_runtime_probe.py")


class NvidiaDriverSmokeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "state"
        self.clock = {"wall": 2_000_000_000., "mono": 1000., "boot": "test-boot", "machine": "a" * 64}
        self.driver = "580.178.04"
        self.gpu_driver = self.driver
        self.package_driver = self.driver
        self.generation = "ffmpeg-generation"
        self.ack_extra = 0
        self.transport_overrides = {}
        self.report_stamp = None
        self.journal_error = False
        self.journal_overflow = False
        self.module_missing = False
        self.transport_missing = False
        self.transport_old = False
        self.bad_command = False
        self.moves_during_sample = False
        self.pod = {
            "metadata": {"name": "stream-v3-runtime-test", "uid": "pod-uid", "ownerReferences": [
                {"kind": "ReplicaSet", "uid": "rs-uid", "controller": True}]},
            "spec": {"nodeName": "test-node"},
            "status": {"phase": "Running", "containerStatuses": [
                {"name": name, "ready": True, "restartCount": 0, "state": {"running": {}},
                 "containerID": "containerd://" + name,
                 "imageID": "sha256:" + str(n) * 64}
                for n, name in enumerate(("stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop"))
            ]},
        }
        self.deployment = {"metadata": {"generation": 1, "uid": "deployment-uid"}, "spec": {"replicas": 1, "template": {"spec": {
            "containers": [{"name": r["name"], "image": "sha256:" + str(n) * 64}
                           for n, r in enumerate(self.pod["status"]["containerStatuses"])]}}},
            "status": {"observedGeneration": 1, "readyReplicas": 1, "availableReplicas": 1}}
        self.replicasets = [{"metadata": {"uid": "rs-uid", "ownerReferences": [
            {"kind": "Deployment", "uid": "deployment-uid", "controller": True}]},
            "spec": {"template": copy.deepcopy(self.deployment["spec"]["template"])}}]
        self.pods = None
        self.process = fixture.healthy_sample()["process"]
        self.process["browser"]["swiftshader_main_process_count"] = 0
        self.process["browser"]["nvidia_vulkan_main_process_count"] = 1
        self.readiness = {"schema": "stream_v3_runtime_readiness.v1", "ready": True, "gpu_ok": True, "nvenc_active": True,
                          "rtmp_socket_established": True, "ffmpeg_pid": 614,
                          "driver_version": self.driver, "boot_id": "test-boot", "pod_uid": "pod-uid"}
        self.probe = smoke.adapter("nvidia_smoke_map", smoke.SOURCES[1])
        self.recorder = smoke.adapter("nvidia_smoke_transport", smoke.SOURCES[2])
        self.commands = []
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(smoke, "clocks", side_effect=lambda: self.clock.copy()).start()
        mock.patch.object(self.probe, "command", side_effect=self.command).start()
        mock.patch.object(self.recorder, "bounded_read", side_effect=self.transport).start()
        original_read = Path.read_text

        def read(path, *args, **kwargs):
            if str(path) == "/sys/module/nvidia/version":
                if self.module_missing:
                    raise PermissionError("private diagnostic")
                return self.driver
            return original_read(path, *args, **kwargs)

        mock.patch.object(Path, "read_text", read).start()

    def advance(self, seconds=60, *, wall=None):
        self.clock["mono"] += seconds
        self.clock["wall"] += seconds if wall is None else wall

    def command(self, args, *, timeout_sec):
        self.commands.append(args)
        if self.bad_command:
            raise subprocess.TimeoutExpired(args, 1, output="secret fixture text")
        if "nvidia-smi" in args[0]:
            raw = "GPU-test, " + self.gpu_driver + "\n"
        elif "dpkg-query" in args[0]:
            raw = "".join(f"{name}\tii \t{self.package_driver}-0ubuntu0.test\n" for name in smoke.PACKAGES)
        elif "journalctl" in args[0]:
            rows = ([{"__CURSOR": "cursor-1", "MESSAGE": "kernel ready"}]
                    if "--after-cursor=cursor-1" not in args else [])
            if self.journal_error:
                rows = [{"__CURSOR": "cursor-2", "MESSAGE": "NVRM: Xid (PCI:example): 79"}]
            if self.journal_overflow:
                rows = [{"__CURSOR": "cursor-2", "MESSAGE": "line"}] * 1001
            raw = "\n".join(json.dumps(r) for r in rows)
        elif "get" in args:
            kind = args[args.index("get") + 1]
            if kind == "pods":
                obj = {"items": self.pods if self.pods is not None else [self.pod]}
            elif kind == "replicasets":
                obj = {"items": self.replicasets}
            elif kind == "pod":
                obj = copy.deepcopy(next(p for p in (self.pods if self.pods is not None else [self.pod])
                    if p['metadata']['name'] == args[args.index('get') + 2]))
                if self.moves_during_sample:
                    obj["status"]["containerStatuses"][0]["restartCount"] += 1
            else:
                obj = self.deployment
            raw = json.dumps(obj)
        elif "--check-only" in args:
            raw = json.dumps(self.readiness)
        else:
            self.assertIn("SMOKE_ONLY = True", args[-1])
            obj = copy.deepcopy(self.process)
            obj["render"]["payload"]["reported_at_ms"] = self.clock["wall"] * 1000 if self.report_stamp is None else self.report_stamp
            raw = json.dumps(obj)
        return subprocess.CompletedProcess(args, 1 if "--check-only" in args and not self.readiness["ready"] else 0, raw, "")

    def transport(self, path):
        if self.transport_missing:
            raise FileNotFoundError("private source")
        mono = self.clock["mono"] - 1000
        value = {
            "schema": "stream_v3.ffmpeg_transport.v1", "observed_at": smoke.jst(self.clock["wall"] - (31 if self.transport_old else 0)),
            "state": "ACK_ADVANCING", "fifo_recovering": False, "ffmpeg_generation": self.generation,
            "ffmpeg_pid": self.readiness['ffmpeg_pid'], "connection_id": "tcp-identity", "connection_generation": 1,
            "ack_bytes_observed_total": int(mono * 100_000) + 1 + self.ack_extra,
            "connections_observed_total": 1, "reconnections_observed_total": 0,
            "fifo_recovery_reports_total": 0, "fifo_recovery_failures_total": 0, "fifo_queue_overflows_total": 0,
            "tcp_metrics": {k: 0 for k in self.recorder.METRICS},
        }
        return json.dumps({**value, **self.transport_overrides}).encode()

    def cli(self, *args, code=0):
        output = io.StringIO()
        with redirect_stdout(output):
            actual = smoke.main(["--state-dir", str(self.directory), *args])
        self.assertEqual(actual, code, output.getvalue())
        result = json.loads(output.getvalue())
        self.assertNotIn("secret", output.getvalue())
        self.assertNotIn("private", output.getvalue())
        return result

    def start(self, name="campaign", *, code=0):
        return self.cli("start", "--run-id", name, "--expected-driver", self.driver, code=code)

    def rows(self, query):
        with closing(sqlite3.connect(self.directory / "smoke.sqlite3")) as db:
            return db.execute(query).fetchall()

    def replace_pod(self, label="replacement"):
        self.pod = copy.deepcopy(self.pod)
        self.pod['metadata'].update(name='stream-v3-runtime-' + label, uid='pod-' + label,
            ownerReferences=[{'kind':'ReplicaSet','uid':'rs-' + label,'controller':True}])
        for row in self.pod['status']['containerStatuses']:
            row['containerID'] = 'containerd://' + label + '-' + row['name']
            row['restartCount'] = 0
        self.readiness.update(pod_uid=self.pod['metadata']['uid'], ffmpeg_pid=self.readiness['ffmpeg_pid'] + 1)
        self.generation = 'ffmpeg-' + label
        self.deployment['metadata']['generation'] += 1
        self.deployment['status']['observedGeneration'] = self.deployment['metadata']['generation']
        self.replicasets.append({'metadata': {'uid':'rs-' + label, 'ownerReferences':[
            {'kind':'Deployment','uid':'deployment-uid','controller':True}]},
            'spec':{'template':copy.deepcopy(self.deployment['spec']['template'])}})
        self.pods = None

    def test_full_48_hours_real_cli_parsers_and_durable_database(self):
        self.assertEqual(self.start()["status"], "RUNNING")
        for n in range(1, 2881):
            if n in (600, 2000):
                self.pods = []
                self.transport_missing = True
            elif n in (601, 2001):
                self.transport_missing = False
                self.replace_pod('day-' + str(n))
            if n in (360, 1800):
                count = 1 if n == 360 else 2
                self.transport_overrides.update(state="SOCKET_UNAVAILABLE", fifo_recovering=True,
                    connection_generation=count + 1, reconnections_observed_total=count,
                    fifo_queue_overflows_total=count, fifo_recovery_failures_total=count)
                self.readiness.update(ready=False, rtmp_socket_established=False)
                for c in self.pod["status"]["containerStatuses"]: c["ready"] = False
                self.deployment["status"].update(readyReplicas=0, availableReplicas=0)
            elif n in (361, 1801):
                self.transport_overrides.update(state="ACK_ADVANCING", fifo_recovering=False)
                self.readiness.update(ready=True, rtmp_socket_established=True)
                for c in self.pod["status"]["containerStatuses"]: c["ready"] = True
                self.deployment["status"].update(readyReplicas=1, availableReplicas=1)
            self.advance()
            result = self.cli("tick")
            self.assertEqual(result["status"], "PASSED" if n == 2880 else "RUNNING")
        self.assertEqual(result["observed_elapsed_sec"], 172800)
        self.assertEqual(result["samples"], 2881)
        self.assertEqual(result["transport_observation"]["event_totals"]["reconnections_observed_total"], 2)
        self.assertEqual(result["transport_observation"]["recovered_episodes"], 2)
        self.assertEqual(result['runtime_observation']['segments'], 3)
        self.assertGreater(result['runtime_observation']['runtime_unverified_sec'], 0)
        self.assertFalse(result["provider_receiver_verified"])
        self.assertEqual(self.rows("SELECT count(*) FROM samples"), [(2881,)])
        self.assertEqual(self.rows("PRAGMA integrity_check"), [("ok",)])
        self.assertLess((self.directory / "smoke.sqlite3").stat().st_size, 32 * 1024**2)
        # A completed historical PASS remains bounded to its observation window.
        self.advance(10000)
        self.assertEqual(self.cli("tick")["status"], "IDLE")
        self.assertEqual(self.cli("status")["status"], "PASSED")
        self.assertTrue(all("get" in c or "exec" in c or any(x in c[0] for x in
                        ("nvidia-smi", "dpkg-query", "journalctl")) for c in self.commands))

    def test_40_hours_cannot_pass(self):
        self.start()
        # Preserve real 60-second accounting; never shorten DURATION in a test.
        for _ in range(40 * 60):
            self.advance()
            result = self.cli("tick")
        self.assertEqual(result["status"], "RUNNING")
        self.assertEqual(result["remaining_sec"], 8 * 3600)

    def test_exact_deadline_requires_a_final_good_sample(self):
        self.start()
        self.advance(48 * 3600)
        stale = self.cli("status", code=1)
        self.assertEqual(stale["verdict"], "INCONCLUSIVE")
        self.assertEqual(stale["observed_elapsed_sec"], 0)
        self.assertEqual(self.cli("tick", code=1)["status"], "INCONCLUSIVE")

    def test_gap_is_terminal_and_history_is_not_reset_by_new_run(self):
        self.start()
        self.advance(151)
        result = self.cli("tick", code=1)
        self.assertIn("OBSERVATION_GAP", result["reasons"])
        self.assertEqual(result["observed_elapsed_sec"], 0)
        self.start(code=2)
        self.start("successor")
        self.assertEqual(len(self.rows("SELECT id FROM runs")), 2)
        self.assertEqual(self.cli("status", "--run-id", "campaign", code=1)["status"], "INCONCLUSIVE")

    def test_boot_and_machine_change_stop_without_reading_new_runtime(self):
        for field in ("boot", "machine"):
            with self.subTest(field=field):
                self.directory = Path(self.temp.name) / field
                self.start()
                self.clock[field] += "-new"
                calls = len(self.commands)
                result = self.cli("tick", code=1)
                self.assertIn("HOST_OR_BOOT_CHANGED", result["reasons"])
                self.assertEqual(len(self.commands), calls)
                self.clock[field] = self.clock[field].removesuffix("-new")

    def test_wall_jump_and_backwards_boottime_are_unknown(self):
        self.start()
        self.advance(60, wall=3600)
        self.assertIn("WALL_CLOCK_JUMP", self.cli("tick", code=1)["reasons"])
        self.directory = Path(self.temp.name) / "backwards"
        self.start()
        self.advance(-1)
        self.assertIn("OBSERVATION_GAP", self.cli("tick", code=1)["reasons"])

    def test_repeated_or_concurrent_start_cannot_replace_active_run(self):
        self.start()
        self.start("other", code=2)
        self.start(code=2)
        with closing(sqlite3.connect(self.directory / "smoke.sqlite3")) as db:
            db.execute("BEGIN IMMEDIATE")
            self.cli("tick", code=2)
        self.assertEqual(self.cli("status")["samples"], 1)

    def test_stop_is_durable_and_requires_correct_run_id(self):
        self.start()
        self.cli("stop", "--run-id", "other", code=2)
        self.cli("stop", "--run-id", "campaign", code=1)
        self.assertEqual(self.cli("tick")["status"], "IDLE")
        self.assertEqual(self.cli("status", code=1)["status"], "STOPPED")
        self.start(code=2)

    def test_missing_bad_or_stale_observation_cannot_pass(self):
        for field in ("module_missing", "transport_missing", "transport_old", "bad_command", "journal_overflow"):
            with self.subTest(field=field):
                self.directory = Path(self.temp.name) / field
                self.start()
                setattr(self, field, True)
                self.advance()
                result = self.cli("tick", code=1)
                self.assertEqual(result["status"], "INCONCLUSIVE")
                self.assertEqual(result["observed_elapsed_sec"], 0)
                setattr(self, field, False)

    def test_observed_failures_are_durable_and_stop_sampling(self):
        for index, (change, reason) in enumerate((
            (lambda: setattr(self, "gpu_driver", "580.173.02"), "HOST_DRIVER_MISMATCH"),
            (lambda: setattr(self, "package_driver", "580.173.02"), "PACKAGE_DRIVER_MISMATCH"),
            (lambda: self.readiness.update(driver_version="580.173.02"), "CONTAINER_DRIVER_MISMATCH"),
            (lambda: setattr(self, "journal_error", True), "KERNEL_GPU_ERROR"),
            (lambda: self.process["browser"].update(nvidia_vulkan_main_process_count=0), "GPU_RENDER_OR_NVENC_RUNTIME_UNHEALTHY"),
            (lambda: self.readiness.update(nvenc_active=False), "GPU_RENDER_OR_NVENC_RUNTIME_UNHEALTHY"),
        )):
            with self.subTest(reason=reason):
                self.directory = Path(self.temp.name) / f"{index}-{reason}"
                self.gpu_driver = self.package_driver = self.driver
                self.journal_error = False
                self.readiness.update(driver_version=self.driver, nvenc_active=True)
                self.pod["status"]["containerStatuses"][0]["restartCount"] = 0
                self.process["browser"]["nvidia_vulkan_main_process_count"] = 1
                self.generation = "ffmpeg-generation"
                self.start()
                change()
                self.advance()
                result = self.cli("tick", code=1)
                self.assertEqual(result["status"], "FAILED")
                self.assertIn(reason, result["reasons"])
                self.assertEqual(self.cli("tick")["status"], "IDLE")

    def test_ack_pause_recovers_while_render_must_always_advance(self):
        self.start()
        self.advance()
        self.ack_extra = -6_000_000
        pending = self.cli("tick")
        self.assertEqual(pending["status"], "RUNNING")
        self.assertEqual(pending["transport_observation"]["state"], "RECOVERING")
        self.ack_extra = 0
        self.advance()
        self.assertEqual(self.cli("tick")["transport_observation"]["recovered_episodes"], 1)
        self.directory = Path(self.temp.name) / "render-stopped"
        self.start()
        self.report_stamp = self.clock["wall"] * 1000
        self.advance()
        self.assertIn("RENDER_NOT_ADVANCING", self.cli("tick", code=1)["reasons"])

    def test_connection_fifo_and_queue_events_are_recorded_without_stopping_driver_test(self):
        for field in ("connection_generation", "reconnections_observed_total", "fifo_recovery_reports_total",
                      "fifo_recovery_failures_total", "fifo_queue_overflows_total"):
            with self.subTest(field=field):
                self.directory = Path(self.temp.name) / field
                self.transport_overrides = {}
                self.start()
                self.transport_overrides[field] = 2
                self.advance()
                result = self.cli("tick")
                self.assertEqual(result["status"], "RUNNING")
                self.assertGreater(result["transport_observation"]["event_totals"][field], 0)

    def test_outage_deadline_survives_observer_process_reload_and_repeated_reconnects(self):
        self.start()
        self.transport_overrides.update(state="SOCKET_UNAVAILABLE", fifo_recovering=True)
        for n in range(1, 6):
            self.transport_overrides.update(connection_generation=n + 1, reconnections_observed_total=n)
            self.advance()
            # Reload the actual module: deadline must come from persisted evidence, not memory.
            with mock.patch.object(smoke, "transport_assessment",
                    side_effect=load("reloaded_transport_policy", SCRIPT).transport_assessment):
                result = self.cli("tick", code=1 if n == 5 else 0)
            self.assertEqual(result["status"], "FAILED" if n == 5 else "RUNNING")
            self.assertEqual(result["transport_observation"]["recovering_since_mono"], 1000)
        self.assertIn("TRANSPORT_RECOVERY_TIMEOUT", result["reasons"])

    def test_recovery_after_deadline_is_not_a_pass(self):
        self.start()
        self.transport_overrides.update(fifo_recovering=True)
        for _ in range(4):
            self.advance()
            self.cli("tick")
        self.transport_overrides.update(fifo_recovering=False)
        self.advance()
        self.assertIn("TRANSPORT_RECOVERY_TIMEOUT", self.cli("tick", code=1)["reasons"])

    def test_counter_regression_is_unknown_even_if_transport_looks_healthy(self):
        self.transport_overrides.update(connection_generation=2, reconnections_observed_total=1)
        self.start()
        self.transport_overrides.update(connection_generation=1, reconnections_observed_total=0)
        self.advance()
        result = self.cli("tick", code=1)
        self.assertEqual(result["status"], "INCONCLUSIVE")
        self.assertIn("TRANSPORT_COUNTER_REGRESSION", result["reasons"])

    def test_bad_transport_baseline_cannot_start_a_healthy_campaign(self):
        self.transport_overrides.update(fifo_recovering=True)
        self.assertIn("TRANSPORT_BASELINE_NOT_HEALTHY", self.start(code=1)["reasons"])

    def test_frozen_render_or_gpu_error_during_recovery_still_fails_immediately(self):
        for kind in ("render", "gpu"):
            self.directory = Path(self.temp.name) / kind
            self.report_stamp = None
            self.journal_error = False
            self.transport_overrides = {}
            self.start()
            self.transport_overrides.update(fifo_recovering=True)
            if kind == "render": self.report_stamp = self.clock["wall"] * 1000
            else: self.journal_error = True
            self.advance()
            self.assertEqual(self.cli("tick", code=1)["status"], "FAILED")

    def test_pending_transport_at_48h_cannot_pass_or_extend_the_window(self):
        self.start()
        # Approach the fixed deadline using valid intervals and real durable state.
        for _ in range(172800 // 150 - 1):
            self.advance(150)
            self.cli("tick")
        self.advance(90)
        self.cli("tick")
        self.transport_overrides.update(fifo_recovering=True)
        self.advance()
        result = self.cli("tick", code=1)
        self.assertEqual(result["status"], "INCONCLUSIVE")
        self.assertIn("TRANSPORT_UNRESOLVED_AT_DEADLINE", result["reasons"])
        self.assertEqual(self.cli("tick")["status"], "IDLE")

    def test_database_writer_failure_rolls_back_the_entire_sample(self):
        self.start()
        with closing(sqlite3.connect(self.directory / "smoke.sqlite3")) as db:
            db.execute("CREATE TRIGGER refuse_sample BEFORE INSERT ON samples BEGIN SELECT RAISE(ABORT,'fixture'); END")
            db.commit()
        self.advance()
        self.cli("tick", code=2)
        self.assertEqual(self.cli("status")["samples"], 1)
        self.assertEqual(self.rows("SELECT count(*) FROM samples"), [(1,)])
        with closing(sqlite3.connect(self.directory / "smoke.sqlite3")) as db:
            db.execute("DROP TRIGGER refuse_sample")
            db.commit()
        self.advance(91)
        self.assertEqual(self.cli("tick", code=1)["status"], "INCONCLUSIVE")

    def test_status_is_readonly_and_historical_restarts_are_not_new_failures(self):
        self.pod["status"]["containerStatuses"][0]["restartCount"] = 2
        self.start()
        path = self.directory / "smoke.sqlite3"
        before = path.read_bytes()
        self.cli("status")
        self.assertEqual(path.read_bytes(), before)
        self.advance()
        self.assertEqual(self.cli("tick")["status"], "RUNNING")

    def test_candidate_must_actually_be_loaded(self):
        self.cli("check", "--expected-driver", "580.173.02", code=1)
        self.assertFalse(self.directory.exists())
        self.assertTrue(all("install" not in c and "restart" not in c for c in self.commands))

    def test_changed_source_cannot_continue(self):
        self.start()
        self.advance()
        with mock.patch.object(smoke, "source_digest", return_value="b" * 64):
            self.assertIn("SOURCE_CHANGED", self.cli("tick", code=1)["reasons"])

    def test_owned_pod_and_image_update_needs_two_progress_samples(self):
        first = self.start()
        self.replace_pod()
        self.pod['status']['containerStatuses'][0]['imageID'] = 'sha256:' + 'f' * 64
        self.advance()
        result = self.cli('tick')
        self.assertEqual(result['runtime_observation']['state'], 'VERIFYING')
        self.assertEqual(result['runtime_observation']['segments'], 1)
        self.advance()
        result = self.cli('tick')
        self.assertEqual(result['runtime_observation']['state'], 'STABLE')
        self.assertEqual(result['runtime_observation']['segments'], 2)
        self.assertEqual(result['started_at_jst'], first['started_at_jst'])
        self.assertEqual(result['current_target']['runtime']['pod_uid'], 'pod-replacement')
        self.assertEqual(result['target']['runtime']['pod_uid'], 'pod-uid')

    def test_scaled_down_and_startup_remain_observed_without_refunding_deadline(self):
        self.start()
        self.pods = []
        self.transport_old = True
        self.advance()
        result = self.cli('tick')
        self.assertEqual(result['runtime_observation']['state'], 'TRANSITION')
        self.transport_old = False
        self.replace_pod()
        self.readiness.update(ready=False, nvenc_active=False, ffmpeg_pid=0)
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'], 'TRANSITION')
        self.readiness.update(ready=True, nvenc_active=True, ffmpeg_pid=700)
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'], 'VERIFYING')
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'], 'STABLE')

    def test_changed_container_and_encoder_generation_are_qualified(self):
        self.start()
        self.pod['status']['containerStatuses'][0].update(restartCount=1, containerID='containerd://restarted')
        self.generation = 'restarted'
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'], 'VERIFYING')
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['segments'], 2)

    def test_empty_or_flapping_rollout_expires_from_original_good_sample(self):
        self.start()
        for n in range(1,6):
            if n % 2: self.pods = []
            else: self.replace_pod(str(n))
            self.advance()
            result = self.cli('tick', code=1 if n==5 else 0)
            self.assertEqual(result['runtime_observation']['pending_since_mono'],1000)
        self.assertIn('RUNTIME_TRANSITION_TIMEOUT',result['reasons'])

    def test_healthy_old_pod_is_observed_during_overlap_then_new_pod_is_qualified(self):
        self.start()
        old = copy.deepcopy(self.pod)
        self.replace_pod()
        self.pods = [old, self.pod]
        # Both replicas have the same fixture template: ambiguity is bounded,
        # never an arbitrary Pod selection.
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'TRANSITION')
        self.pods = [self.pod]
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'VERIFYING')
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'STABLE')

    def test_unowned_pod_foreign_node_and_recreated_deployment_are_rejected(self):
        for change in ('owner','node','deployment'):
            self.directory = Path(self.temp.name)/change
            original_pod, original_deployment = copy.deepcopy(self.pod),copy.deepcopy(self.deployment)
            self.start()
            if change=='owner':self.pod['metadata']['ownerReferences'][0]['uid']='foreign-rs'
            if change=='node':self.pod['spec']['nodeName']='foreign-node'
            if change=='deployment':self.deployment['metadata']['uid']='recreated-deployment'
            self.advance()
            self.assertEqual(self.cli('tick',code=1)['status'],'INCONCLUSIVE')
            self.pod,self.deployment = original_pod,original_deployment

    def test_gpu_error_during_pod_absence_is_still_terminal(self):
        self.start()
        self.pods=[]
        self.journal_error=True
        self.advance()
        self.assertIn('KERNEL_GPU_ERROR',self.cli('tick',code=1)['reasons'])

    def test_old_ready_pod_is_used_until_desired_replacement_is_running(self):
        self.start()
        old=copy.deepcopy(self.pod)
        old_readiness=copy.deepcopy(self.readiness)
        old_generation=self.generation
        self.replace_pod()
        new_readiness=copy.deepcopy(self.readiness)
        new_generation=self.generation
        self.deployment['spec']['template']['spec']['containers'][0]['image']='new-image'
        self.replicasets[-1]['spec']['template']=copy.deepcopy(self.deployment['spec']['template'])
        self.pod['status']['phase']='Pending'
        for c in self.pod['status']['containerStatuses']:c['ready']=False
        self.pods=[old,self.pod]
        self.readiness,self.generation=old_readiness,old_generation
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'STABLE')
        self.pod['status']['phase']='Running'
        for c in self.pod['status']['containerStatuses']:c['ready']=True
        self.readiness,self.generation=new_readiness,new_generation
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'VERIFYING')
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['segments'],2)

    def test_network_outage_cannot_extend_deadline_by_starting_a_pod_transition(self):
        self.start()
        self.transport_overrides.update(fifo_recovering=True)
        self.advance()
        self.cli('tick')
        self.pods=[]
        for n in range(2,6):
            self.advance()
            result=self.cli('tick',code=1 if n==5 else 0)
        self.assertIn('RUNTIME_TRANSITION_TIMEOUT',result['reasons'])

    def test_frozen_new_runtime_cannot_be_adopted(self):
        self.start()
        self.replace_pod()
        self.advance()
        self.cli('tick')
        self.report_stamp=self.clock['wall']*1000
        for n in range(2,6):
            self.advance()
            result=self.cli('tick',code=1 if n==5 else 0)
        self.assertIn('RUNTIME_TRANSITION_TIMEOUT',result['reasons'])
        self.assertEqual(result['runtime_observation']['segments'],1)

    def test_pending_pod_at_exact_48h_is_inconclusive_without_extension(self):
        self.start()
        for _ in range(172800//150-1):
            self.advance(150)
            self.cli('tick')
        self.advance(90)
        self.cli('tick')
        self.pods=[]
        self.advance()
        result=self.cli('tick',code=1)
        self.assertIn('RUNTIME_UNRESOLVED_AT_DEADLINE',result['reasons'])
        self.assertEqual(result['status'],'INCONCLUSIVE')
        self.assertEqual(self.cli('tick')['status'],'IDLE')

    def test_pod_change_mid_collection_is_pending_not_a_fabricated_valid_sample(self):
        self.start()
        self.moves_during_sample=True
        self.advance()
        result=self.cli('tick')
        self.assertEqual(result['runtime_observation']['state'],'TRANSITION')
        self.moves_during_sample=False
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'VERIFYING')
        self.advance()
        self.assertEqual(self.cli('tick')['runtime_observation']['state'],'STABLE')

    def test_observer_upgrade_preserves_run_start_history_and_rejects_replay(self):
        self.start()
        with closing(sqlite3.connect(self.directory/'smoke.sqlite3')) as db:
            value=json.loads(db.execute('SELECT last FROM runs').fetchone()[0])
            value['policy']='driver_with_transport_recovery_v2'
            value.pop('runtime_observation')
            value['source']='a'*64
            packed=smoke.packed(value)
            db.execute('UPDATE runs SET last=?',(packed,))
            db.execute('UPDATE samples SET observation=?',(packed,))
            db.commit()
        prefix=self.rows('SELECT * FROM samples')
        start=self.rows('SELECT started,mono,observed FROM runs')[0]
        self.advance()
        digest=smoke.source_digest()
        args=('upgrade-observer','--run-id','campaign','--from-source','a'*64,'--to-source',digest)
        wrong=(*args[:-1],'b'*64)
        self.cli(*wrong,code=2)
        self.assertEqual(self.rows('SELECT * FROM samples WHERE seq=1'),prefix)
        self.assertEqual(self.rows('SELECT started,mono,observed FROM runs')[0],start)
        result=self.cli(*args)
        self.assertEqual(result['policy'],smoke.POLICY)
        self.assertEqual(result['observed_elapsed_sec'],60)
        self.assertEqual(self.rows('SELECT started,mono FROM runs')[0],start[:2])
        self.assertEqual(self.rows('SELECT * FROM samples WHERE seq=1'),prefix)
        self.assertEqual(len(result['source_lineage']),1)
        self.cli(*args,code=2)
        self.advance()
        self.assertEqual(self.cli('tick')['samples'],3)

    def test_idle_tick_does_not_create_state_or_query_runtime(self):
        self.assertEqual(self.cli("tick"), {"status": "IDLE"})
        self.assertFalse(self.directory.exists())
        self.assertEqual(self.commands, [])

    def test_readonly_precheck_does_not_start_or_create_state(self):
        result = self.cli("check", "--expected-driver", self.driver)
        self.assertEqual(result["status"], "PRECHECK_PASSED")
        self.assertFalse(result["campaign_started"])
        self.assertFalse(self.directory.exists())

    def test_old_active_timer_boundary_skips_but_completion_timer_collects(self):
        self.start()
        self.advance(59.8)
        self.assertEqual(self.cli('tick')['samples'], 1)
        self.advance(60)
        self.assertEqual(self.cli('tick')['samples'], 2)
        self.assertAlmostEqual(self.cli('status')['observed_elapsed_sec'], 119.8)
        self.directory = Path(self.temp.name) / 'completion-timer'
        config = configparser.ConfigParser()
        config.read(ROOT / 'ops/host-maintenance/systemd/stream-v3-nvidia-smoke.timer')
        period = float(config['Timer']['OnUnitInactiveSec'])
        self.assertNotIn('OnUnitActiveSec', config['Timer'])
        original = self.command

        def slow(args, *, timeout_sec):
            if 'get' in args and args[args.index('get') + 1] == 'pod':
                self.advance(20)
            return original(args, timeout_sec=timeout_sec)

        with mock.patch.object(self.probe, 'command', side_effect=slow):
            self.start()
            for n in range(3):
                self.advance(period)
                result = self.cli('tick')
                self.assertEqual(result['samples'], n + 2)
                self.assertEqual(result['status'], 'RUNNING')
        self.assertEqual(result['observed_elapsed_sec'], 240)

    def test_real_parallel_process_writer_excludes_writers_but_allows_status(self):
        self.start()
        entered = Path(self.temp.name) / 'entered'
        release = Path(self.temp.name) / 'release'
        code = r'''
import importlib.util, pathlib, resource, sys, time
resource.setrlimit(resource.RLIMIT_CPU, (10, 10))
resource.setrlimit(resource.RLIMIT_AS, (512*1024**2, 512*1024**2))
spec=importlib.util.spec_from_file_location('smoke_child_fixture', sys.argv[1])
module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
case=module.NvidiaDriverSmokeTests(); case.setUp()
try:
    case.directory=pathlib.Path(sys.argv[2]); case.advance(60)
    entered, release=pathlib.Path(sys.argv[3]), pathlib.Path(sys.argv[4])
    original=case.command
    def blocked(args, *, timeout_sec):
        if 'nvidia-smi' in args[0]:
            entered.touch()
            deadline=time.monotonic()+10
            while not release.exists():
                if time.monotonic()>deadline: raise TimeoutError('fixture release missing')
                time.sleep(.01)
        return original(args, timeout_sec=timeout_sec)
    with module.mock.patch.object(case.probe,'command',side_effect=blocked):
        result=case.cli('tick')
        assert result['samples']==2 and result['status']=='RUNNING'
finally:
    case.doCleanups()
'''
        child = subprocess.Popen(['/usr/bin/python3', '-I', '-B', '-c', code, str(Path(__file__)),
                                  str(self.directory), str(entered), str(release)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                 env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while not entered.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(entered.exists())
            self.assertEqual(self.cli('status')['samples'], 1)
            self.cli('tick', code=2)
            self.cli('stop', '--run-id', 'campaign', code=2)
            self.start('competing', code=2)
            release.touch()
            stdout, stderr = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0, stdout + stderr)
            self.advance()
            self.assertEqual(self.cli('status')['samples'], 2)
            self.assertEqual(self.rows('SELECT count(*) FROM runs'), [(1,)])
            self.assertEqual(self.rows('SELECT count(*) FROM samples'), [(2,)])
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=10)

    def test_slow_collection_uses_read_time_and_does_not_credit_baseline_work(self):
        original = self.command

        def slow(args, *, timeout_sec):
            if "get" in args and args[args.index("get") + 1] == "pod":
                self.advance(20)
            return original(args, timeout_sec=timeout_sec)

        with mock.patch.object(self.probe, "command", side_effect=slow):
            result = self.start()
            self.assertEqual(result["status"], "RUNNING")
            self.assertEqual(result["observed_elapsed_sec"], 0)
            self.assertEqual(self.clock["mono"], 1020)
            # An explicit tick at 60 seconds from collection start is still supported.
            self.advance(40)
            result = self.cli("tick")
        self.assertEqual(result["samples"], 2)
        self.assertEqual(result["observed_elapsed_sec"], 60)

    def test_database_corruption_and_foreign_state_are_preserved(self):
        self.directory.mkdir()
        path = self.directory / "smoke.sqlite3"
        for value in (b"broken preserved evidence",):
            path.write_bytes(value)
            self.start(code=2)
            self.assertEqual(path.read_bytes(), value)
        path.unlink()
        with closing(sqlite3.connect(path)) as db:
            db.execute("CREATE TABLE foreign_state (value)")
            db.commit()
        self.start(code=2)
        self.assertEqual(self.rows("SELECT name FROM sqlite_master"), [("foreign_state",)])

    def test_installed_layout_cold_import_does_not_need_checkout(self):
        bundle = Path(self.temp.name) / "bundle"
        for path in smoke.SOURCES:
            target = bundle / path.relative_to(ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
        cp = subprocess.run(["/usr/bin/python3", "-I", "-B", str(bundle / SCRIPT.relative_to(ROOT)),
                             "--state-dir", str(bundle / "state"), "tick"],
                            text=True, capture_output=True, cwd="/", timeout=10)
        self.assertEqual(cp.returncode, 0, cp.stderr)
        self.assertEqual(json.loads(cp.stdout)["status"], "IDLE")
        code = ("import runpy,sys; m=runpy.run_path(sys.argv[1]); "
                "m['source_digest'](); m['adapter']('cold_map',m['SOURCES'][1]); "
                "m['adapter']('cold_transport',m['SOURCES'][2]); print('READY')")
        cp = subprocess.run(["/usr/bin/python3", "-I", "-B", "-c", code, str(bundle / SCRIPT.relative_to(ROOT))],
                            text=True, capture_output=True, cwd="/", timeout=10)
        self.assertEqual(cp.returncode, 0, cp.stderr)
        self.assertEqual(cp.stdout.strip(), "READY")

    def test_gpu_only_probe_executes_without_weather_io(self):
        def endpoint(url, **kwargs):
            self.assertEqual(url, "http://127.0.0.1:18080/render/status.json")
            return io.BytesIO(b'{"ready":true}')

        output = io.StringIO()
        with mock.patch.object(Path, "iterdir", return_value=[]), \
                mock.patch("urllib.request.urlopen", side_effect=endpoint) as http, redirect_stdout(output):
            exec(compile("SMOKE_ONLY = True\n" + self.probe.PROCESS_PROBE, "gpu-only-probe", "exec"), {})
        self.assertEqual(http.call_count, 1)
        self.assertEqual(json.loads(output.getvalue())["weather_integrity"], {})

    def test_kernel_baseline_does_not_reclassify_historical_xid(self):
        self.journal_error = True
        self.assertEqual(self.start()["status"], "RUNNING")


if __name__ == "__main__":
    unittest.main()
