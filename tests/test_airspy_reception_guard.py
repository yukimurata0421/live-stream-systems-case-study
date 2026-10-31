"""Reception and bounded-restart tests using real file ledgers and CLI processes."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SOURCE = Path(os.environ.get("AIRSPY_RX_TEST_SOURCE", str(Path(__file__).resolve().parents[1] / "ops" / "airspy-reception")))
sys.path.insert(0, str(SOURCE))
import receiver_guard as guard


def sample(mono, instance="initial", start=10, rate=100, positions=10, counter=None, **changes):
    value = dict(now=1_800_000_000 + mono, mono=mono, boot="boot", instance=instance,
                 pid=100 if instance == "initial" else 200, start_mono=start,
                 start_epoch=1_800_000_000 + start, source_now=1_800_000_000 + mono,
                 readsb_now=1_800_000_000 + mono, rate=rate, df17=20, gain=16,
                 lost_buffers=0, positions=counter if counter is not None else mono * 10,
                 readsb_start=10, fresh_positions=positions, config_sha="config",
                 binary_sha="binary", command_sha="command")
    value.update(changes)
    return value


class Backend:
    def __init__(self, value):
        self.value = value
        self.calls = 0
        self.result = 0
        self.exception = None
        self.before_effect = None

    def collect(self, policy):
        if isinstance(self.value, Exception):
            raise self.value
        return dict(self.value)

    def restart(self):
        self.calls += 1
        if self.before_effect:
            self.before_effect()
        if self.exception:
            raise self.exception
        return self.result


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = guard.Store(self.root)
        self.store.initialize()
        self.policy = guard.Policy()
        guard.POLICY_CONTEXT = self.policy
        self.backend = Backend(sample(100))

    def tearDown(self):
        self.tmp.cleanup()

    def tick(self, value, allow=True):
        self.backend.value = value
        with self.store.locked():
            return guard.observe(self.store, self.store.load(), self.backend, self.policy, allow)

    def baseline(self):
        for t in (100, 160, 220):
            self.tick(sample(t))
        self.assertTrue(guard.read_json(self.store.status_path)["reception_confirmed"])

    def failure_until(self, finish=570, **changes):
        counter = 2200
        result = None
        for t in range(270, finish + 1, 60):
            result = self.tick(sample(t, "failed", 250, 2, 0, counter, **changes))
        return result

    def test_env_ratio_is_configurable_and_invalid_values_fail(self):
        with patch.dict(os.environ, {"AIRSPY_RX_RATE_RATIO": "0.07"}, clear=True):
            self.assertEqual(guard.Policy.from_env().rate_ratio, .07)
        for raw in ("nan", "inf", "0", "1", "-0.1", "garbage"):
            with self.subTest(raw=raw), patch.dict(os.environ, {"AIRSPY_RX_RATE_RATIO": raw}, clear=True):
                with self.assertRaises(ValueError):
                    guard.Policy.from_env()

    def test_healthy_start_requires_two_distinct_snapshots_and_progress(self):
        self.assertFalse(self.tick(sample(100))["reception_confirmed"])
        self.assertFalse(self.tick(sample(120, source_now=1_800_000_100))["reception_confirmed"])
        self.assertTrue(self.tick(sample(160))["reception_confirmed"])
        self.assertEqual(self.backend.calls, 0)

    def test_new_start_without_pretraffic_never_restarts(self):
        for t in range(270, 1500, 60):
            self.tick(sample(t, "quiet", 250, 0, 0, 0))
        self.assertEqual(self.backend.calls, 0)
        self.assertEqual(guard.read_json(self.store.status_path)["state"], "QUIET_OR_UNKNOWN")

    def test_sustained_collapse_reserves_one_attempt_before_effect(self):
        self.baseline()
        seen = []
        self.backend.before_effect = lambda: seen.append(self.store.load()["episode"]["attempts"])
        result = self.failure_until()
        self.assertEqual(result["state"], "VERIFYING")
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual(seen, [1])
        events=[json.loads(line) for line in (self.root/'events.jsonl').read_text().splitlines()]
        reservation=next(e for e in events if e['event']=='recovery_restart_reserved')
        self.assertGreaterEqual(len(reservation['evidence']['bad_window']),3)
        self.assertTrue(any(e['event']=='recovery_restart_result' and e['returncode']==0 for e in events))

    def test_recovery_child_and_repeated_cycles_do_not_rearm(self):
        self.baseline()
        self.failure_until()
        original = self.store.load()["episode"]["id"]
        for t in range(630, 1600, 60):
            result = self.tick(sample(t, "recovery-child", 580, 2, 0, 2200))
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual(self.store.load()["episode"]["id"], original)
        self.assertEqual(result["recovery_attempts"], 1)
        self.assertEqual(result["state"], "EXHAUSTED")

    def test_second_child_without_verified_recovery_does_not_rearm(self):
        self.baseline()
        self.failure_until()
        for t in range(630, 1200, 60):
            self.tick(sample(t, "child-a", 580, 2, 0, 2200))
        for t in range(1230, 1800, 60):
            self.tick(sample(t, "child-b", 1200, 2, 0, 2200))
        self.assertEqual(self.backend.calls, 1)
        self.assertEqual(self.store.load()["episode"]["attempts"], 1)

    def test_observed_positions_verify_recovery_then_new_external_start_gets_own_budget(self):
        self.baseline()
        self.failure_until()
        old = self.store.load()["episode"]["id"]
        self.tick(sample(630, "retry", 580, counter=2300))
        healthy = self.tick(sample(690, "retry", 580, counter=2400))
        self.assertTrue(healthy["reception_confirmed"])
        for t in (750, 810, 870, 930, 990):
            self.tick(sample(t, "retry", 580))
        self.tick(sample(1050, "external-next", 1040))
        current = self.store.load()["episode"]
        self.assertNotEqual(current["id"], old)
        self.assertEqual(current["attempts"], 0)
        self.assertIsNotNone(current["baseline"])
        self.assertEqual(self.store.load()["closed"][-1]["attempts"], 1)

    def test_nonzero_restart_return_does_not_refund(self):
        self.baseline()
        self.backend.result = 1
        self.assertEqual(self.failure_until()["state"], "RESTART_FAILED")
        self.failure_until(990)
        self.assertEqual(self.backend.calls, 1)

    def test_restart_timeout_is_outcome_unknown_and_never_resent(self):
        self.baseline()
        self.backend.exception = subprocess.TimeoutExpired("systemctl", 15)
        self.assertEqual(self.failure_until()["state"], "OUTCOME_UNKNOWN")
        self.failure_until(990)
        self.assertEqual(self.backend.calls, 1)

    def test_process_crash_after_reservation_keeps_consumed_budget(self):
        self.baseline()
        self.failure_until(510)
        self.backend.before_effect = lambda: (_ for _ in ()).throw(SystemExit(99))
        with self.assertRaises(SystemExit):
            self.tick(sample(570, "failed", 250, 2, 0, 2200))
        self.assertEqual(self.store.load()["episode"]["attempts"], 1)
        self.backend.before_effect = None
        for t in range(630, 1100, 60):
            self.tick(sample(t, "uncertain-child", 580, 2, 0, 2200))
        self.assertEqual(self.backend.calls, 1)

    def test_repeated_snapshot_cannot_satisfy_persistence(self):
        self.baseline()
        self.tick(sample(390, "failed", 250, 2, 0, 2200))
        for t in range(420, 1000, 30):
            self.tick(sample(t, "failed", 250, 2, 0, 2200, source_now=1_800_000_390))
        self.assertEqual(self.backend.calls, 0)

    def test_high_rate_or_position_progress_excludes_failure(self):
        for rate, positions in ((20, 0), (2, 1)):
            with self.subTest(rate=rate, positions=positions):
                self.baseline()
                for t in range(270, 1100, 60):
                    self.tick(sample(t, "failed", 250, rate, positions))
                self.assertEqual(self.backend.calls, 0)

    def test_equal_threshold_is_not_below(self):
        self.baseline()
        for t in range(270, 1000, 60):
            self.tick(sample(t, "failed", 250, 10, 0, 2200))
        self.assertEqual(self.backend.calls, 0)

    def test_sample_loss_or_decoder_reset_only_alerts(self):
        self.baseline()
        result = self.failure_until(990, lost_buffers=1)
        self.assertEqual(self.backend.calls, 0)
        self.assertEqual(result["state"], "DEGRADED")
        self.failure_until(1400, readsb_start=11)
        self.assertEqual(self.backend.calls, 0)

    def test_unknown_observation_breaks_persistence(self):
        self.baseline()
        self.failure_until(510)
        unknown = self.tick(ValueError("stale"))
        self.assertEqual(unknown["state"], "UNKNOWN")
        self.tick(sample(570, "failed", 250, 2, 0, 2200))
        self.assertEqual(self.backend.calls, 0)

    def test_binding_drift_prohibits_restart(self):
        self.baseline()
        self.failure_until(510)
        result = self.tick(sample(570, "failed", 250, 2, 0, 2200, config_sha="drift"))
        self.assertEqual(result["state"], "UNKNOWN")
        self.assertEqual(self.backend.calls, 0)

    def test_state_corruption_is_not_reinitialized(self):
        self.store.path.write_text('{"schema":1,"history":[],"episode":{"attempts":false}}')
        with patch.dict(os.environ, {"AIRSPY_RX_STATE_DIR": str(self.root)}, clear=True), redirect_stdout(io.StringIO()):
            self.assertEqual(guard.main(["observe"], self.backend), 2)
        self.assertEqual(self.backend.calls, 0)
        self.assertFalse(json.loads(self.store.path.read_text())["episode"]["attempts"])
        with self.assertRaises(ValueError):
            self.store.initialize()

    def test_observation_only_does_not_consume_attempt(self):
        self.baseline()
        for t in range(270, 1000, 60):
            result = self.tick(sample(t, "failed", 250, 2, 0, 2200), allow=False)
        self.assertEqual(result["state"], "WOULD_RESTART")
        self.assertEqual(self.store.load()["episode"]["attempts"], 0)
        self.assertEqual(self.backend.calls, 0)


    def test_state_write_failure_prevents_effect(self):
        self.baseline()
        self.failure_until(510)
        original = self.store.save
        def fail_on_reservation(state):
            if state["episode"]["attempts"] == 1:
                raise OSError("disk full")
            original(state)
        with patch.object(self.store, "save", fail_on_reservation), self.assertRaises(OSError):
            self.tick(sample(570, "failed", 250, 2, 0, 2200))
        self.assertEqual(self.backend.calls, 0)

    def test_clock_regression_breaks_failure_window(self):
        self.baseline()
        self.failure_until(510)
        result = self.tick(sample(570, "failed", 250, 2, 0, 2200,
                                  source_now=1_800_000_450))
        self.assertEqual(result["state"], "UNKNOWN")
        self.assertEqual(self.backend.calls, 0)

    def test_missing_state_keeps_marker_and_cannot_initialize_again(self):
        self.store.path.unlink()
        with patch.dict(os.environ, {"AIRSPY_RX_STATE_DIR":str(self.root)}, clear=True), redirect_stdout(io.StringIO()):
            self.assertEqual(guard.main(["observe"], self.backend), 2)
        with self.assertRaises(ValueError):self.store.initialize()
        self.assertFalse(self.store.path.exists())
        self.assertEqual(self.backend.calls, 0)


    def test_policy_change_preserves_consumed_budget(self):
        self.baseline();self.failure_until()
        self.policy=guard.Policy(rate_ratio=.15)
        for t in range(630,1500,60):self.tick(sample(t,'retry',580,2,0,2200))
        self.assertEqual(self.backend.calls,1)
        self.assertEqual(self.store.load()['episode']['attempts'],1)


class PublicGainAndBoundaryTests(unittest.TestCase):
    setUp=GuardTests.setUp
    tearDown=GuardTests.tearDown
    tick=GuardTests.tick
    baseline=GuardTests.baseline
    failure_until=GuardTests.failure_until
    def test_external_gain_change_has_one_extra_restart_and_does_not_write_config(self):
        self.baseline()
        cfg=self.root/'config'
        content=b'GAIN="18"\nOPTIONS="-b -p"\nSAMPLE_RATE="20"\n'
        cfg.write_bytes(content)
        with patch.object(guard,'CONFIG_FILE',cfg):
            for t in range(270,631,60):
                self.tick(sample(t,'external-gain',250,2,0,2200,gain=18,config_sha='gain-18',command_sha='args-18'))
            origin=self.store.load()['episode']['id']
            for t in range(690,1400,60):
                self.tick(sample(t,'gain-retry',640,2,0,2200,gain=18,config_sha='gain-18',command_sha='args-18'))
        self.assertEqual(self.backend.calls,1)
        self.assertEqual(self.store.load()['episode']['id'],origin)
        self.assertEqual(cfg.read_bytes(),content)

    def test_verified_recovery_then_external_gain_change_gets_own_one_attempt(self):
        self.baseline();self.failure_until()
        for t in range(630,1000,60):
            self.tick(sample(t,'retry',580,100,3))
        old=self.store.load()['episode']['id']
        for t in range(1070,1431,60):
            self.tick(sample(t,'external-gain',1050,2,0,9900,gain=18,config_sha='gain-18',command_sha='args-18'))
        e=self.store.load()['episode']
        self.assertNotEqual(e['id'],old)
        self.assertEqual(e['attempts'],1)
        self.assertEqual(self.backend.calls,2)
        self.assertEqual(self.store.load()['closed'][-1]['attempts'],1)

    def test_gain_drift_during_unknown_recovery_cannot_rearm(self):
        self.baseline()
        self.backend.exception=subprocess.TimeoutExpired('fixture',15)
        self.failure_until()
        for t in range(630,1500,60):
            result=self.tick(sample(t,'external-gain',600,2,0,2200,gain=18,config_sha='gain-18',command_sha='args-18'))
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertEqual(self.backend.calls,1)
        self.assertEqual(self.store.load()['episode']['attempts'],1)

    def test_public_cli_exposes_no_gain_or_power_writer(self):
        self.assertFalse(hasattr(guard,'gain_cycle'))
        with redirect_stdout(io.StringIO()), patch('sys.stderr',new=io.StringIO()),self.assertRaises(SystemExit):
            guard.main(['gain'],self.backend)
        self.assertEqual(self.backend.calls,0)

    def test_public_unit_is_observation_only_until_explicit_opt_in(self):
        unit=(SOURCE/'airspy-reception-guard.service').read_text()
        opt=(SOURCE/'enable-recovery.conf.example').read_text()
        self.assertIn('receiver_guard.py observe-only\n',unit)
        self.assertIn('receiver_guard.py observe\n',opt)
        for key in ['ProtectSystem=strict','ProtectHome=true','NoNewPrivileges=true','RestrictAddressFamilies=AF_UNIX']:
            self.assertIn(key,unit)


class ProcessBoundaryTests(unittest.TestCase):
    """Exercise CLI processes, flock, fsync and child commands; the effect is a fixture."""

    def setUp(self):
        GuardTests.setUp(self)
        self.action_log = self.root / "actions.jsonl"
        fake = self.root / "fixture-systemctl"
        fake.write_text('#!/usr/bin/python3\nimport sys,json,os\n'
                        'assert sys.argv[1:]==["restart","airspy_adsb.service"]\n'
                        f'with open({str(self.action_log)!r},"a") as f:\n'
                        ' f.write(json.dumps(sys.argv[1:])+"\\n");f.flush();os.fsync(f.fileno())\n')
        fake.chmod(0o700)
        self.fixture = self.root / "sample.json"
        self.runner = self.root / "runner.py"
        self.runner.write_text('import sys,json,subprocess,resource,os,signal\n'
            f'sys.path.insert(0,{str(SOURCE)!r})\nimport receiver_guard as guard\n'
            'resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))\n'
            'resource.setrlimit(resource.RLIMIT_CPU,(5,5))\n'
            'class FixtureBackend:\n'
            f' def collect(self,policy):return json.load(open({str(self.fixture)!r}))\n'
            ' def restart(self):\n'
            '  if os.environ.get("FIXTURE_CRASH"):os._exit(77)\n'
            f'  return subprocess.run([{str(fake)!r},"restart","airspy_adsb.service"],timeout=5).returncode\n'
            'if os.environ.get("FIXTURE_DISK_LIMIT"):\n'
            ' signal.signal(signal.SIGXFSZ,signal.SIG_IGN)\n'
            ' resource.setrlimit(resource.RLIMIT_FSIZE,(64,64))\n'
            'raise SystemExit(guard.main(["observe"],FixtureBackend()))\n')

    tearDown = GuardTests.tearDown
    tick = GuardTests.tick
    baseline = GuardTests.baseline
    failure_until = GuardTests.failure_until

    def run_child(self, env=None, asynchronous=False):
        e = {"PATH":"/usr/bin:/bin", "AIRSPY_RX_STATE_DIR":str(self.root)}
        e.update(env or {})
        command = [sys.executable, str(self.runner)]
        if asynchronous:
            return subprocess.Popen(command, env=e, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return subprocess.run(command, env=e, text=True, capture_output=True, timeout=10)

    def prepare_boundary(self):
        self.baseline()
        self.failure_until(510)
        self.fixture.write_text(json.dumps(sample(570, "failed", 250, 2, 0, 2200)))

    def test_parallel_outer_entrypoints_issue_one_real_fixture_command(self):
        self.prepare_boundary()
        children = [self.run_child(asynchronous=True) for _ in range(4)]
        for child in children:
            stdout, stderr = child.communicate(timeout=15)
            self.assertIn(child.returncode, (0, 2), (stdout, stderr))
        self.assertEqual(len(self.action_log.read_text().splitlines()), 1)
        self.assertEqual(self.store.load()["episode"]["attempts"], 1)

    def test_real_process_exit_after_reservation_does_not_resend(self):
        self.prepare_boundary()
        result = self.run_child({"FIXTURE_CRASH":"1"})
        self.assertEqual(result.returncode, 77)
        self.assertEqual(self.store.load()["episode"]["attempts"], 1)
        self.assertFalse(self.action_log.exists())
        self.fixture.write_text(json.dumps(sample(630, "child-after-unknown", 580, 2, 0, 2200)))
        self.assertIn(self.run_child().returncode, (0, 2))
        self.assertFalse(self.action_log.exists())
        self.assertEqual(self.store.load()["episode"]["attempts"], 1)

    def test_real_file_size_limit_stops_before_effect_and_preserves_state(self):
        self.prepare_boundary()
        before = self.store.path.read_bytes()
        result = self.run_child({"FIXTURE_DISK_LIMIT":"1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.action_log.exists())
        self.assertEqual(before, self.store.path.read_bytes())




class LiveAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.actual_path = Path
        self.binary = self.root / "airspy-binary"
        self.binary.write_bytes(b"fixture binary")
        self.config = self.root / "config"
        self.config.write_text('GAIN="16"\nOPTIONS="-b"\n')
        for name in ["run/airspy_adsb", "run/readsb", "proc/123", "proc/sys/kernel/random"]:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / "proc/123/exe").symlink_to(self.binary)
        (self.root / "proc/123/cmdline").write_bytes(b"\0".join([b"airspy_adsb",b"-g",b"16",b"-b",b""]))
        (self.root / "proc/sys/kernel/random/boot_id").write_text("fixture-boot")
        now = time.time()
        self.airspy = {"now":now-2, "gain":16, "df_counts":[100]*32, "lost_buffers":0}
        self.readsb = {"now":now-2,"total":{"start":now-1000,"position_count_total":100}}
        self.aircraft = {"now":now-1,"aircraft":[{"lat":36,"lon":140,"seen_pos":1}]}
        self.backend = guard.LiveBackend()
        self.service = {"ActiveState":"active","SubState":"running","MainPID":"123",
                        "InvocationID":"fixture-invocation",
                        "ExecMainStartTimestampMonotonic":str(int((time.monotonic()-100)*1e6))}

    def tearDown(self):self.tmp.cleanup()

    def collect(self):
        for rel,value in [("run/airspy_adsb/stats.json",self.airspy),
                          ("run/readsb/stats.json",self.readsb),
                          ("run/readsb/aircraft.json",self.aircraft)]:
            (self.root/rel).write_text(json.dumps(value))
        def mapped(value):
            value=str(value)
            if value.startswith(("/proc", "/run")):
                return self.root/value.lstrip("/")
            return self.actual_path(value)
        with patch.object(guard,"Path",mapped),patch.object(guard,"CONFIG_FILE",self.config),\
             patch.object(guard,"BINARY",str(self.binary)),patch.object(self.backend,"service",return_value=self.service):
            return self.backend.collect(guard.Policy())

    def test_real_json_files_validated_and_identifiers_not_exported(self):
        self.aircraft["aircraft"][0]["hex"]="private-id"
        result=self.collect()
        self.assertEqual(result["fresh_positions"],1)
        self.assertNotIn("private-id",json.dumps(result))

    def test_stale_future_bool_and_nonfinite_source_times_rejected(self):
        for value in [time.time()-91,time.time()+5,True,float("nan")]:
            with self.subTest(value=value):
                self.airspy["now"]=value
                with self.assertRaises(ValueError):self.collect()

    def test_invalid_gain_df_counters_and_mismatched_gain_rejected(self):
        for key,value in [("gain",19),("gain",True),("df_counts",[1]*31),("df_counts",[-1]*32),("lost_buffers",-1)]:
            old=self.airspy.get(key);self.airspy[key]=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):self.collect()
            self.airspy[key]=old

    def test_restart_command_is_exactly_airspy_only(self):
        with patch.object(guard.subprocess,"run") as run:
            run.return_value.returncode=0
            self.assertEqual(self.backend.restart(),0)
            self.assertEqual(run.call_args.args[0],["/usr/bin/systemctl","restart","airspy_adsb.service"])




if __name__ == "__main__":
    unittest.main()
