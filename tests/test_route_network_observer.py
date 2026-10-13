"""経路identity、観測不能、ICMP単独失敗、計画停止、学習汚染の固定期待値。"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / "ops/scripts"
sys.path.insert(0, str(SCRIPTS))
import route_network_observer as obs


def utc(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def sample():
    return {"pings": {name: {"ok": True} for name in obs.PUBLIC_PINGS},
            "anchors": {"status": "VALID", "probes": {name: True for name in
                        ("cloudflare_v4", "google_v4", "cloudflare_v6", "google_v6")}},
            "local": {"status": "VALID", "network": {"network_down": False, "tcp_probe_ok": True, "dns_ok": True}},
            "oci": {"status": "VALID", "twitch_planned_break": False},
            "planes": {name: {"status": "FLOWING"} for name in ("local_ack", "oci_input", "youtube", "twitch")}}


class RateTests(unittest.TestCase):
    def test_rate_uses_source_time(self):
        r = obs.rate({"id": "a", "bytes": 100, "source_time": 100},
                     {"id": "a", "bytes": 1000100}, utc(105), 107)
        self.assertEqual((r["mbps"], r["interval_sec"], r["status"]), (1.6, 5, "FLOWING"))

    def test_new_generation_never_subtracts_old_counter(self):
        r = obs.rate({"id": "a", "bytes": 9999, "source_time": 100},
                     {"id": "b", "bytes": 1}, utc(105), 107)
        self.assertEqual(r["status"], "WARMING")
        self.assertNotIn("mbps", r)

    def test_counter_regression_is_unknown(self):
        r = obs.rate({"id": "a", "bytes": 9999, "source_time": 100},
                     {"id": "a", "bytes": 1}, utc(105), 107)
        self.assertEqual(r["status"], "UNKNOWN")

    def test_duplicate_and_gap_are_unknown(self):
        for source in (100, 146):
            self.assertEqual(obs.rate({"id": "a", "bytes": 0, "source_time": 100},
                                     {"id": "a", "bytes": 1}, utc(source), source)["status"], "UNKNOWN")

    def test_stale_and_future_sources_are_unknown(self):
        for source in (69, 103):
            self.assertEqual(obs.rate(None, {"id": "a", "bytes": 1}, utc(source), 100)["status"], "UNKNOWN")

    def test_boolean_counter_is_invalid(self):
        self.assertEqual(obs.rate(None, {"id": "a", "bytes": True}, utc(100), 100)["status"], "UNKNOWN")


class AssessmentTests(unittest.TestCase):
    def test_progress_is_transport_only(self):
        self.assertEqual(obs.assessment(sample(), {})["kind"], "TRANSPORT_PROGRESS_OBSERVED")

    def test_icmp_failure_with_tcp_success_is_not_wan_loss(self):
        s = sample()
        for ping in s["pings"].values():
            ping["ok"] = False
        for _ in range(4):
            self.assertEqual(obs.assessment(s, {})["kind"], "ICMP_UNREACHABLE_WITH_TCP_PROGRESS")

    def test_loss_requires_multiple_planes_and_three_samples(self):
        s = sample()
        for ping in s["pings"].values():
            ping["ok"] = False
        s["anchors"]["probes"] = {k: False for k in s["anchors"]["probes"]}
        s["local"]["network"]["tcp_probe_ok"] = False
        s["planes"]["oci_input"]["status"] = "UNKNOWN"
        streaks = {}
        self.assertEqual(obs.assessment(s, streaks)["kind"], "LOCAL_CONNECTIVITY_LOSS_CONFIRMING")
        self.assertEqual(obs.assessment(s, streaks)["kind"], "LOCAL_CONNECTIVITY_LOSS_CONFIRMING")
        self.assertEqual(obs.assessment(s, streaks)["kind"], "LOCAL_WAN_LOSS_SUSPECTED")
        s["pings"]["google"]["ok"] = True
        self.assertNotIn("LOSS", obs.assessment(s, streaks)["kind"])

    def test_positive_oci_progress_blocks_wan_loss(self):
        s = sample()
        for ping in s["pings"].values():
            ping["ok"] = False
        s["anchors"]["probes"] = {k: False for k in s["anchors"]["probes"]}
        s["local"]["network"]["tcp_probe_ok"] = False
        self.assertEqual(obs.assessment(s, {})["kind"], "ICMP_UNREACHABLE_WITH_TCP_PROGRESS")

    def test_ipv6_success_prevents_full_wan_claim(self):
        s = sample()
        for ping in s["pings"].values():
            ping["ok"] = False
        s["anchors"]["probes"]["cloudflare_v4"] = False
        s["anchors"]["probes"]["google_v4"] = False
        s["local"]["network"]["tcp_probe_ok"] = False
        s["planes"]["oci_input"]["status"] = "UNKNOWN"
        streaks = {}
        for _ in range(3):
            result = obs.assessment(s, streaks)
        self.assertEqual(result["kind"], "IPV4_PATH_LOSS_SUSPECTED")

    def test_unknown_anchors_cannot_confirm_loss(self):
        s = sample()
        for ping in s["pings"].values():
            ping["ok"] = False
        s["anchors"] = {"status": "UNKNOWN"}
        s["planes"]["oci_input"]["status"] = "UNKNOWN"
        s["oci"]["status"] = "UNKNOWN"
        s["local"]["network"]["tcp_probe_ok"] = False
        for _ in range(4):
            self.assertEqual(obs.assessment(s, {})["kind"], "OCI_OBSERVATION_UNAVAILABLE")

    def test_dns_failure_is_separate(self):
        s = sample()
        s["local"]["network"]["dns_ok"] = False
        self.assertEqual(obs.assessment(s, {})["kind"], "DNS_FAILURE_SUSPECTED")

    def test_twitch_break_keeps_youtube_progress(self):
        s = sample()
        s["oci"]["twitch_planned_break"] = True
        s["planes"]["twitch"]["status"] = "DISCONNECTED"
        for _ in range(4):
            self.assertEqual(obs.assessment(s, {})["kind"], "TRANSPORT_PROGRESS_OBSERVED")

    def test_single_provider_stop_does_not_claim_wan_loss(self):
        s = sample()
        s["planes"]["twitch"]["status"] = "STALLED"
        streaks = {}
        for _ in range(3):
            result = obs.assessment(s, streaks)
        self.assertEqual(result["kind"], "PROVIDER_OUTPUT_NOT_PROGRESSING")
        self.assertEqual(result["providers"], ["twitch"])

    def test_unknown_interrupts_provider_confirmation(self):
        s = sample()
        s["planes"]["twitch"]["status"] = "STALLED"
        streaks = {}
        obs.assessment(s, streaks)
        obs.assessment(s, streaks)
        s["oci"]["status"] = "UNKNOWN"
        obs.assessment(s, streaks)
        s["oci"]["status"] = "VALID"
        self.assertNotEqual(obs.assessment(s, streaks)["kind"], "PROVIDER_OUTPUT_NOT_PROGRESSING")

    def test_error_flag_is_observed_even_if_counter_moves(self):
        s = sample()
        s["oci"]["destinations"] = [{"provider": "youtube", "error_present": True}]
        self.assertEqual(obs.assessment(s, {})["kind"], "PROVIDER_OUTPUT_ERROR_OBSERVED")


class BaselineTests(unittest.TestCase):
    def test_route_partition_and_healthy_training_are_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            store = obs.Store(Path(directory))
            store.db.executemany("INSERT INTO rates VALUES(?,?,?,?,?)", [
                (10, "direct", "local_ack", 4.9, 1), (20, "oci", "local_ack", 3.6, 1),
                (30, "oci", "local_ack", .1, 0)])
            result = store.baselines("oci", 40)["windows"]["1d"]["local_ack"]
            self.assertEqual((result["sample_count"], result["healthy_count"], result["median_mbps"]), (2, 1, 3.6))
            self.assertFalse(result["ready"])
            store.db.close()

    def test_ready_requires_full_span_and_coverage_and_bounded_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            store = obs.Store(Path(directory))
            store.db.executemany("INSERT INTO rates VALUES(?,?,?,?,?)",
                                 [(t, "oci", "oci_input", 3.6, 1) for t in range(15, 86401, 15)])
            self.assertTrue(store.baselines("oci", 86401)["windows"]["1d"]["oci_input"]["ready"])
            store.cache_at = 0
            store.db.execute("DELETE FROM rates WHERE ts BETWEEN 10000 AND 10100")
            self.assertFalse(store.baselines("oci", 86401)["windows"]["1d"]["oci_input"]["ready"])
            store.db.close()

    def test_new_store_does_not_inherit_legacy_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            store = obs.Store(Path(directory))
            self.assertFalse(store.baselines("new", 100)["windows"]["1d"]["local_ack"]["ready"])
            store.db.close()

    def test_known_planned_pause_is_censored_but_unknown_gap_blocks_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            store = obs.Store(Path(directory))
            paused = list(range(10005, 10111, 15))
            store.db.executemany("INSERT INTO rates VALUES(?,?,?,?,?)",
                                 [(t, "oci", "twitch", 3.6, 1) for t in range(15, 86401, 15) if t not in paused])
            store.db.executemany("INSERT INTO censored VALUES(?,?,?,?)",
                                 [(t, "oci", "twitch", "TWITCH_PLANNED_BREAK") for t in paused])
            result = store.baselines("oci", 86401)["windows"]["1d"]["twitch"]
            self.assertTrue(result["ready"])
            self.assertEqual(result["censored_sample_count"], len(paused))
            store.db.execute("DELETE FROM censored")
            store.cache_at = 0
            self.assertFalse(store.baselines("oci", 86401)["windows"]["1d"]["twitch"]["ready"])
            store.db.close()


class CycleTrainingTests(unittest.TestCase):
    def collect(self, maintenance_inactive=True, planned_break=False):
        clock = [100.0]
        local = lambda _now: {"status": "VALID", "observed_at": utc(clock[0]), "route": "PRIMARY", "route_generation": 2,
                              "encoder_profile_sha256": "e" * 64, "target_sha256": "t", "ffmpeg_running": True,
                              "stream_established": True, "in_flight_count": 0, "network": {"dns_ok": True}, "tcp": {},
                              "point": {"id": "local", "bytes": int(clock[0] * 450000)}}
        def remote(_reader):
            return {"status": "VALID", "observed_at": utc(clock[0]), "host_boot_id": "boot", "relay_instance": "relay",
                    "relay_source_sha256": "r" * 64, "enabled": True, "halted": False,
                    "input": {"id": "input", "bytes": int(clock[0] * 450000)},
                    "twitch_planned_break": planned_break,
                    "destinations": [{"provider": provider, "id": provider, "bytes": int(clock[0] * 450000),
                                      "error_present": False} for provider in (("youtube",) if planned_break else ("youtube", "twitch"))]}
        with tempfile.TemporaryDirectory() as directory:
            store = obs.Store(Path(directory))
            with patch.object(obs.time, "time", side_effect=lambda: clock[0]), \
                 patch.object(obs, "local_facts", side_effect=local), \
                 patch.object(obs, "remote_facts", side_effect=remote), \
                 patch.object(obs, "ping_facts", return_value=sample()["pings"]), \
                 patch.object(obs, "anchor_facts", return_value=sample()["anchors"]), \
                 patch.object(obs, "network_epoch", return_value={"status": "VALID", "identity_sha256": "wan"}), \
                 patch.object(obs, "maintenance_facts", return_value={"status": "VALID", "inactive": maintenance_inactive}):
                previous, streaks = {}, {}
                first = obs.cycle(previous, streaks, store, Path("unused"))
                self.assertFalse(any(first["training_eligible"].values()))
                clock[0] = 115.0
                second = obs.cycle(previous, streaks, store, Path("unused"))
            store.db.close()
            return second

    def test_positive_transport_during_maintenance_is_not_normal_training(self):
        value = self.collect(maintenance_inactive=False)
        self.assertEqual(value["assessment"]["kind"], "TRANSPORT_PROGRESS_OBSERVED")
        self.assertFalse(any(value["training_eligible"].values()))

    def test_planned_twitch_stop_does_not_pollute_other_baselines(self):
        value = self.collect(planned_break=True)
        self.assertEqual(value["assessment"]["kind"], "TRANSPORT_PROGRESS_OBSERVED")
        self.assertEqual(value["training_eligible"], {"local_ack": True, "oci_input": True, "youtube": True, "twitch": False})
        self.assertEqual(value["censored_planes"], ["twitch"])


class ReaderTests(unittest.TestCase):
    def test_remote_reader_unknown_never_becomes_valid(self):
        with patch.object(obs.subprocess, "run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = b'{"schema":"stream.route_oci_facts.v1","status":"UNKNOWN"}'
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "reader.py"
                path.write_text("print('test')")
                self.assertEqual(obs.remote_facts(path)["status"], "UNKNOWN")

    def test_atomic_file_uses_private_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            obs.atomic(path, {"test": True})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(path.read_text()), {"test": True})

    def test_maintenance_requires_positive_bound_inactive_proof(self):
        target = {"pid": 42}
        raw = {"source_observed_at": utc(100), "fresh_until": utc(120), "target_snapshot_status": "VALID",
               "payload": {"available": True, "source_target_identity": target, "maintenance_state": "INACTIVE",
                           "proof": {"positive_inactive_proof": True, "integrity_ok": True, "startup_reconciled": True,
                                     "transaction_uncertain": False, "active_fence_count": 0, "incomplete_abort_count": 0,
                                     "incomplete_exit_count": 0, "unresolved_transaction_count": 0}}}
        with patch.object(obs, "read_json", return_value=raw):
            self.assertTrue(obs.maintenance_facts(105, obs.digest(target))["inactive"])
            self.assertFalse(obs.maintenance_facts(121, obs.digest(target))["inactive"])
            self.assertFalse(obs.maintenance_facts(105, "other-target")["inactive"])
            raw["payload"]["proof"]["active_fence_count"] = 1
            self.assertFalse(obs.maintenance_facts(105, obs.digest(target))["inactive"])

    def test_network_epoch_does_not_leak_addresses(self):
        raw = {"ts_utc": utc(100), "signature": {"ipv4_default_dev": "eth0", "ipv4_default_gateway": "192.0.2.1",
                                                "public_ipv4": "198.51.100.1"}}
        with patch.object(obs, "read_json", return_value=raw):
            result = obs.network_epoch(105)
            self.assertEqual(result["status"], "VALID")
            self.assertNotIn("198.51.100.1", json.dumps(result))
            first = result["identity_sha256"]
            raw["signature"]["public_ipv4"] = "198.51.100.2"
            self.assertNotEqual(obs.network_epoch(105)["identity_sha256"], first)
            self.assertEqual(obs.network_epoch(281)["status"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
