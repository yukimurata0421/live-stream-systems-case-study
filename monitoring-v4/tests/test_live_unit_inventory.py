from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr
from importlib.util import module_from_spec, spec_from_file_location
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from stream_monitoring_v4.architecture import inventory_report
from stream_monitoring_v4.inventory.contract import (
    evaluate_live_unit_contract,
    load_live_unit_contract,
)
from stream_monitoring_v4.inventory.systemd import (
    SNAPSHOT_SCHEMA,
    collection_failure_code,
    collect_systemd_snapshot,
    load_systemd_snapshot,
    parse_show_blocks,
    parse_unit_file_names,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT.parent if (ROOT.parent / "src" / "stream_core").is_dir() else ROOT.parent / "stream_v3"
CONTRACT_PATH = ROOT / "config" / "monitoring_v4_live_unit_contract.json"
MANIFEST_PATH = ROOT / "config" / "monitoring_v4_inventory.example.json"


def healthy_snapshots(contract: dict[str, object]) -> list[dict[str, object]]:
    snapshots: list[dict[str, object]] = []
    for host in contract["hosts"]:  # type: ignore[index]
        units: list[dict[str, str]] = []
        for group in host["groups"]:
            expected = group["expected"]
            for name in group["units"]:
                units.append(
                    {
                        "name": name,
                        "load_state": expected["load_state"][0],
                        "active_state": expected["active_state"][0],
                        "sub_state": expected["sub_state"][0],
                        "unit_file_state": expected["unit_file_state"][0],
                    }
                )
        snapshots.append(
            {
                "schema": SNAPSHOT_SCHEMA,
                "captured_at_utc": "2026-08-15T03:00:00Z",
                "host_role": host["host_role"],
                "patterns": host["patterns"],
                "unit_count": len(units),
                "units": sorted(units, key=lambda item: item["name"]),
            }
        )
    return snapshots


class LiveUnitInventoryTests(unittest.TestCase):
    def test_systemd_parsers_are_exact_and_reject_duplicate_blocks(self) -> None:
        listed = """
stream-v3-a.service enabled enabled
unrelated.service enabled enabled
stream-v3-b.timer static -
"""
        self.assertEqual(
            parse_unit_file_names(listed, ("stream-v3-*",)),
            ["stream-v3-a.service", "stream-v3-b.timer"],
        )
        shown = """Id=stream-v3-a.service
LoadState=loaded
ActiveState=active
SubState=running
UnitFileState=enabled

Id=stream-v3-b.timer
LoadState=loaded
ActiveState=active
SubState=waiting
UnitFileState=enabled
"""
        parsed = parse_show_blocks(shown)
        self.assertEqual(parsed["stream-v3-b.timer"]["SubState"], "waiting")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_show_blocks(shown + "\n" + shown.split("\n\n", 1)[0] + "\n")

    def test_collector_uses_argv_prefix_and_emits_no_endpoint(self) -> None:
        calls: list[tuple[str, ...]] = []

        def fake_run(command, _timeout):
            calls.append(tuple(command))
            if "list-unit-files" in command:
                return subprocess.CompletedProcess(command, 0, "stream-v3-a.service enabled enabled\n", "")
            return subprocess.CompletedProcess(
                command,
                0,
                "Id=stream-v3-a.service\nLoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\n",
                "",
            )

        payload = collect_systemd_snapshot(
            "arena-server",
            ("stream-v3-*",),
            command_prefix=("ssh", "arena-alias"),
            run_command=fake_run,
        )

        self.assertEqual(payload["unit_count"], 1)
        self.assertNotIn("arena-alias", json.dumps(payload))
        self.assertEqual(calls[0][:3], ("ssh", "arena-alias", "systemctl"))

    def test_collection_failure_code_is_bounded_and_secret_free(self) -> None:
        error = RuntimeError("ssh private-host.example: classified diagnostic")

        code = collection_failure_code("Raspberry-Pi-public-only", error)

        self.assertEqual(code, "collection_failed:Raspberry-Pi-public-only:RuntimeError")
        self.assertNotIn("private-host", code)
        with self.assertRaisesRegex(ValueError, "host_role"):
            collection_failure_code("invalid role", error)

    def test_contract_covers_all_three_hosts_and_source_units(self) -> None:
        contract = load_live_unit_contract(CONTRACT_PATH)
        report = evaluate_live_unit_contract(
            contract,
            healthy_snapshots(contract),
            repositories={"stream_v3": SOURCE, "stream_v4": ROOT},
        )

        self.assertTrue(report["static_ok"])
        self.assertTrue(report["runtime_complete"])
        self.assertTrue(report["evidence_complete"])
        self.assertEqual(report["required_host_count"], 3)
        counts = {item["host_role"]: item["expected_unit_count"] for item in report["hosts"]}
        self.assertEqual(counts, {"arena-server": 56, "Dell-runtime": 46, "Raspberry-Pi-public-only": 4})

    def test_contract_rejects_a_host_role_that_cannot_be_collected(self) -> None:
        payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        payload["hosts"][0]["host_role"] = "invalid role"
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "contract.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid or duplicate role"):
                load_live_unit_contract(path)

    def test_new_missing_or_failed_unit_fails_closed(self) -> None:
        contract = load_live_unit_contract(CONTRACT_PATH)
        snapshots = healthy_snapshots(contract)
        arena = snapshots[0]
        arena["units"].append(  # type: ignore[union-attr]
            {
                "name": "stream-v3-unclassified.timer",
                "load_state": "loaded",
                "active_state": "active",
                "sub_state": "waiting",
                "unit_file_state": "enabled",
            }
        )
        arena["unit_count"] = len(arena["units"])  # type: ignore[arg-type]
        dell = snapshots[1]
        removed = dell["units"].pop()  # type: ignore[union-attr]
        dell["unit_count"] = len(dell["units"])  # type: ignore[arg-type]
        pi = snapshots[2]
        pi_unit = next(item for item in pi["units"] if item["name"] == "nginx.service")  # type: ignore[union-attr]
        pi_unit["active_state"] = "failed"
        pi_unit["sub_state"] = "failed"

        report = evaluate_live_unit_contract(
            contract,
            snapshots,
            repositories={"stream_v3": SOURCE, "stream_v4": ROOT},
        )

        by_role = {item["host_role"]: item for item in report["hosts"]}
        self.assertEqual(by_role["arena-server"]["unexpected_units"], ["stream-v3-unclassified.timer"])
        self.assertIn(removed["name"], by_role["Dell-runtime"]["missing_units"])
        self.assertEqual(len(by_role["Raspberry-Pi-public-only"]["state_mismatches"]), 2)
        self.assertFalse(report["evidence_complete"])

    def test_collection_failure_preserves_other_host_evidence(self) -> None:
        contract = load_live_unit_contract(CONTRACT_PATH)
        snapshots = healthy_snapshots(contract)[:2]
        failure = "collection_failed:Raspberry-Pi-public-only:RuntimeError"

        report = evaluate_live_unit_contract(
            contract,
            snapshots,
            repositories={"stream_v3": SOURCE, "stream_v4": ROOT},
            collection_errors=(failure,),
        )

        by_role = {item["host_role"]: item for item in report["hosts"]}
        self.assertTrue(by_role["arena-server"]["runtime_verified"])
        self.assertTrue(by_role["Dell-runtime"]["runtime_verified"])
        self.assertFalse(by_role["Raspberry-Pi-public-only"]["runtime_verified"])
        self.assertEqual(report["verified_required_host_count"], 2)
        self.assertEqual(report["snapshot_errors"], [failure])
        self.assertFalse(report["evidence_complete"])

    def test_inventory_cli_writes_partial_report_and_requires_completeness(self) -> None:
        script = ROOT / "ops" / "scripts" / "monitoring_v4_inventory.py"
        spec = spec_from_file_location("monitoring_v4_inventory_test_module", script)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader if spec else None)
        module = module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(module)  # type: ignore[union-attr]
        secret = "ssh private-host.example: classified diagnostic"

        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "partial.json"
            stderr = StringIO()
            with patch.object(module, "collect_systemd_snapshot", side_effect=RuntimeError(secret)):
                with redirect_stderr(stderr):
                    return_code = module.main(
                        [
                            "--source-repo",
                            str(SOURCE),
                            "--collect-systemd",
                            "Raspberry-Pi-public-only=unresolved-alias",
                            "--require-live-completeness",
                            "--output",
                            str(output),
                        ]
                    )
            payload = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(return_code, 1)
        self.assertTrue(payload["ok"])
        self.assertFalse(payload["evidence_complete"])
        self.assertEqual(
            payload["live_surface"]["snapshot_errors"],
            ["collection_failed:Raspberry-Pi-public-only:RuntimeError"],
        )
        serialized = json.dumps(payload) + stderr.getvalue()
        self.assertNotIn("private-host", serialized)
        self.assertNotIn("unresolved-alias", serialized)

    def test_snapshot_loader_rejects_count_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snapshot.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": SNAPSHOT_SCHEMA,
                        "captured_at_utc": "2026-08-15T03:00:00Z",
                        "host_role": "arena-server",
                        "patterns": ["stream-v3-*"],
                        "unit_count": 2,
                        "units": [],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "unit_count"):
                load_systemd_snapshot(path)

    def test_architecture_report_distinguishes_structure_from_live_evidence(self) -> None:
        contract = load_live_unit_contract(CONTRACT_PATH)
        without_live = inventory_report(
            ROOT,
            SOURCE,
            MANIFEST_PATH,
            live_unit_contract=CONTRACT_PATH,
        )
        with_live = inventory_report(
            ROOT,
            SOURCE,
            MANIFEST_PATH,
            live_unit_contract=CONTRACT_PATH,
            systemd_snapshots=healthy_snapshots(contract),
        )

        self.assertTrue(without_live["ok"])
        self.assertFalse(without_live["evidence_complete"])
        self.assertTrue(with_live["ok"])
        self.assertTrue(with_live["evidence_complete"])


if __name__ == "__main__":
    unittest.main()
