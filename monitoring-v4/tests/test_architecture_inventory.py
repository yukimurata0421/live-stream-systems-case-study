from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stream_monitoring_v4.architecture import (
    forbidden_imports,
    load_inventory,
    migration_source_contamination,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "examples" / "external-migration-source"
MANIFEST = ROOT / "config" / "monitoring_v4_inventory.example.json"
PHASE_GATES = ROOT / "config" / "monitoring_v4_phase_gates.json"


class ArchitectureInventoryTests(unittest.TestCase):
    def test_manifest_has_required_ownership_and_pi_boundary(self) -> None:
        inventory = load_inventory(MANIFEST)
        entries = {item["id"]: item for item in inventory["entries"]}
        pi = entries["raspberry-pi-public-publisher"]
        self.assertEqual(pi["host_role"], "Raspberry-Pi-public-only")
        self.assertEqual(pi["mutations"], [])
        self.assertIn("outside Monitoring v4", pi["target_owner"])
        baseline = inventory["known_source_monitoring_release"]
        self.assertEqual(baseline["release"], "stream-v3-input-quality-raw-authority-v1.19-20260811")
        self.assertEqual(baseline["scope"], "arena-server monitoring plane only")
        self.assertFalse(baseline["v4_production_changed"])
        for entry in entries.values():
            for field in ("reads", "writes", "credentials", "mutations"):
                self.assertIsInstance(entry[field], list)
            self.assertTrue(entry["target_owner"])

    def test_phase_manifest_keeps_all_production_authority_disabled(self) -> None:
        payload = json.loads(PHASE_GATES.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema"], "monitoring_v4.phase_gates.v1")
        self.assertEqual([item["phase"] for item in payload["phases"]], [f"R{index}" for index in range(7)])
        authority = payload["authority_grants"]
        self.assertTrue(authority["isolated_shadow_pilot_state_created"])
        self.assertTrue(authority["isolated_shadow_service_installed"])
        self.assertFalse(authority["isolated_shadow_timer_enabled"])
        self.assertTrue(authority["isolated_shadow_k3s_subsystem_deployed"])
        self.assertTrue(authority["isolated_shadow_postgresql_statefulset_deployed"])
        self.assertTrue(authority["credential_free_safe_input_projector_deployed"])
        self.assertTrue(authority["host_detection_only_sentinel_installed"])
        for field in (
            "production_service_installed",
            "production_state_write",
            "real_notification_credential",
            "real_notification_writer",
            "dell_runtime_mutation",
            "raspberry_pi_internal_v4_dependency",
        ):
            self.assertFalse(authority[field])
        self.assertTrue(payload["migration_source_reference"]["semantics_ported_to_v4"])
        self.assertFalse(payload["migration_source_reference"]["v4_production_changed"])
        units = ROOT / "ops" / "systemd"
        self.assertEqual(
            {path.name for path in units.iterdir()},
            {
                "stream-monitoring-v4-shadow.service",
                "stream-monitoring-v4-shadow.timer",
                "stream-monitoring-v4-shadow-report.service",
                "stream-monitoring-v4-shadow-report.timer",
                "stream-monitoring-v4-k3s-sentinel.service",
                "stream-monitoring-v4-k3s-sentinel.timer",
            },
        )
        shadow_service_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in units.glob("stream-monitoring-v4-shadow*.service")
        )
        self.assertIn("RestrictAddressFamilies=AF_UNIX", shadow_service_text)
        for forbidden in ("WEBHOOK", "TOKEN=", "PASSWORD=", "kubectl", "systemctl", "ssh "):
            self.assertNotIn(forbidden, shadow_service_text)
        self.assertIn("--summary-only", shadow_service_text)
        self.assertIn("shadow_report", shadow_service_text)
        sentinel = (units / "stream-monitoring-v4-k3s-sentinel.service").read_text(
            encoding="utf-8"
        )
        self.assertIn("monitoring_v4_k3s_sentinel.py", sentinel)
        self.assertNotIn("restart k3s", sentinel)
        self.assertFalse((ROOT / "src" / "stream_runtime_executor").exists())

    def test_document_index_links_exist(self) -> None:
        expected = (
            ROOT / "docs" / "architecture.md",
            ROOT / "docs" / "failure-mode-mitigation-record.md",
            ROOT / "docs" / "hardening.md",
            ROOT / "docs" / "status.md",
            ROOT / "docs" / "history.md",
            ROOT / "docs" / "postgresql-k3s.md",
            ROOT / "docs" / "public-release.md",
        )
        self.assertTrue(all(path.is_file() for path in expected))

    def test_current_tree_has_no_forbidden_imports(self) -> None:
        self.assertEqual(forbidden_imports(ROOT, source_root=SOURCE), [])

    def test_contract_cannot_import_implementation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            package = root / "src" / "stream_contracts" / "monitoring_v4"
            package.mkdir(parents=True)
            (package / "bad.py").write_text("import stream_monitoring_v4\n", encoding="utf-8")
            violations = forbidden_imports(root, source_root=SOURCE)
        self.assertEqual([item.code for item in violations], ["contracts_import_implementation"])

    def test_monitoring_cannot_import_runtime_executor(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            package = root / "src" / "stream_monitoring_v4" / "control"
            package.mkdir(parents=True)
            (package / "bad.py").write_text("from stream_runtime_executor import actions\n", encoding="utf-8")
            violations = forbidden_imports(root, source_root=SOURCE)
        self.assertEqual([item.code for item in violations], ["monitoring_import_runtime_or_legacy_cli"])

    def test_relative_domain_import_cannot_reach_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            package = root / "src" / "stream_monitoring_v4" / "domains"
            package.mkdir(parents=True)
            (package / "bad.py").write_text("from ..adapters import base\n", encoding="utf-8")
            violations = forbidden_imports(root, source_root=SOURCE)
        self.assertEqual([item.code for item in violations], ["domain_import_application_io"])

    def test_source_contamination_guard_detects_v4_package_in_migration_source(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            source = Path(td)
            package = source / "src" / "stream_monitoring_v4"
            package.mkdir(parents=True)
            self.assertEqual(migration_source_contamination(source), ["src/stream_monitoring_v4"])

    def test_inventory_rejects_missing_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "inventory.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "monitoring_v4.current_surface_inventory.v1",
                        "entries": [{"id": "broken"}],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "missing fields"):
                load_inventory(path)


if __name__ == "__main__":
    unittest.main()
