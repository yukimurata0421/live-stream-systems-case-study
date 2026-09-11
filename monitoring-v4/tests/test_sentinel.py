from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from ops.scripts import monitoring_v4_k3s_sentinel as sentinel
from stream_contracts.monitoring_v4.time import utc_text
from stream_contracts.monitoring_v4.youtube_api import YouTubeApiEvidence
from stream_monitoring_v4.sentinel import kube as sentinel_kube
from stream_monitoring_v4.sentinel.assessment import SentinelEvidence, assess
from stream_monitoring_v4.sentinel.contracts import POSTGRES_IMAGE

from tests.helpers import BASE_TS


def _report_payload(*, generated_ts: int = BASE_TS) -> dict[str, object]:
    return {
        "schema": sentinel.REPORT_SCHEMA,
        "generated_at": utc_text(generated_ts),
        "build_revision": "a" * 40,
        "build_revision_immutable": True,
        "source_revision_known": True,
        "safety_boundary": {
            "real_delivery_enabled": False,
            "runtime_mutation_enabled": False,
            "raspberry_pi_dependency": False,
        },
        "parity_14d": {
            "unclassified_contract_difference_count": 0,
            "invalid_payload_count": 0,
        },
        "projection_integrity_14d": {"failed_cycles": 0},
        "latest_cycle": {
            "parity_payload_valid": True,
            "parity_unclassified_contract_difference_count": 0,
            "projection_integrity_complete": True,
            "projection_count": 6,
            "projection_expected_count": 6,
            "projection_rejection_count": 0,
        },
    }


class SentinelTests(unittest.TestCase):
    def test_youtube_api_evidence_is_schema_time_permission_and_probe_checked(self) -> None:
        payload = YouTubeApiEvidence(
            collector_revision="monitoring-v4-youtube-api-collector-r1",
            request_started_at=utc_text(BASE_TS),
            collected_at=utc_text(BASE_TS),
            probe_status="ok",
            result_kind="observed",
            http_status=0,
            error_reason="",
            lifecycle_status="live",
            stream_status="active",
            stream_health_status="good",
            configuration_issues=(),
            broadcast_id_sha256="a" * 64,
            bound_stream_id_sha256="b" * 64,
            active_broadcast_count=1,
            api_request_count=2,
            oauth_refresh_performed=True,
            oauth_scope_class="youtube_readonly_only",
            oauth_scope_count=1,
        ).to_dict()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "youtube-api.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            path.chmod(0o640)
            healthy = sentinel._youtube_api_status(
                path,
                now_ts=BASE_TS + 10,
                max_age_sec=300,
            )
            self.assertTrue(healthy["readable"])
            self.assertTrue(healthy["schema_supported"])
            self.assertTrue(healthy["fresh"])
            self.assertTrue(healthy["permissions_safe"])
            self.assertTrue(healthy["probe_ok"])

            path.chmod(0o600)
            unsafe_mode = sentinel._youtube_api_status(
                path,
                now_ts=BASE_TS + 10,
                max_age_sec=300,
            )
            self.assertFalse(unsafe_mode["permissions_safe"])

            path.chmod(0o640)
            payload["probe_status"] = "unknown"
            payload["result_kind"] = "api_timeout"
            payload["lifecycle_status"] = ""
            payload["stream_status"] = ""
            payload["stream_health_status"] = ""
            payload["broadcast_id_sha256"] = ""
            payload["bound_stream_id_sha256"] = ""
            path.write_text(json.dumps(payload), encoding="utf-8")
            unavailable = sentinel._youtube_api_status(
                path,
                now_ts=BASE_TS + 10,
                max_age_sec=300,
            )
            self.assertTrue(unavailable["readable"])
            self.assertTrue(unavailable["fresh"])
            self.assertFalse(unavailable["probe_ok"])

    def test_runner_skips_kubectl_when_k3s_is_inactive_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for name in ("backup", "independent", "verification", "database"):
                (root / name).mkdir()
            args = Namespace(
                output=root / "sentinel.json",
                report=root / "report.json",
                report_max_age_sec=420,
                youtube_api_evidence=root / "youtube-api.json",
                youtube_api_max_age_sec=300,
                backup_dir=root / "backup",
                independent_backup_dir=root / "independent",
                backup_max_age_sec=27 * 3600,
                restore_verification_dir=root / "verification",
                restore_verification_max_age_sec=27 * 3600,
                restore_verification_pending_grace_sec=3600,
                database_storage_dir=root / "database",
                minimum_free_bytes=0,
                minimum_free_percent=0.0,
                minimum_free_inode_percent=0.0,
                kubectl="k3s",
            )
            with patch(
                "stream_monitoring_v4.sentinel.runner.kube.command",
                return_value=(False, "inactive"),
            ), patch(
                "stream_monitoring_v4.sentinel.runner.kube.pods"
            ) as pods, patch(
                "stream_monitoring_v4.sentinel.runner.kube.release_identity"
            ) as release:
                assessment = sentinel.collect(args, now_ts=BASE_TS)
        pods.assert_not_called()
        release.assert_not_called()
        self.assertFalse(assessment.healthy)
        self.assertIn("k3s_active", assessment.failed_checks)
        self.assertEqual(assessment.payload["k3s"]["detail"], "inactive")

    def test_previous_continuity_requires_exact_schema_types_and_component_counts(self) -> None:
        runtime = {}
        for component, count in sentinel.REQUIRED_COMPONENT_COUNTS.items():
            for index in range(count):
                runtime[f"{component}-{index}"] = {
                    "uid": f"uid-{component}-{index}",
                    "component": component,
                    "restart_count": 0,
                }
        previous = {
            "schema": sentinel.SENTINEL_SCHEMA,
            "release": {"revision": "a" * 40},
            "pod_runtime": runtime,
        }
        decoded, revision, complete = sentinel._previous_continuity(previous)
        self.assertTrue(complete)
        self.assertEqual(revision, "a" * 40)
        self.assertEqual(len(decoded), sum(sentinel.REQUIRED_COMPONENT_COUNTS.values()))

        malformed = json.loads(json.dumps(previous))
        malformed["pod_runtime"]["core-0"]["restart_count"] = "0"
        self.assertEqual(sentinel._previous_continuity(malformed), ({}, "", False))
        missing = json.loads(json.dumps(previous))
        missing["pod_runtime"].pop("exporter-1")
        self.assertFalse(sentinel._previous_continuity(missing)[2])

    def test_filesystem_capacity_and_independent_device_are_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status = sentinel._filesystem_status(
                Path(td),
                minimum_free_bytes=1,
                minimum_free_percent=0.0,
                minimum_free_inode_percent=0.0,
            )
        self.assertTrue(status["readable"])
        self.assertTrue(status["sufficient"])
        self.assertFalse(sentinel._filesystems_independent(status, status))
        second = dict(status)
        second["device"] = int(status["device"]) + 1
        self.assertTrue(sentinel._filesystems_independent(status, second))

        with tempfile.TemporaryDirectory() as td:
            insufficient = sentinel._filesystem_status(
                Path(td),
                minimum_free_bytes=2**63,
                minimum_free_percent=0.0,
                minimum_free_inode_percent=0.0,
            )
        self.assertFalse(insufficient["sufficient"])

    def test_filesystem_threshold_arguments_reject_unsafe_numbers(self) -> None:
        for arguments in (
            ["--minimum-free-bytes", "-1"],
            ["--minimum-free-percent", "nan"],
            ["--minimum-free-inode-percent", "101"],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                sentinel.main(
                    [
                        "--output",
                        "/tmp/not-written",
                        "--report",
                        "/tmp/not-read",
                        *arguments,
                    ]
                )

    def test_future_report_is_not_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "report.json"
            path.write_text(
                json.dumps(_report_payload(generated_ts=BASE_TS + 86400)),
                encoding="utf-8",
            )
            result = sentinel._report(path, now_ts=BASE_TS, max_age_sec=420)
        self.assertTrue(result["future"])
        self.assertFalse(result["fresh"])

    def test_parity_and_projection_failures_are_exposed(self) -> None:
        payload = _report_payload()
        payload["parity_14d"]["unclassified_contract_difference_count"] = 7
        payload["parity_14d"]["conflicting_rollout_evidence_count"] = 1
        payload["projection_integrity_14d"]["failed_cycles"] = 1
        payload["latest_cycle"]["projection_rejection_count"] = 1
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "report.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            result = sentinel._report(path, now_ts=BASE_TS + 1, max_age_sec=420)
        self.assertFalse(result["parity_clean"])
        self.assertEqual(result["parity_conflicting_rollout_evidence_count"], 1)
        self.assertFalse(result["projection_clean"])
        self.assertFalse(result["latest_cycle_clean"])

    def test_backup_requires_nonempty_nonfuture_fresh_dump(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self.assertFalse(
                sentinel._backup_status(root, now_ts=BASE_TS, max_age_sec=3600)["fresh"]
            )
            backup = root / "stream-v4-20260811T000000Z.dump"
            backup.write_bytes(b"valid")
            backup.chmod(0o600)
            digest = hashlib.sha256(backup.read_bytes()).hexdigest()
            checksum = backup.with_name(f"{backup.name}.sha256")
            checksum.write_text(f"{digest}  {backup.name}\n", encoding="ascii")
            checksum.chmod(0o600)
            backup.touch()
            # mtime is explicitly controlled because BASE_TS is a historical fixture.
            import os

            os.utime(backup, (BASE_TS - 60, BASE_TS - 60))
            self.assertTrue(
                sentinel._backup_status(root, now_ts=BASE_TS, max_age_sec=3600)["fresh"]
            )
            backup.chmod(0o640)
            checksum.chmod(0o640)
            self.assertTrue(
                sentinel._backup_status(root, now_ts=BASE_TS, max_age_sec=3600)["fresh"]
            )
            backup.write_bytes(b"corrupted")
            mismatch = sentinel._backup_status(
                root,
                now_ts=BASE_TS,
                max_age_sec=3600,
            )
            self.assertFalse(mismatch["fresh"])
            self.assertEqual(mismatch["error"], "backup_checksum_mismatch")
            backup.write_bytes(b"valid")
            os.utime(backup, (BASE_TS + 1, BASE_TS + 1))
            self.assertFalse(
                sentinel._backup_status(root, now_ts=BASE_TS, max_age_sec=3600)["fresh"]
            )

    def test_backup_selection_ignores_future_mtime_after_clock_rollback(self) -> None:
        import os

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            current = root / "stream-v4-20260810T000000Z.dump"
            future = root / "stream-v4-20260811T000000Z.dump"
            for path, content, modified in (
                (current, b"current", BASE_TS - 60),
                (future, b"future", BASE_TS + 86400),
            ):
                path.write_bytes(content)
                path.chmod(0o600)
                checksum = path.with_name(f"{path.name}.sha256")
                checksum.write_text(
                    f"{hashlib.sha256(content).hexdigest()}  {path.name}\n",
                    encoding="ascii",
                )
                checksum.chmod(0o600)
                os.utime(path, (modified, modified))
            selected = sentinel._backup_status(
                root,
                now_ts=BASE_TS,
                max_age_sec=3600,
            )
        self.assertTrue(selected["fresh"])
        self.assertEqual(Path(selected["path"]).name, current.name)
        self.assertEqual(selected["candidate_count"], 2)
        self.assertEqual(selected["future_candidate_count"], 1)

    def test_backup_permissions_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            backup = root / "stream-v4-20260811T000000Z.dump"
            backup.write_bytes(b"valid")
            backup.chmod(0o644)
            checksum = backup.with_name(f"{backup.name}.sha256")
            checksum.write_text(
                f"{hashlib.sha256(backup.read_bytes()).hexdigest()}  {backup.name}\n",
                encoding="ascii",
            )
            checksum.chmod(0o600)
            result = sentinel._backup_status(
                root,
                now_ts=int(backup.stat().st_mtime),
                max_age_sec=3600,
            )
        self.assertFalse(result["fresh"])
        self.assertEqual(result["error"], "backup_permissions_too_broad")

    def test_backup_checksum_must_name_the_selected_dump(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            backup = root / "stream-v4-20260811T000000Z.dump"
            backup.write_bytes(b"valid")
            backup.chmod(0o600)
            digest = hashlib.sha256(backup.read_bytes()).hexdigest()
            checksum = backup.with_name(f"{backup.name}.sha256")
            checksum.write_text(
                f"{digest}  another.dump\n",
                encoding="ascii",
            )
            checksum.chmod(0o600)
            result = sentinel._backup_status(
                root,
                now_ts=int(backup.stat().st_mtime),
                max_age_sec=3600,
            )
        self.assertFalse(result["fresh"])
        self.assertEqual(result["error"], "backup_checksum_invalid")

    def test_restore_verification_is_exact_and_has_bounded_pending_grace(self) -> None:
        backup_name = "stream-v4-20260811T000000Z.dump"
        digest = "c" * 64
        backup = {
            "fresh": True,
            "age_sec": 60,
            "path": f"/backup/{backup_name}",
            "sha256": digest,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            pending = sentinel._restore_verification_status(
                root,
                backup=backup,
                now_ts=BASE_TS,
                max_age_sec=3600,
                pending_grace_sec=120,
            )
            self.assertTrue(pending["acceptable"])
            self.assertTrue(pending["pending"])

            path = root / f"{backup_name}.restore-verified.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "monitoring_v4.postgresql_restore_verification.v1",
                        "backup_name": backup_name,
                        "backup_sha256": digest,
                        "verified_at": utc_text(BASE_TS - 30),
                        "schema_versions": [1, 2, 3, 4, 5, 6],
                        "observations": 10,
                        "shadow_cycles": 3,
                        "public_artifact_publications": 2,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            path.chmod(0o640)
            verified = sentinel._restore_verification_status(
                root,
                backup=backup,
                now_ts=BASE_TS,
                max_age_sec=3600,
                pending_grace_sec=0,
            )
            self.assertTrue(verified["verified"])
            self.assertTrue(verified["acceptable"])

            path.chmod(0o644)
            broad_permissions = sentinel._restore_verification_status(
                root,
                backup=backup,
                now_ts=BASE_TS,
                max_age_sec=3600,
                pending_grace_sec=0,
            )
            self.assertFalse(broad_permissions["acceptable"])
            self.assertFalse(broad_permissions["verified"])
            self.assertEqual(broad_permissions["error"], "ValueError")
            path.chmod(0o640)

            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["backup_sha256"] = "d" * 64
            path.write_text(json.dumps(payload), encoding="utf-8")
            mismatch = sentinel._restore_verification_status(
                root,
                backup=backup,
                now_ts=BASE_TS,
                max_age_sec=3600,
                pending_grace_sec=0,
            )
            self.assertFalse(mismatch["acceptable"])
            self.assertFalse(mismatch["verified"])

    def test_release_requires_explicit_image_digest(self) -> None:
        digest = "sha256:" + "b" * 64
        payload = json.dumps(
            {"data": {"build-revision": "a" * 40, "app-image-id": digest}}
        )
        with patch.object(sentinel_kube, "command", return_value=(True, payload)):
            ok, identity, error = sentinel._release_identity("k3s")
        self.assertTrue(ok)
        self.assertEqual(error, "")
        self.assertEqual(identity["app_image_id"], digest)

    def test_structurally_invalid_kubectl_json_fails_closed(self) -> None:
        digest = "sha256:" + "b" * 64
        invalid_release_payloads = (
            "[]",
            json.dumps({"data": []}),
            json.dumps({"data": {"build-revision": ["a" * 40], "app-image-id": digest}}),
            json.dumps({"data": {"build-revision": "a" * 40, "app-image-id": float("nan")}}),
        )
        for payload in invalid_release_payloads:
            with self.subTest(release=payload):
                with patch.object(sentinel_kube, "command", return_value=(True, payload)):
                    ok, identity, error = sentinel._release_identity("k3s")
                self.assertFalse(ok)
                self.assertEqual(identity, {})
                self.assertEqual(error, "release_json_invalid")

        invalid_pod_payloads = (
            "[]",
            json.dumps({"items": {}}),
            json.dumps({"items": [{"metadata": [], "status": {}, "spec": {}}]}),
            '{"items":[],"overflow":1e999}',
            '{"items":[],"overflow":9223372036854775808}',
        )
        for payload in invalid_pod_payloads:
            with self.subTest(pods=payload):
                with patch.object(sentinel_kube, "command", return_value=(True, payload)):
                    ok, pods, error = sentinel._pods("k3s")
                self.assertFalse(ok)
                self.assertEqual(pods, {})
                self.assertEqual(error, "pod_json_invalid")

        valid = json.dumps({"data": {"build-revision": "a" * 40, "app-image-id": digest}})
        with patch.object(sentinel_kube, "command", return_value=(True, valid)):
            self.assertTrue(sentinel._release_identity("k3s")[0])

    def test_ready_pod_requires_complete_container_identity(self) -> None:
        pod = {
            "metadata": {
                "name": "core-1",
                "uid": "uid-1",
                "labels": {"app.kubernetes.io/component": "core"},
            },
            "spec": {"containers": [{"image": "stream-monitoring-v4:" + "a" * 40}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [],
            },
        }
        with patch.object(
            sentinel_kube,
            "command",
            return_value=(True, json.dumps({"items": [pod]})),
        ):
            ok, _pods, error = sentinel._pods("k3s")
        self.assertFalse(ok)
        self.assertEqual(error, "pod_json_invalid")

        pod["status"]["containerStatuses"] = [
            {"imageID": "containerd://sha256:" + "b" * 64, "restartCount": 0}
        ]
        with patch.object(
            sentinel_kube,
            "command",
            return_value=(True, json.dumps({"items": [pod]})),
        ):
            ok, pods, error = sentinel._pods("k3s")
        self.assertTrue(ok)
        self.assertEqual(error, "")
        self.assertEqual(pods["core-1"]["restart_count"], 0)

        duplicate = {"items": [pod, pod]}
        with patch.object(
            sentinel_kube,
            "command",
            return_value=(True, json.dumps(duplicate)),
        ):
            ok, _pods, error = sentinel._pods("k3s")
        self.assertFalse(ok)
        self.assertEqual(error, "pod_json_invalid")

        pod["status"]["containerStatuses"][0]["restartCount"] = -1
        with patch.object(
            sentinel_kube,
            "command",
            return_value=(True, json.dumps({"items": [pod]})),
        ):
            ok, _pods, error = sentinel._pods("k3s")
        self.assertFalse(ok)
        self.assertEqual(error, "pod_json_invalid")

    def test_report_requires_explicit_false_safety_flags(self) -> None:
        for invalid_safety in ({}, [], {"real_delivery_enabled": 0}):
            payload = _report_payload()
            payload["safety_boundary"] = invalid_safety
            with self.subTest(safety=invalid_safety), tempfile.TemporaryDirectory() as td:
                path = Path(td) / "report.json"
                path.write_text(json.dumps(payload), encoding="utf-8")
                result = sentinel._report(path, now_ts=BASE_TS + 1, max_age_sec=420)
            if isinstance(invalid_safety, list):
                self.assertFalse(result["readable"])
            else:
                self.assertTrue(result["unsafe_real_delivery"])

    def test_report_rejects_numeric_string_coercion(self) -> None:
        payload = _report_payload()
        payload["parity_14d"]["invalid_payload_count"] = "0"
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "report.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            result = sentinel._report(path, now_ts=BASE_TS + 1, max_age_sec=420)
        self.assertFalse(result["readable"])

    def test_assessment_exposes_named_failed_checks_without_changing_payload_schema(self) -> None:
        revision = "a" * 40
        app_digest = "sha256:" + "b" * 64
        database_digest = "sha256:" + "742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
        pods: dict[str, dict[str, object]] = {}
        for component, count in sentinel.REQUIRED_COMPONENT_COUNTS.items():
            for index in range(count):
                name = f"{component}-{index}"
                is_database = component == "database"
                pods[name] = {
                    "uid": f"uid-{name}",
                    "component": component,
                    "phase": "Running",
                    "ready": True,
                    "images": [
                        POSTGRES_IMAGE
                        if is_database
                        else f"stream-monitoring-v4:{revision}"
                    ],
                    "image_ids": [
                        f"containerd://{database_digest}"
                        if is_database
                        else f"containerd://{app_digest}"
                    ],
                    "restart_count": 0,
                }
        previous = {
            "schema": sentinel.SENTINEL_SCHEMA,
            "release": {"revision": revision},
            "pod_runtime": {
                name: {
                    "uid": pod["uid"],
                    "component": pod["component"],
                    "restart_count": 0,
                }
                for name, pod in pods.items()
            },
        }
        report = {
            "schema_supported": True,
            "fresh": True,
            "build_revision": revision,
            "build_revision_immutable": True,
            "source_revision_known": True,
            "parity_clean": True,
            "projection_clean": True,
            "latest_cycle_clean": True,
            "unsafe_real_delivery": False,
            "unsafe_runtime_mutation": False,
            "unsafe_raspberry_pi_dependency": False,
        }
        backup = {
            "fresh": True,
            "age_sec": 60,
            "path": "/primary/stream-v4-20260811T000000Z.dump",
            "sha256": "c" * 64,
        }
        independent_backup = {
            **backup,
            "path": "/secondary/stream-v4-20260811T000000Z.dump",
        }
        filesystem = {"readable": True, "sufficient": True, "device": 1}
        evidence = SentinelEvidence(
            now_ts=BASE_TS,
            previous=previous,
            k3s_active=True,
            k3s_detail="active",
            pods_ok=True,
            pods=pods,
            pods_error="",
            release_ok=True,
            release_identity={"revision": revision, "app_image_id": app_digest},
            release_error="",
            youtube_api_evidence={
                "readable": True,
                "schema_supported": True,
                "permissions_safe": True,
                "fresh": True,
                "probe_ok": True,
            },
            report=report,
            backup=backup,
            independent_backup=independent_backup,
            restore_verification={
                "readable": True,
                "verified": True,
                "acceptable": True,
                "pending": False,
            },
            database_filesystem=filesystem,
            backup_filesystem=filesystem,
            independent_backup_filesystem={
                "readable": True,
                "sufficient": True,
                "device": 2,
            },
            minimum_free_bytes=1,
            minimum_free_percent=1.0,
            minimum_free_inode_percent=1.0,
        )

        healthy = assess(evidence)
        self.assertTrue(healthy.healthy, healthy.failed_checks)
        self.assertEqual(healthy.failed_checks, ())
        self.assertEqual(healthy.payload["status"], "good")
        self.assertNotIn("failed_checks", healthy.payload)

        bad_report = dict(report)
        bad_report["parity_clean"] = False
        unhealthy = assess(replace(evidence, report=bad_report))
        self.assertFalse(unhealthy.healthy)
        self.assertEqual(unhealthy.failed_checks, ("report_parity_clean",))
        self.assertEqual(unhealthy.payload["status"], "bad")

        rogue_auxiliary = {
            **pods,
            "rogue-backup": {
                "uid": "uid-rogue-backup",
                "component": "backup",
                "phase": "Running",
                "ready": True,
                "images": [f"stream-monitoring-v4:{revision}"],
                "image_ids": [f"containerd://{app_digest}"],
                "restart_count": 0,
            },
        }
        rogue = assess(replace(evidence, pods=rogue_auxiliary))
        self.assertIn(
            "auxiliary_image_identity_matches_release",
            rogue.failed_checks,
        )


if __name__ == "__main__":
    unittest.main()
