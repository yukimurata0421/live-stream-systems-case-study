from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import stream_engine  # type: ignore  # noqa: E402
from stream_core.engine import rendering_boot  # noqa: E402


class Mp10CandidateTests(unittest.TestCase):
    def test_stream_engine_audit_binds_projection_and_p2_disabled_evaluator(self) -> None:
        engine = object.__new__(stream_engine.StreamEngine)
        engine.run_id = "run-fixture"
        engine.restart_count = 7

        with mock.patch.object(stream_engine, "audit_maintenance_decision") as audit:
            engine.audit_self_recovery(
                phase="EFFECT_BOUNDARY",
                operation="terminate_ffmpeg",
                resource="ffmpeg/pid/4100",
                correlation_id="native-fixture",
                count=1,
            )

        kwargs = audit.call_args.kwargs
        self.assertEqual(kwargs["path_id"], "MP-10")
        self.assertTrue(kwargs["bind_source_target"])
        self.assertTrue(kwargs["p2_disabled_evaluation"])
        self.assertEqual(kwargs["native_operation_id"], "native-fixture")
        self.assertEqual(kwargs["native_operation_generation"], "run-fixture:7")
        self.assertEqual(kwargs["actual_production_decision"], "LEGACY_EFFECT_CALL_PROCEEDS")

    def test_helper_popen_effect_boundary_is_adjacent_and_non_interfering(self) -> None:
        order: list[str] = []

        def audit(**kwargs: object) -> None:
            order.append(f"audit:{kwargs['phase']}")
            if kwargs["phase"] == "EFFECT_BOUNDARY":
                raise RuntimeError("audit failure")

        def popen(*_args: object, **_kwargs: object) -> mock.Mock:
            order.append("effect:Popen")
            return mock.Mock()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = SimpleNamespace(
                use_overlay_wrapper=True,
                overlay_port=18080,
                overlay_server_log_file=root / "overlay.log",
                base_dir=ROOT,
                overlay_bind_host="127.0.0.1",
                overlay_dir=ROOT / "ui" / "overlay",
                stream1090_url="http://127.0.0.1:1090/",
            )
            with (
                mock.patch.object(rendering_boot, "is_port_listening", return_value=False),
                mock.patch.object(rendering_boot, "audit_maintenance_decision", side_effect=audit),
                mock.patch.object(rendering_boot.subprocess, "Popen", side_effect=popen),
                mock.patch.object(rendering_boot.time, "sleep", return_value=None),
            ):
                rendering_boot.start_overlay_server(cfg)

        self.assertEqual(order, ["audit:ADMISSION", "audit:EFFECT_BOUNDARY", "effect:Popen"])

    def test_rendering_audit_requests_projection_binding_without_mutation_signal(self) -> None:
        with mock.patch.object(rendering_boot, "audit_maintenance_decision") as audit:
            rendering_boot._audit_start(
                phase="EFFECT_BOUNDARY",
                operation="start_browser",
                resource_identity="browser/profile/test",
                correlation_id="helper-fixture",
                count=1,
            )

        kwargs = audit.call_args.kwargs
        self.assertTrue(kwargs["bind_source_target"])
        self.assertTrue(kwargs["p2_disabled_evaluation"])
        self.assertEqual(kwargs["actual_production_decision"], "LEGACY_EFFECT_CALL_PROCEEDS")
        self.assertNotIn("production_branch_signal", kwargs)


if __name__ == "__main__":
    unittest.main()
