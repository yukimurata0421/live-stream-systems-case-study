from __future__ import annotations

import ast
from pathlib import Path
from unittest import mock

from watchers import fast_recovery


def test_audit_helper_result_is_never_used_as_a_branch_signal() -> None:
    source_path = Path(fast_recovery.__file__).resolve()
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    helper = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "audit_mp03")
    audit_calls = [
        node
        for node in ast.walk(helper)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "audit_maintenance_decision"
    ]
    assert len(audit_calls) == 1
    assert isinstance(next(node for node in helper.body if isinstance(node, ast.Expr)), ast.Expr)
    assert not any(isinstance(node, ast.Return) and node.value is not None for node in ast.walk(helper))


def test_would_block_audit_cannot_block_legacy_ffmpeg_dispatch() -> None:
    with (
        mock.patch.object(fast_recovery, "audit_mp03") as audit,
        mock.patch.object(fast_recovery, "k8s_supervisor_active", return_value=True),
        mock.patch.object(fast_recovery, "restart_ffmpeg_child", return_value=(True, "legacy-ok")) as restart,
    ):
        ok, detail, scope = fast_recovery.execute_recovery_action(
            reason_kind="tcp_stall",
            reason="fixture",
            ffmpeg_pid=4100,
            correlation_id="native-r1",
        )
    assert (ok, detail, scope) == (True, "legacy-ok", "ffmpeg_child")
    audit.assert_called_once()
    restart.assert_called_once_with(4100, "fixture", correlation_id="native-r1")


def test_audit_exception_cannot_stop_legacy_runtime_dispatch() -> None:
    with (
        mock.patch.object(fast_recovery, "audit_maintenance_decision", side_effect=RuntimeError("injected")),
        mock.patch.object(fast_recovery, "k8s_supervisor_active", return_value=False),
        mock.patch.object(fast_recovery, "restart_stream", return_value=(True, "legacy-ok")) as restart,
    ):
        result = fast_recovery.execute_recovery_action(
            reason_kind="network_down",
            reason="fixture",
            ffmpeg_pid=4100,
            correlation_id="native-r1",
        )
    assert result == (True, "legacy-ok", "runtime")
    restart.assert_called_once_with("fixture", correlation_id="native-r1")
