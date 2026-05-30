from __future__ import annotations

from pathlib import Path

from .model import CheckResult


DEFAULT_NEEDRESTART_CONTRACT = Path("/etc/needrestart/conf.d/stream-24x7.conf")


def needrestart_contract_status(path: Path = DEFAULT_NEEDRESTART_CONTRACT) -> dict:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        return {"ok": False, "path": str(path), "reason": f"missing_or_unreadable:{e}"}
    has_stream = "adsb-streamnew-" in text and "youtube-stream" in text and "auto-dj" in text
    disables = "= 0" in text or "=0" in text
    has_override = "$nrconf{override_rc}" in text
    ok = has_stream and disables and has_override
    return {
        "ok": ok,
        "path": str(path),
        "has_stream_units": has_stream,
        "has_override_rc": has_override,
        "disables_restart": disables,
        "reason": "ok" if ok else "needrestart stream override is incomplete",
    }


def needrestart_result(path: Path = DEFAULT_NEEDRESTART_CONTRACT) -> CheckResult:
    status = needrestart_contract_status(path)
    return needrestart_result_from_status(status)


def needrestart_result_from_status(status: dict) -> CheckResult:
    return CheckResult(
        name="needrestart:stream_override",
        category="needrestart_contract",
        severity="ok" if status["ok"] else "fail",
        ok=bool(status["ok"]),
        fatal=not bool(status["ok"]),
        summary=f"needrestart contract: {status['path']}" if status["ok"] else f"needrestart contract: {status['reason']}",
        path=str(status.get("path", "")),
        data=status,
    )
