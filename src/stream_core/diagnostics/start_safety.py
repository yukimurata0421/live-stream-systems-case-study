from __future__ import annotations

from pathlib import Path
from typing import Callable

from .ingest_contract import PLACEHOLDER_STREAM_KEYS, ingest_result
from .model import CheckResult


def start_safety_results(
    *,
    read_env_file: Callable[[Path], dict[str, str]],
    is_active: Callable[[str], bool],
    legacy_stream_service: str,
    stream_ingest_status: Callable[[Path], dict],
    env_path: Path = Path("/etc/default/adsb-streamnew"),
) -> list[CheckResult]:
    cfg = read_env_file(env_path)
    stream_key = cfg.get("STREAM_KEY", "")
    display_name = cfg.get("DISPLAY_NAME", cfg.get("DISPLAY", ":99"))
    test_mode = cfg.get("TEST_MODE", "0").strip() == "1"
    results: list[CheckResult] = []

    ingest = stream_ingest_status(env_path)
    ingest_check = ingest_result(ingest)
    if test_mode:
        ingest_check = CheckResult(
            name=ingest_check.name,
            category=ingest_check.category,
            severity="info" if not ingest_check.ok else ingest_check.severity,
            ok=True,
            fatal=False,
            summary=f"{ingest_check.summary}; ignored because TEST_MODE=1",
            detail=ingest_check.detail,
            path=ingest_check.path,
            data=ingest_check.data,
        )
    results.append(ingest_check)

    placeholder = stream_key in PLACEHOLDER_STREAM_KEYS
    if placeholder and not test_mode:
        results.append(
            CheckResult(
                name="start_safety:stream_key_configured",
                category="start_safety",
                severity="fail",
                ok=False,
                fatal=True,
                summary="/etc/default/adsb-streamnew has placeholder STREAM_KEY and TEST_MODE=0",
                detail="set real STREAM_KEY or set TEST_MODE=1 before start",
                path=str(env_path),
            )
        )
    else:
        results.append(
            CheckResult(
                name="start_safety:stream_key_configured",
                category="start_safety",
                severity="ok" if not placeholder else "info",
                ok=True,
                fatal=False,
                summary="STREAM_KEY is configured" if not placeholder else "placeholder STREAM_KEY allowed in TEST_MODE=1",
                path=str(env_path),
            )
        )

    legacy_conflict = is_active(legacy_stream_service) and display_name.startswith(":99") and not test_mode
    results.append(
        CheckResult(
            name="start_safety:legacy_display_conflict",
            category="start_safety",
            severity="fail" if legacy_conflict else "ok",
            ok=not legacy_conflict,
            fatal=legacy_conflict,
            summary=(
                "legacy stream is active and stream-new is configured to use :99 in production mode"
                if legacy_conflict
                else "legacy display conflict not present"
            ),
            detail="stop legacy first, or use TEST_MODE=1 with a different display such as :101" if legacy_conflict else "",
        )
    )
    return results
