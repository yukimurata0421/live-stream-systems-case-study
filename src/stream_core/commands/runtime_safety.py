from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

try:
    from stream_core.diagnostics import ingest_contract, start_safety
except ModuleNotFoundError:
    from diagnostics import ingest_contract, start_safety

YOUTUBE_RTMP_HOSTS = ingest_contract.YOUTUBE_RTMP_HOSTS
PREFERRED_YOUTUBE_RTMPS_URL = ingest_contract.PREFERRED_YOUTUBE_RTMPS_URL
PLACEHOLDER_STREAM_KEYS = ingest_contract.PLACEHOLDER_STREAM_KEYS


@dataclass(frozen=True)
class RuntimeSafetyContext:
    base_dir: Path
    stream_service: str
    legacy_stream_service: str
    read_env_file: Callable[[Path], dict[str, str]]
    run: Callable[..., object]
    run_systemctl: Callable[..., object]
    is_active: Callable[[str], bool]


def parse_stream_key_from_rtmp_url(url: str) -> str:
    return ingest_contract.parse_stream_key_from_rtmp_url(url)


def stream_ingest_endpoint_status(ctx: RuntimeSafetyContext, env_path: Path = Path("/etc/default/adsb-streamnew")) -> dict:
    return ingest_contract.stream_ingest_endpoint_status(ctx.read_env_file, env_path)


def youtube_monitor_max_fails(ctx: RuntimeSafetyContext) -> int:
    cfg = ctx.read_env_file(Path("/etc/default/adsb-streamnew-youtube-monitor"))
    raw = cfg.get("YTW_MAX_FAILS", "3").strip()
    try:
        value = int(raw)
    except ValueError:
        return 3
    return max(1, value)


def youtube_watchdog_state_path(ctx: RuntimeSafetyContext) -> Path:
    cfg = ctx.read_env_file(Path("/etc/default/adsb-streamnew-youtube-monitor"))
    state_file = cfg.get("YTW_STATE_FILE", "").strip()
    if state_file:
        return Path(state_file)
    runtime_root = cfg.get("STREAM_RUNTIME_STATE_DIR", "").strip()
    if runtime_root:
        return Path(runtime_root) / "youtube_watchdog_state.json"
    return ctx.base_dir / ".state" / "adsb-streamnew-v2" / "youtube_watchdog_state.json"


def youtube_watchdog_unhealthy(*, state_path: Path, max_fails: int) -> bool:
    if not state_path.exists():
        return False
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    fail_count = int(payload.get("fail_count", 0))
    if fail_count < max_fails:
        return False
    reason = str(payload.get("last_reason", "")).lower()
    markers = (
        "livebroadcastcontent=none",
        "islivenow=true not found",
        "no active live video id",
    )
    return any(marker in reason for marker in markers)


def expected_stream_key(ctx: RuntimeSafetyContext) -> str:
    cfg = ctx.read_env_file(Path("/etc/default/adsb-streamnew"))
    key = cfg.get("STREAM_KEY", "").strip()
    if key:
        return key
    return parse_stream_key_from_rtmp_url(cfg.get("RTMP_URL", ""))


def default_stream1090_upstream_url(ctx: RuntimeSafetyContext) -> str:
    cfg = ctx.read_env_file(Path("/etc/default/adsb-streamnew"))
    return cfg.get("STREAM1090_URL", "").strip() or "http://stream1090.lan/stream1090/"


def split_url_root_and_path(url: str, default_path: str = "/") -> tuple[str, str]:
    raw = (url or "").strip()
    if not raw:
        return "", default_path
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.rstrip("/"), default_path
    root = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or default_path
    if not path.startswith("/"):
        path = "/" + path
    return root, path


def stream_main_pid(ctx: RuntimeSafetyContext, unit: str) -> int:
    cp = ctx.run_systemctl(["show", unit, "--property=MainPID", "--value"], check=False)
    if cp.returncode != 0:
        return 0
    raw = (cp.stdout or "").strip()
    if not raw:
        return 0
    try:
        pid = int(raw)
    except ValueError:
        return 0
    return pid if pid > 1 else 0


def stream_ffmpeg_pid(ctx: RuntimeSafetyContext, main_pid: int) -> int:
    if main_pid <= 1:
        return 0
    cp = ctx.run(["pgrep", "-P", str(main_pid), "ffmpeg"], check=False)
    if cp.returncode != 0:
        return 0
    for line in (cp.stdout or "").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            pid = int(text)
        except ValueError:
            continue
        if pid > 1:
            return pid
    return 0


def running_stream_key(ctx: RuntimeSafetyContext, *, stream_main_pid_func: Callable[[str], int], stream_ffmpeg_pid_func: Callable[[int], int]) -> str:
    main_pid = stream_main_pid_func(ctx.stream_service)
    ffmpeg_pid = stream_ffmpeg_pid_func(main_pid)
    if ffmpeg_pid <= 1:
        return ""
    cmdline_path = Path(f"/proc/{ffmpeg_pid}/cmdline")
    try:
        raw = cmdline_path.read_bytes()
    except OSError:
        return ""
    for part in raw.split(b"\x00"):
        if not part:
            continue
        try:
            arg = part.decode("utf-8", errors="ignore")
        except Exception:
            continue
        key = parse_stream_key_from_rtmp_url(arg)
        if key:
            return key
    return ""


def guard_start_safety(ctx: RuntimeSafetyContext, *, stream_ingest_status_func: Callable[[Path], dict] | None = None) -> int:
    env_path = Path("/etc/default/adsb-streamnew")
    ingest_status = stream_ingest_status_func or (lambda path: stream_ingest_endpoint_status(ctx, path))
    results = start_safety.start_safety_results(
        read_env_file=ctx.read_env_file,
        is_active=ctx.is_active,
        legacy_stream_service=ctx.legacy_stream_service,
        stream_ingest_status=ingest_status,
        env_path=env_path,
    )
    failed = [item for item in results if item.fatal]
    if not failed:
        warnings = [item for item in results if item.severity == "warn"]
        for item in warnings:
            print(f"[warn] {item.summary}")
        return 0
    for item in failed:
        print(f"[error] {item.summary}")
        if item.detail:
            print(f"[hint] {item.detail}")
    return 1
