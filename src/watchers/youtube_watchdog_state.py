from __future__ import annotations

import json
import hashlib
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar

try:
    import fcntl
except Exception:  # pragma: no cover
    fcntl = None

try:
    from .youtube_watchdog_config import (
        LOG_FILE,
        OAUTH_TOKEN_STATE_FILE,
        OK_HEARTBEAT_FILE,
        OK_LOG_EVERY_SEC,
        QUOTA_STATE_FILE,
        RESTART_REASON_FILE,
        STATS_FILE,
        STATE_FILE,
        FORCE_LIVE_STATE_FILE,
        VIDEO_RESOLVER_STATE_FILE,
    )
except ImportError:
    from youtube_watchdog_config import (
        LOG_FILE,
        OAUTH_TOKEN_STATE_FILE,
        OK_HEARTBEAT_FILE,
        OK_LOG_EVERY_SEC,
        QUOTA_STATE_FILE,
        RESTART_REASON_FILE,
        STATS_FILE,
        STATE_FILE,
        FORCE_LIVE_STATE_FILE,
        VIDEO_RESOLVER_STATE_FILE,
    )


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_iso_ts(raw: str) -> int:
    text = str(raw or "").strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except Exception:
        return 0


def _latest_iso_ts(*values: str) -> str:
    best_raw = ""
    best_ts = 0
    for raw in values:
        text = str(raw or "").strip()
        ts = _parse_iso_ts(text)
        if ts > best_ts:
            best_ts = ts
            best_raw = text
    return best_raw


def _derive_remote_source(payload: dict) -> str:
    existing = str(payload.get("remote_source", "") or payload.get("remote_probe_source", "") or "").strip()
    if existing:
        return existing
    has_oauth = bool(str(payload.get("oauth_checked_ts_utc", "") or "").strip())
    has_data_api = bool(str(payload.get("data_api_checked_ts_utc", "") or "").strip())
    if has_oauth and has_data_api:
        return "data_api_oauth"
    if has_oauth:
        return "oauth_api"
    if has_data_api:
        return "data_api_videos"
    return ""


def _derive_ffmpeg_generation(payload: dict) -> str:
    existing = str(payload.get("ffmpeg_generation", "") or "").strip()
    if existing:
        return existing
    try:
        ffmpeg_pid = int(payload.get("ffmpeg_pid", 0) or 0)
    except (TypeError, ValueError):
        ffmpeg_pid = 0
    return f"ffmpeg_pid={ffmpeg_pid}" if ffmpeg_pid > 0 else ""


def enrich_remote_probe_fields(payload: dict) -> dict:
    out = dict(payload)
    remote_probe_ts_utc = str(out.get("remote_probe_ts_utc", "") or "").strip()
    if not remote_probe_ts_utc:
        remote_probe_ts_utc = _latest_iso_ts(
            str(out.get("oauth_checked_ts_utc", "") or ""),
            str(out.get("data_api_checked_ts_utc", "") or ""),
        )
    remote_source = _derive_remote_source(out)
    ffmpeg_generation = _derive_ffmpeg_generation(out)
    recovery_episode_id = str(out.get("recovery_episode_id", "") or "").strip()
    if remote_probe_ts_utc:
        out.setdefault("remote_probe_ts_utc", remote_probe_ts_utc)
    if remote_source:
        out.setdefault("remote_probe_source", remote_source)
        out.setdefault("remote_sample_source", remote_source)
        out.setdefault("remote_source", remote_source)
    if ffmpeg_generation:
        out.setdefault("ffmpeg_generation", ffmpeg_generation)
    if "recovery_episode_id" not in out:
        out["recovery_episode_id"] = recovery_episode_id
    if remote_probe_ts_utc and remote_source and not str(out.get("remote_sample_id", "") or "").strip():
        raw = "|".join(
            [
                remote_probe_ts_utc,
                remote_source,
                recovery_episode_id,
                ffmpeg_generation,
                str(out.get("video_id", "") or "").strip(),
            ]
        )
        out["remote_sample_id"] = "rps-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return out


def _tmp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")


@contextmanager
def _file_lock(path: Path):
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_fh:
        if fcntl is not None:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


def _load_json_file_unlocked(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return {}


def _write_json_file_unlocked(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)


def append_event(payload: dict) -> None:
    payload = {
        "ts_utc": utc_now(),
        "event_id": f"evt-ytw-{int(time.time())}-{os.getpid()}-{uuid.uuid4().hex[:8]}",
        **payload,
    }
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception as e:
        log(f"WARN failed to append log file: {e}")


def _write_json_file(path: str, payload: dict) -> None:
    p = Path(path)
    with _file_lock(p):
        _write_json_file_unlocked(p, payload)


def should_emit_ok_event(now_ts: int | None = None) -> bool:
    if OK_LOG_EVERY_SEC <= 0:
        return True
    current = int(time.time()) if now_ts is None else int(now_ts)
    last_ok_ts = 0
    try:
        with open(OK_HEARTBEAT_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                last_ok_ts = int(data.get("last_ok_event_ts", 0) or 0)
    except Exception:
        last_ok_ts = 0
    if last_ok_ts > 0 and current - last_ok_ts < OK_LOG_EVERY_SEC:
        return False
    _write_json_file(OK_HEARTBEAT_FILE, {"last_ok_event_ts": current, "updated_at_utc": utc_now()})
    return True


def classify_judgment(status: str, healthy: bool) -> tuple[str, str]:
    normalized = (status or "").strip().lower()
    if normalized == "ok" and healthy:
        return "ok", "availability_healthy"
    if normalized in {"startup_grace", "degraded_public", "quota_guard"}:
        return "deferred", "non_actionable_observation"
    if normalized in {"warn", "restart"}:
        return "ng", "availability_unhealthy"
    if healthy:
        return "deferred", "healthy_non_final_state"
    return "ng", "unhealthy_non_final_state"


def write_stats(payload: dict) -> None:
    stats_file_updated_at = utc_now()
    payload = enrich_remote_probe_fields(payload)
    _write_json_file(
        STATS_FILE,
        {
            **payload,
            "ts_utc": stats_file_updated_at,
            "stats_file_updated_at_utc": stats_file_updated_at,
        },
    )


def update_stats(payload: dict) -> None:
    path = Path(STATS_FILE)
    with _file_lock(path):
        current = _load_json_file_unlocked(path)
        stats_file_updated_at = utc_now()
        current.update(enrich_remote_probe_fields(payload))
        _write_json_file_unlocked(
            path,
            {
                **current,
                "ts_utc": stats_file_updated_at,
                "stats_file_updated_at_utc": stats_file_updated_at,
            },
        )


def load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "fail_count": 0,
            "degraded_public_count": 0,
            "last_reason": "",
            "last_video_id_ts": 0,
            "last_restart_ts": 0,
            "restart_history_ts": [],
        }


def save_state(state: dict) -> None:
    p = Path(STATE_FILE)
    with _file_lock(p):
        _write_json_file_unlocked(p, state)


def load_oauth_token_state() -> dict:
    path = Path(OAUTH_TOKEN_STATE_FILE)
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return {}


def save_oauth_token_state(state: dict) -> None:
    path = Path(OAUTH_TOKEN_STATE_FILE)
    with _file_lock(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = _tmp_path(path)
        tmp.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        os.chmod(tmp, 0o600)
        tmp.replace(path)
        os.chmod(path, 0o600)


def load_quota_state() -> dict:
    path = Path(QUOTA_STATE_FILE)
    with _file_lock(path):
        return _load_json_file_unlocked(path)


def save_quota_state(state: dict) -> None:
    path = Path(QUOTA_STATE_FILE)
    with _file_lock(path):
        _write_json_file_unlocked(path, state)


T = TypeVar("T")


def update_quota_state(updater: Callable[[dict], tuple[dict, T]]) -> T:
    path = Path(QUOTA_STATE_FILE)
    with _file_lock(path):
        state = _load_json_file_unlocked(path)
        next_state, result = updater(dict(state))
        _write_json_file_unlocked(path, next_state)
        return result


def quota_exhausted_active(now_ts: int | None = None) -> tuple[bool, dict]:
    state = load_quota_state()
    active = bool(state.get("quota_exhausted", False))
    if not active:
        return False, state

    current = int(time.time()) if now_ts is None else int(now_ts)
    until_ts = int(state.get("quota_exhausted_until_ts", 0) or 0)
    if until_ts > 0 and current >= until_ts:
        state["quota_exhausted"] = False
        state["quota_exhausted_cleared_at_utc"] = utc_now()
        save_quota_state(state)
        return False, state
    return True, state


def load_force_live_state() -> dict:
    path = Path(FORCE_LIVE_STATE_FILE)
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return {}


def save_force_live_state(state: dict) -> None:
    path = Path(FORCE_LIVE_STATE_FILE)
    with _file_lock(path):
        _write_json_file_unlocked(path, state)


def write_restart_reason(component: str, reason: str, unit: str) -> None:
    payload = {
        "ts_utc": utc_now(),
        "source": "youtube_watchdog",
        "event_id": f"evt-ytw-restart-{int(time.time())}-{uuid.uuid4().hex[:8]}",
        "component": component,
        "reason": reason,
        "target_unit": unit,
    }
    rr = Path(RESTART_REASON_FILE)
    rr.parent.mkdir(parents=True, exist_ok=True)
    rr.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def load_video_resolver_state() -> dict:
    path = Path(VIDEO_RESOLVER_STATE_FILE)
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return {}


def save_video_resolver_state(state: dict) -> None:
    path = Path(VIDEO_RESOLVER_STATE_FILE)
    with _file_lock(path):
        _write_json_file_unlocked(path, state)
