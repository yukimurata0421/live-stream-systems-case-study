from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .jsonio import iter_jsonl, latest_jsonl, read_json


@dataclass(frozen=True)
class RuntimeInputs:
    source_state_root: Path
    youtube_watchdog_stats: dict[str, Any]
    youtube_video_id_resolver_state: dict[str, Any]
    stream_watchdog_stats: dict[str, Any]
    latest_runtime_state: dict[str, Any]
    restart_reason: dict[str, Any]
    slo_snapshot: dict[str, Any]
    api_cost_latest: dict[str, Any]
    fast_recovery_state: dict[str, Any]
    latest_stream_watchdog_event: dict[str, Any]
    latest_watchdog_timeline_event: dict[str, Any]
    latest_fast_recovery_event: dict[str, Any]
    latest_fast_recovery_restart_event: dict[str, Any]
    latest_youtube_watchdog_event: dict[str, Any]
    latest_stream1090_report: dict[str, Any]
    latest_upstream_stream1090_report: dict[str, Any]
    latest_stream_engine_event: dict[str, Any]
    latest_play_history_event: dict[str, Any]
    pulse_health_state: dict[str, Any]
    adsb_freshness_state: dict[str, Any]
    recovery_stage_state: dict[str, Any]
    overlay_now_playing: dict[str, Any]
    audio_fail_count: int
    pulse_source_missing_count: int


class SourceReader:
    """Read production runtime state as input only.

    This class intentionally has no write methods. The v2 aggregator is the
    single writer for v2 subsystem snapshots.
    """

    def __init__(self, source_state_root: Path):
        self.source_state_root = source_state_root

    def read(self) -> RuntimeInputs:
        root = self.source_state_root
        logs = root / "logs"
        return RuntimeInputs(
            source_state_root=root,
            youtube_watchdog_stats=read_json(root / "youtube_watchdog_stats.json") or {},
            youtube_video_id_resolver_state=read_json(root / "youtube_video_id_resolver_state.json") or {},
            stream_watchdog_stats=read_json(root / "stream_watchdog_stats.json") or {},
            latest_runtime_state=self._latest_runtime_state(),
            restart_reason=read_json(root / "restart_reason.json") or {},
            slo_snapshot=read_json(root / "slo_snapshot.json") or {},
            api_cost_latest=read_json(root / "reports" / "youtube_api_cost" / "open_day_latest.json") or {},
            fast_recovery_state=read_json(root / "fast_recovery_state.json") or {},
            latest_stream_watchdog_event=latest_jsonl(logs / "stream_watchdog_events.jsonl") or {},
            latest_watchdog_timeline_event=latest_jsonl(logs / "watchdog_state_timeline.jsonl") or {},
            latest_fast_recovery_event=latest_jsonl(logs / "fast_recovery_events.jsonl") or {},
            latest_fast_recovery_restart_event=self._latest_jsonl_where(
                logs / "fast_recovery_events.jsonl",
                lambda item: item.get("kind") == "restart",
            ),
            latest_youtube_watchdog_event=latest_jsonl(logs / "youtube_watchdog.jsonl") or {},
            latest_stream1090_report=latest_jsonl(logs / "stream1090_report.jsonl") or {},
            latest_upstream_stream1090_report=latest_jsonl(logs / "upstream_stream1090_report.jsonl") or {},
            latest_stream_engine_event=latest_jsonl(logs / "stream_engine_events.jsonl") or {},
            latest_play_history_event=latest_jsonl(logs / "play_history.jsonl") or {},
            pulse_health_state=read_json(root / "watchdog" / "pulse_health_state.json") or {},
            adsb_freshness_state=read_json(root / "watchdog" / "adsb_freshness_state.json") or {},
            recovery_stage_state=read_json(root / "watchdog" / "recovery_stage_state.json") or {},
            overlay_now_playing=self._first_json(
                [
                    root / "ui" / "overlay" / "now_playing.json",
                    root / "overlay" / "now_playing.json",
                    root / "now_playing.json",
                ]
            ),
            audio_fail_count=self._read_int(root / "watchdog" / "audio_fail_count"),
            pulse_source_missing_count=self._read_int(root / "watchdog" / "pulse_source_missing_count"),
        )

    def _latest_runtime_state(self) -> dict[str, Any]:
        candidates = sorted(
            self.source_state_root.glob("stream_runtime_state_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        running: list[tuple[Path, dict[str, Any]]] = []
        loaded: list[tuple[Path, dict[str, Any]]] = []
        for path in candidates:
            payload = read_json(path) or {}
            if not payload:
                continue
            loaded.append((path, payload))
            if str(payload.get("status", "")).lower() == "running":
                running.append((path, payload))
        chosen: Optional[tuple[Path, dict[str, Any]]] = None
        if running:
            chosen = running[0]
        elif loaded:
            chosen = loaded[0]
        if not chosen:
            return {}
        path, payload = chosen
        payload = dict(payload)
        payload.setdefault("_source_file", str(path))
        return payload

    def _first_json(self, paths: Iterable[Path]) -> dict[str, Any]:
        for path in paths:
            payload = read_json(path)
            if payload:
                payload = dict(payload)
                payload.setdefault("_source_file", str(path))
                return payload
        return {}

    def _read_int(self, path: Path, *, default: int = 0) -> int:
        try:
            raw = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    def _latest_jsonl_where(self, path: Path, predicate: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
        latest: dict[str, Any] = {}
        for item in iter_jsonl(path):
            if predicate(item):
                latest = item
        return latest
