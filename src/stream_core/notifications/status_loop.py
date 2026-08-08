from __future__ import annotations

import calendar
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.json_io import append_jsonl, iter_jsonl, read_json_file
    from stream_core.common.timeutil import parse_utc_ts, utc_now_text
    from stream_core.notifications import outbox as notify_outbox
except ModuleNotFoundError:
    from common.json_io import append_jsonl, iter_jsonl, read_json_file
    from common.timeutil import parse_utc_ts, utc_now_text
    from notifications import outbox as notify_outbox


@dataclass(frozen=True)
class NotifyStatusContext:
    notify_events_file: Path
    notify_outbox_file: Path
    load_config: Callable[[], dict]
    load_state: Callable[[], dict]
    save_state: Callable[[dict], None]
    collect_incidents: Callable[..., list[dict]]
    recovery_observation_for_incident: Callable[[str, int], tuple[int, str]]
    format_message: Callable[..., str]
    send_webhook: Callable[..., tuple[bool, str]]
    maintenance_notification_incident: Callable[[int], dict | None]
    fast_recovery_events_file: Path
    stream_engine_events_file: Path
    stream_watchdog_events_file: Path
    runtime_state_base_dir: Path
    send_slack_webhook: Callable[..., tuple[bool, str]] | None = None


@dataclass(frozen=True)
class RecoveryEvidence:
    restart_observed: bool
    recovery_confirmed: bool
    recovery_lag_sec: int | None
    diagnostic_context: str


SLACK_EVENT_PHASES = {"restart_observed", "recovery_unconfirmed", "auto_recovered"}
SLACK_ACTIVE_PHASES = {"detected", "status"}
SLACK_ALLOW_EMPTY_PHASES = {"test"}
SLACK_IMMEDIATE_COMPONENTS = {
    "stream_health",
    "youtube_remote_observability",
    "youtube_encoder_viewer_state",
    "fast_recovery_remote_warning",
    "rtmp_ffmpeg_transport",
    "rtmps_ingest_tls",
    "fast_recovery",
    "stream_engine",
    "k8s_runtime_container",
    "runtime_lifecycle",
    "nvidia_gpu",
    "gpu_runtime",
}
SLACK_IMMEDIATE_ID_PREFIXES = (
    "observe:",
    "stream:",
    "youtube:current_",
    "youtube:enable_auto_stop_false",
    "fast_recovery:",
    "ffmpeg:",
    "rtmps:",
    "runtime:",
    "k8s:",
    "gpu:",
    "nvidia:",
)


def fast_recovery_recovery_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    triggers: list[str],
    events_file: Path,
    max_events: int = 8,
    confirmation_wait_sec: int = 180,
) -> list[dict]:
    trigger_set = {str(item).strip() for item in triggers if str(item).strip()}
    if not trigger_set or not events_file.exists():
        return []

    acknowledged = state.get("fast_recovery_recovery_evidence_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}
    legacy_acknowledged = state.get("fast_recovery_auto_recovered_notified")
    if not isinstance(legacy_acknowledged, dict):
        legacy_acknowledged = {}

    cutoff = int(now_ts) - max(60, int(recent_sec or 0))
    context_cutoff = cutoff - 300
    context_end = int(now_ts) + 60
    tcp_samples: list[tuple[int, dict]] = []
    restart_candidates: list[tuple[int, str, str, dict]] = []
    for item in iter_jsonl(events_file):
        event_ts = parse_utc_ts(str(item.get("ts_utc", "")))
        if event_ts > 0 and context_cutoff <= event_ts <= context_end and str(item.get("kind", "")) == "tcp_send_sample":
            tcp_samples.append((event_ts, item))
        if str(item.get("kind", "")) != "restart":
            continue
        trigger = str(item.get("trigger", "")).strip()
        if trigger not in trigger_set:
            continue
        if event_ts <= 0 or event_ts < cutoff or event_ts > int(now_ts) + 60:
            continue
        key = f"{item.get('ts_utc')}|{trigger}"
        if legacy_acknowledged.get(key):
            continue
        restart_candidates.append((event_ts, trigger, key, item))

    events: list[dict] = []
    for event_ts, trigger, key, item in restart_candidates:
        recovery = fast_recovery_restart_evidence(event_ts, tcp_samples)
        if recovery.recovery_confirmed:
            phase = "auto_recovered"
            severity = "info"
            summary = "stream service restart and TCP send recovery confirmed"
        elif (int(now_ts) - event_ts) < max(1, int(confirmation_wait_sec)):
            phase = "restart_observed"
            severity = "info"
            summary = "stream service restart observed; TCP send recovery pending"
        else:
            phase = "recovery_unconfirmed"
            severity = "warning"
            summary = "stream service restart observed; TCP send recovery unconfirmed"

        notification_key = f"{key}|{phase}"
        if acknowledged.get(notification_key):
            continue

        evidence = str(item.get("message", "") or item.get("reason", "") or f"trigger={trigger}")
        events.append(
            {
                "id": f"fast_recovery:{phase}:{trigger}:{int(event_ts)}",
                "severity": severity,
                "component": "fast_recovery",
                "summary": summary,
                "evidence": evidence[:240],
                "recovery_type": f"fast_recovery_restart:{trigger}",
                "follow_up": "次回 routine check で同じ時間帯・trigger が再発していないか確認する",
                "observed_ts": int(event_ts),
                "trigger": trigger,
                "_event_key": key,
                "_notification_phase": phase,
                "restart_observed": recovery.restart_observed,
                "recovery_confirmed": recovery.recovery_confirmed,
                "recovery_lag_sec": recovery.recovery_lag_sec,
                "diagnostic_context": recovery.diagnostic_context,
            }
        )

    events.sort(key=lambda event: int(event.get("observed_ts", 0) or 0))
    return events[-max(1, int(max_events)) :]


def fast_recovery_auto_recovered_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    triggers: list[str],
    events_file: Path,
    max_events: int = 8,
) -> list[dict]:
    return [
        event
        for event in fast_recovery_recovery_events(
            state=state,
            now_ts=now_ts,
            recent_sec=recent_sec,
            triggers=triggers,
            events_file=events_file,
            max_events=max_events,
        )
        if event.get("_notification_phase") == "auto_recovered"
    ]


def _tcp_sample_number(item: dict, key: str) -> str:
    value = item.get(key, "")
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def compact_tcp_send_sample(label: str, event_ts: int, sample_ts: int, item: dict) -> str:
    offset = sample_ts - event_ts
    sign = "+" if offset >= 0 else ""
    return (
        f"{label}={sign}{offset}s "
        f"mbps={_tcp_sample_number(item, 'mbps')} "
        f"delta={_tcp_sample_number(item, 'bytes_sent_delta')} "
        f"lastsnd={_tcp_sample_number(item, 'lastsnd_ms')} "
        f"notsent={_tcp_sample_number(item, 'notsent')} "
        f"unacked={_tcp_sample_number(item, 'unacked')} "
        f"pid={_tcp_sample_number(item, 'ffmpeg_pid')}"
    )


def fast_recovery_restart_evidence(
    event_ts: int,
    tcp_samples: list[tuple[int, dict]],
    *,
    before_sec: int = 180,
    after_sec: int = 300,
    recovered_mbps: float = 4.5,
) -> str:
    before = [
        (ts, item)
        for ts, item in tcp_samples
        if event_ts - max(1, before_sec) <= ts <= event_ts and str(item.get("kind", "")) == "tcp_send_sample"
    ]
    after = [
        (ts, item)
        for ts, item in tcp_samples
        if event_ts < ts <= event_ts + max(1, after_sec) and str(item.get("kind", "")) == "tcp_send_sample"
    ]
    parts: list[str] = []
    if before:
        ts, item = before[-1]
        parts.append(compact_tcp_send_sample("pre", event_ts, ts, item))
    recovered = []
    for ts, item in after:
        try:
            mbps = float(item.get("mbps", 0) or 0)
        except Exception:
            mbps = 0.0
        if mbps >= recovered_mbps:
            recovered.append((ts, item))
            break
    if recovered:
        ts, item = recovered[0]
        parts.append(compact_tcp_send_sample("post", event_ts, ts, item))
        parts.append(f"recovery_lag_sec={ts - event_ts}")
        return RecoveryEvidence(
            restart_observed=True,
            recovery_confirmed=True,
            recovery_lag_sec=ts - event_ts,
            diagnostic_context="; ".join(parts)[:320],
        )
    elif after:
        ts, item = after[0]
        parts.append(compact_tcp_send_sample("post_first", event_ts, ts, item))
        parts.append("recovery_lag_sec=unknown")
    if not parts:
        context = "tcp_sample_context=missing"
    else:
        context = "; ".join(parts)[:320]
    return RecoveryEvidence(
        restart_observed=True,
        recovery_confirmed=False,
        recovery_lag_sec=None,
        diagnostic_context=context,
    )


def fast_recovery_restart_diagnostic_context(
    event_ts: int,
    tcp_samples: list[tuple[int, dict]],
    *,
    before_sec: int = 180,
    after_sec: int = 300,
    recovered_mbps: float = 4.5,
) -> str:
    return fast_recovery_restart_evidence(
        event_ts,
        tcp_samples,
        before_sec=before_sec,
        after_sec=after_sec,
        recovered_mbps=recovered_mbps,
    ).diagnostic_context


def mark_fast_recovery_recovery_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    acknowledged = state.get("fast_recovery_recovery_evidence_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - 86400
    compacted: dict[str, int] = {}
    for key, value in acknowledged.items():
        try:
            ts = int(value or 0)
        except Exception:
            ts = 0
        if ts >= cutoff:
            compacted[str(key)] = ts

    for event in events:
        key = str(event.get("_event_key", "") or "")
        phase = str(event.get("_notification_phase", "") or "")
        if key:
            compacted[f"{key}|{phase}"] = int(now_ts)
    state["fast_recovery_recovery_evidence_notified"] = compacted


def mark_fast_recovery_auto_recovered_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    normalized = [
        {**event, "_notification_phase": str(event.get("_notification_phase", "") or "auto_recovered")}
        for event in events
    ]
    mark_fast_recovery_recovery_events_notified(state, normalized, now_ts=now_ts)


def stream_engine_ffmpeg_auto_recovered_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    events_file: Path,
    max_events: int = 8,
) -> list[dict]:
    if not events_file.exists():
        return []

    acknowledged = state.get("stream_engine_ffmpeg_auto_recovered_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - max(60, int(recent_sec or 0))
    scheduled: list[tuple[int, dict]] = []
    started_by_episode: dict[tuple[str, int], tuple[int, dict]] = {}
    for item in iter_jsonl(events_file):
        event_type = str(item.get("event_type", "")).strip()
        if event_type not in {"ffmpeg_restart_scheduled", "ffmpeg_started"}:
            continue
        event_ts = parse_utc_ts(str(item.get("ts_utc", "")))
        if event_ts <= 0 or event_ts < cutoff or event_ts > int(now_ts) + 60:
            continue
        run_id = str(item.get("run_id", "") or "")
        try:
            restart_count = int(item.get("restart_count", 0) or 0)
        except Exception:
            restart_count = 0
        episode = (run_id, restart_count)
        if event_type == "ffmpeg_started":
            current = started_by_episode.get(episode)
            if current is None or event_ts < current[0]:
                started_by_episode[episode] = (event_ts, item)
            continue
        scheduled.append((event_ts, item))

    events: list[dict] = []
    for event_ts, item in scheduled:
        run_id = str(item.get("run_id", "") or "")
        try:
            restart_count = int(item.get("restart_count", 0) or 0)
        except Exception:
            restart_count = 0
        started = started_by_episode.get((run_id, restart_count))
        if started is None or started[0] < event_ts:
            continue
        started_ts, started_item = started
        exit_code = str(item.get("exit_code", "") or "")
        key = str(item.get("event_id", "") or "")
        if not key:
            key = f"{item.get('ts_utc')}|{run_id}|{restart_count}|{exit_code}"
        if acknowledged.get(key):
            continue
        evidence = (
            f"exit_code={exit_code or 'unknown'} "
            f"delay_sec={item.get('delay_sec', '')} "
            f"run_id={run_id} restart_count={restart_count} "
            f"restarted_at={started_item.get('ts_utc', '')} "
            f"ffmpeg_pid={started_item.get('ffmpeg_pid', '')}"
        )
        events.append(
            {
                "id": f"stream_engine:ffmpeg_auto_recovered:{run_id}:{restart_count}:{int(event_ts)}",
                "severity": "info",
                "component": "stream_engine_ffmpeg",
                "summary": "ffmpeg child restart completed",
                "evidence": evidence[:240],
                "recovery_type": f"ffmpeg_child_self_recovery:exit_{exit_code or 'unknown'}",
                "follow_up": "次回 routine check で同じ exit_code の再発回数と YouTube ingest 側の同時変化を確認する",
                "observed_ts": int(event_ts),
                "trigger": f"ffmpeg_exit_code_{exit_code or 'unknown'}",
                "_event_key": key,
                "_recovered_ts": int(started_ts),
            }
        )

    events.sort(key=lambda event: int(event.get("observed_ts", 0) or 0))
    return events[-max(1, int(max_events)) :]


def mark_stream_engine_ffmpeg_auto_recovered_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    acknowledged = state.get("stream_engine_ffmpeg_auto_recovered_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - 86400
    compacted: dict[str, int] = {}
    for key, value in acknowledged.items():
        try:
            ts = int(value or 0)
        except Exception:
            ts = 0
        if ts >= cutoff:
            compacted[str(key)] = ts

    for event in events:
        key = str(event.get("_event_key", "") or "")
        if key:
            compacted[key] = int(now_ts)
    state["stream_engine_ffmpeg_auto_recovered_notified"] = compacted


def k8s_container_restart_auto_recovered_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    events_file: Path,
    max_events: int = 8,
) -> list[dict]:
    if not events_file.exists():
        return []

    acknowledged = state.get("k8s_container_restart_auto_recovered_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - max(60, int(recent_sec or 0))
    events: list[dict] = []
    for item in iter_jsonl(events_file):
        if str(item.get("event_type", "")).strip() != "k8s_container_restart_count_changed":
            continue
        event_ts = parse_utc_ts(str(item.get("ts_utc", "")))
        if event_ts <= 0 or event_ts < cutoff or event_ts > int(now_ts) + 60:
            continue
        container = str(item.get("container", "") or "")
        pod = str(item.get("pod", "") or "")
        current_count = str(item.get("restart_count", "") or "")
        previous_count = str(item.get("previous_restart_count", "") or "")
        key = str(item.get("event_id", "") or "")
        if not key:
            key = f"{item.get('ts_utc')}|{pod}|{container}|{previous_count}|{current_count}"
        if acknowledged.get(key):
            continue
        evidence = (
            f"workload={item.get('workload', '')} pod={pod} container={container} "
            f"restartCount={previous_count}->{current_count} delta={item.get('delta', '')} "
            f"state={item.get('state', '')} last_state={item.get('last_state', '')}"
        )
        events.append(
            {
                "id": f"k8s:container_restart:{pod}:{container}:{int(event_ts)}",
                "severity": "info",
                "component": "k8s_runtime_container",
                "summary": "k8s container restart observed",
                "evidence": evidence[:240],
                "recovery_type": "k8s_container_restart_count_change",
                "follow_up": "次回 routine check で対象 container の restartCount と OOMKilled / exit reason を確認する",
                "observed_ts": int(event_ts),
                "trigger": "k8s_container_restart_count",
                "_event_key": key,
            }
        )

    events.sort(key=lambda event: int(event.get("observed_ts", 0) or 0))
    return events[-max(1, int(max_events)) :]


def mark_k8s_container_restart_auto_recovered_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    acknowledged = state.get("k8s_container_restart_auto_recovered_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - 86400
    compacted: dict[str, int] = {}
    for key, value in acknowledged.items():
        try:
            ts = int(value or 0)
        except Exception:
            ts = 0
        if ts >= cutoff:
            compacted[str(key)] = ts

    for event in events:
        key = str(event.get("_event_key", "") or "")
        if key:
            compacted[key] = int(now_ts)
    state["k8s_container_restart_auto_recovered_notified"] = compacted


def runtime_start_ts_from_run_id(run_id: str) -> int:
    raw = str(run_id or "").strip()
    token = raw.split("-", 1)[0]
    try:
        return int(calendar.timegm(time.strptime(token, "%Y%m%dT%H%M%SZ")))
    except Exception:
        return 0


def latest_runtime_snapshot(runtime_state_base_dir: Path) -> tuple[Path | None, dict]:
    candidates: list[Path] = [
        runtime_state_base_dir / "stream_runtime_state_remote.json",
        runtime_state_base_dir / "stream_runtime_state.json",
    ]
    candidates.extend(sorted(runtime_state_base_dir.glob("stream_runtime_state_*.json")))

    best_path: Path | None = None
    best_payload: dict = {}
    best_score = -1
    seen: set[Path] = set()
    for path in candidates:
        if path in seen or not path.exists():
            continue
        seen.add(path)
        payload = read_json_file(path)
        if not payload:
            continue
        updated_ts = parse_utc_ts(str(payload.get("updated_at_utc", "") or payload.get("ts_utc", "")))
        start_ts = runtime_start_ts_from_run_id(str(payload.get("run_id", "") or ""))
        try:
            mtime_ts = int(path.stat().st_mtime)
        except OSError:
            mtime_ts = 0
        score = max(updated_ts, start_ts, mtime_ts)
        if score > best_score:
            best_score = score
            best_path = path
            best_payload = payload
    return best_path, best_payload


def runtime_lifecycle_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    runtime_state_base_dir: Path,
) -> tuple[list[dict], dict | None]:
    path, payload = latest_runtime_snapshot(runtime_state_base_dir)
    if not path or not payload:
        return [], None

    run_id = str(payload.get("run_id", "") or "").strip()
    if not run_id:
        return [], None
    try:
        restart_count = int(payload.get("restart_count", 0) or 0)
    except Exception:
        restart_count = 0
    marker = {
        "run_id": run_id,
        "status": str(payload.get("status", "") or ""),
        "ffmpeg_pid": str(payload.get("ffmpeg_pid", "") or ""),
        "restart_count": restart_count,
        "updated_at_utc": str(payload.get("updated_at_utc", "") or ""),
        "source_file": str(path),
    }

    previous = state.get("runtime_lifecycle_seen")
    if not isinstance(previous, dict):
        return [], marker
    previous_run_id = str(previous.get("run_id", "") or "")
    if not previous_run_id or previous_run_id == run_id:
        return [], marker

    observed_ts = runtime_start_ts_from_run_id(run_id)
    if observed_ts <= 0:
        observed_ts = parse_utc_ts(marker["updated_at_utc"])
    if observed_ts <= 0:
        observed_ts = int(now_ts)
    cutoff = int(now_ts) - max(60, int(recent_sec or 0))
    if observed_ts < cutoff or observed_ts > int(now_ts) + 60:
        return [], marker

    evidence = (
        f"previous_run_id={previous_run_id} run_id={run_id} "
        f"status={marker['status']} ffmpeg_pid={marker['ffmpeg_pid']} "
        f"restart_count={marker['restart_count']} source={path.name}"
    )
    return [
        {
            "id": f"runtime:lifecycle:{run_id}:{int(observed_ts)}",
            "severity": "info",
            "component": "runtime_lifecycle",
            "summary": "runtime process restarted",
            "evidence": evidence[:240],
            "recovery_type": "runtime_run_id_changed",
            "follow_up": "次回 routine check で Pod restartCount / stream-engine OOMKilled / FFmpeg PID を確認する",
            "observed_ts": int(observed_ts),
            "trigger": "runtime_run_id_changed",
            "_runtime_marker": marker,
        }
    ], marker


def mark_runtime_lifecycle_seen(state: dict, marker: dict, *, now_ts: int) -> None:
    state["runtime_lifecycle_seen"] = {**marker, "seen_ts": int(now_ts), "seen_at_utc": utc_now_text(now_ts)}


def notify_maintenance_message_due(state: dict, item: dict, *, now_ts: int, repeat_sec: int, dry_run: bool) -> bool:
    if dry_run:
        return True
    last_sent = int(state.get("last_maintenance_status_sent_ts", 0) or 0)
    started_ts = int(item.get("_first_seen_ts", now_ts) or now_ts)
    return last_sent < started_ts or (now_ts - last_sent) >= repeat_sec


def slack_route_config(cfg: dict) -> dict:
    return {
        **cfg,
        "enabled": bool(cfg.get("enabled")) and bool(cfg.get("slack_enabled")),
        "webhook_url": str(cfg.get("slack_webhook_url", "") or ""),
        "username": str(cfg.get("slack_username", cfg.get("username", "ADS-B Stream Watchdog")) or "ADS-B Stream Watchdog"),
    }


def active_incident_state(state: dict, ident: str) -> dict:
    active = state.get("active") if isinstance(state.get("active"), dict) else {}
    stored = active.get(ident, {}) if isinstance(active, dict) else {}
    return stored if isinstance(stored, dict) else {}


def incident_first_seen_ts(item: dict, state: dict, now_ts: int) -> int:
    ident = str(item.get("id", "") or "")
    stored = active_incident_state(state, ident)
    candidates = [
        item.get("_first_seen_ts"),
        stored.get("first_seen_ts"),
        item.get("observed_ts"),
        now_ts,
    ]
    for value in candidates:
        try:
            ts = int(value or 0)
        except Exception:
            ts = 0
        if ts > 0:
            return ts
    return int(now_ts)


def incident_slack_notified_ts(item: dict, state: dict) -> int:
    for value in (item.get("_slack_notified_ts"), active_incident_state(state, str(item.get("id", "") or "")).get("slack_first_notified_ts")):
        try:
            ts = int(value or 0)
        except Exception:
            ts = 0
        if ts > 0:
            return ts
    return 0


def slack_immediate_incident(item: dict) -> bool:
    if str(item.get("severity", "") or "").lower() == "critical":
        return True
    component = str(item.get("component", "") or "")
    if component in SLACK_IMMEDIATE_COMPONENTS:
        return True
    ident = str(item.get("id", "") or "")
    return any(ident.startswith(prefix) for prefix in SLACK_IMMEDIATE_ID_PREFIXES)


def slack_incident_due(item: dict, state: dict, cfg: dict, *, now_ts: int) -> bool:
    if incident_slack_notified_ts(item, state) > 0:
        return False
    if slack_immediate_incident(item):
        return True
    first_seen = incident_first_seen_ts(item, state, now_ts)
    min_active_sec = max(60, int(cfg.get("slack_min_active_sec", 1800) or 1800))
    return (int(now_ts) - first_seen) >= min_active_sec


def slack_route_messages(
    *,
    ctx: NotifyStatusContext,
    messages: list[tuple[str, list[dict]]],
    state: dict,
    cfg: dict,
    now_ts: int,
) -> list[tuple[str, list[dict], str]]:
    if not (bool(cfg.get("enabled")) and bool(cfg.get("slack_enabled"))):
        return []
    routed: list[tuple[str, list[dict], str]] = []
    for phase, phase_incidents in messages:
        selected: list[dict] = []
        if phase in SLACK_ALLOW_EMPTY_PHASES:
            selected = list(phase_incidents)
        elif phase in SLACK_EVENT_PHASES:
            selected = list(phase_incidents)
        elif phase in SLACK_ACTIVE_PHASES:
            selected = [item for item in phase_incidents if slack_incident_due(item, state, cfg, now_ts=now_ts)]
        elif phase == "recovered":
            selected = [item for item in phase_incidents if incident_slack_notified_ts(item, state) > 0]
        if not selected and phase not in SLACK_ALLOW_EMPTY_PHASES:
            continue
        content = ctx.format_message(phase=phase, incidents=selected, state=state, now_ts=now_ts)
        routed.append((phase, selected, content))
    return routed


def mark_slack_active_incidents_notified(state: dict, messages: list[tuple[str, list[dict], str]], *, now_ts: int) -> None:
    active = state.get("active") if isinstance(state.get("active"), dict) else {}
    if not isinstance(active, dict):
        return
    for phase, phase_incidents, _content in messages:
        if phase not in SLACK_ACTIVE_PHASES:
            continue
        for item in phase_incidents:
            ident = str(item.get("id", "") or "")
            stored = active.get(ident)
            if not isinstance(stored, dict):
                continue
            if int(stored.get("slack_first_notified_ts", 0) or 0) > 0:
                continue
            stored["slack_first_notified_ts"] = int(now_ts)
            stored["slack_first_notified_at_utc"] = utc_now_text(now_ts)
            stored["slack_first_notified_phase"] = phase


def deliver_rendered_messages(
    *,
    ctx: NotifyStatusContext,
    rendered_messages: list[tuple[str, list[dict], str]],
    cfg: dict,
    now_ts: int,
    dry_run: bool,
    route: str,
    send_webhook: Callable[..., tuple[bool, str]],
    missing_webhook_warning: str = "",
) -> tuple[int, int, int]:
    if not dry_run and cfg["enabled"] and cfg["webhook_url"]:
        outbox = notify_outbox.load_notify_outbox(
            ctx.notify_outbox_file,
            now_ts=now_ts,
            ttl_sec=int(cfg["outbox_ttl_sec"]),
        )
        outbox = notify_outbox.enqueue_notify_messages(
            outbox,
            rendered_messages,
            username=str(cfg["username"]),
            now_ts=now_ts,
            max_pending=int(cfg["outbox_max_pending"]),
            route=route,
        )
        notify_outbox.save_notify_outbox(ctx.notify_outbox_file, outbox)
        return notify_outbox.flush_notify_outbox(
            outbox_path=ctx.notify_outbox_file,
            events_path=ctx.notify_events_file,
            cfg=cfg,
            now_ts=now_ts,
            send_webhook=send_webhook,
            dry_run=dry_run,
            route=route,
        )
    if not dry_run and rendered_messages:
        reason = "disabled" if not cfg["enabled"] else "missing_webhook_url"
        if reason == "missing_webhook_url" and missing_webhook_warning:
            print(missing_webhook_warning)
        sent = 0
        failures = 0
        for phase, phase_incidents, content in rendered_messages:
            event = {
                "ts_utc": utc_now_text(now_ts),
                "phase": phase,
                "incident_ids": [item.get("id") for item in phase_incidents],
                "dry_run": False,
                "enabled": cfg["enabled"],
                "route": route,
                "message": content,
                "outbox": False,
                "send_ok": reason == "disabled",
                "send_reason": reason,
            }
            append_jsonl(ctx.notify_events_file, event)
            if event["send_ok"]:
                sent += 1
            else:
                failures += 1
        return sent, failures, 0
    if not dry_run:
        return notify_outbox.flush_notify_outbox(
            outbox_path=ctx.notify_outbox_file,
            events_path=ctx.notify_events_file,
            cfg=cfg,
            now_ts=now_ts,
            send_webhook=send_webhook,
            dry_run=dry_run,
            route=route,
        )
    return 0, 0, 0


def deliver_notify_messages(
    *,
    ctx: NotifyStatusContext,
    messages: list[tuple[str, list[dict]]],
    state: dict,
    cfg: dict,
    now_ts: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    rendered_messages: list[tuple[str, list[dict], str]] = []
    for phase, phase_incidents in messages:
        content = ctx.format_message(phase=phase, incidents=phase_incidents, state=state, now_ts=now_ts)
        if dry_run or not cfg["enabled"]:
            print(content)
        rendered_messages.append((phase, phase_incidents, content))

    sent = 0
    failures = 0
    pending = 0
    route_sent, route_failures, route_pending = deliver_rendered_messages(
        ctx=ctx,
        rendered_messages=rendered_messages,
        cfg=cfg,
        now_ts=now_ts,
        dry_run=dry_run,
        route="discord",
        send_webhook=ctx.send_webhook,
        missing_webhook_warning="[warn] STREAM_NOTIFY_DISCORD_WEBHOOK_URL is not configured",
    )
    sent += route_sent
    failures += route_failures
    pending += route_pending

    slack_cfg = slack_route_config(cfg)
    slack_messages = slack_route_messages(ctx=ctx, messages=messages, state=state, cfg=cfg, now_ts=now_ts)
    if slack_messages or (not dry_run and slack_cfg["enabled"] and slack_cfg["webhook_url"]):
        slack_send = ctx.send_slack_webhook or ctx.send_webhook
        route_sent, route_failures, route_pending = deliver_rendered_messages(
            ctx=ctx,
            rendered_messages=slack_messages,
            cfg=slack_cfg,
            now_ts=now_ts,
            dry_run=dry_run,
            route="slack",
            send_webhook=slack_send,
            missing_webhook_warning="[warn] STREAM_NOTIFY_SLACK_WEBHOOK_URL is not configured",
        )
        sent += route_sent
        failures += route_failures
        pending += route_pending
        if not dry_run and slack_messages and slack_cfg["enabled"] and slack_cfg["webhook_url"]:
            mark_slack_active_incidents_notified(state, slack_messages, now_ts=now_ts)
    return sent, failures, pending


def notify_status(*, ctx: NotifyStatusContext, dry_run: bool = False, force_test: bool = False, now_ts: int | None = None) -> int:
    now = int(time.time() if now_ts is None else now_ts)
    cfg = ctx.load_config()
    state = ctx.load_state()
    active_state = state.get("active") if isinstance(state.get("active"), dict) else {}
    messages: list[tuple[str, list[dict]]] = []
    if force_test:
        messages.append(("test", []))

    maintenance_item = ctx.maintenance_notification_incident(now)
    if maintenance_item is not None:
        if notify_maintenance_message_due(
            state,
            maintenance_item,
            now_ts=now,
            repeat_sec=int(cfg.get("maintenance_repeat_sec", 600)),
            dry_run=dry_run,
        ):
            messages.append(("maintenance", [maintenance_item]))
            state["last_maintenance_status_sent_ts"] = now
        state["maintenance_active"] = True
        state["maintenance_started_ts"] = int(maintenance_item.get("_first_seen_ts", now) or now)
        state["active"] = active_state
        state["updated_ts_utc"] = utc_now_text(now)
        sent, failures, pending = deliver_notify_messages(
            ctx=ctx,
            messages=messages,
            state=state,
            cfg=cfg,
            now_ts=now,
            dry_run=dry_run,
        )
        if not dry_run:
            ctx.save_state(state)
        if not messages and pending <= 0 and sent <= 0 and failures <= 0:
            print("[notify-status] maintenance mode active; no reminder due")
        else:
            print(f"[notify-status] messages={len(messages)} sent={sent} failures={failures} pending={pending}")
        return 0 if failures == 0 else 1

    state["maintenance_active"] = False

    incidents = ctx.collect_incidents(
        now_ts=now,
        report_stale_sec=int(cfg["report_stale_sec"]),
        startup_grace_sec=int(cfg.get("startup_grace_sec", 0) or 0),
    )
    incident_by_id = {str(item.get("id")): item for item in incidents}
    previous_ids = set(active_state.keys())
    current_ids = set(incident_by_id.keys())
    new_ids = current_ids - previous_ids
    recovered_ids = previous_ids - current_ids

    for ident in current_ids:
        item = incident_by_id[ident]
        existing = active_state.get(ident, {})
        observed_ts = int(item.get("observed_ts", 0) or 0)
        first_default = observed_ts if observed_ts > 0 else now
        existing_first = int(existing.get("first_seen_ts", 0) or 0)
        first_seen = min(existing_first, first_default) if existing_first > 0 else first_default
        active_state[ident] = {
            **existing,
            "first_seen_ts": first_seen,
            "first_notified_ts": int(existing.get("first_notified_ts", now) or now),
            "last_bad_ts": observed_ts if observed_ts > 0 else now,
            "last_notified_ts": now,
            "last_incident": item,
        }

    if incidents:
        last_sent = int(state.get("last_status_sent_ts", 0) or 0)
        due = (now - last_sent) >= int(cfg["repeat_sec"])
        if new_ids or due:
            phase = "detected" if new_ids else "status"
            messages.append((phase, incidents))
            state["last_status_sent_ts"] = now
    if recovered_ids:
        recovered: list[dict] = []
        for ident in sorted(recovered_ids):
            stored = active_state.get(ident, {})
            item = dict(stored.get("last_incident", {"id": ident, "summary": "recovered"}))
            item["_first_seen_ts"] = int(stored.get("first_seen_ts", now) or now)
            item["_first_notified_ts"] = int(stored.get("first_notified_ts", item["_first_seen_ts"]) or item["_first_seen_ts"])
            item["_last_bad_ts"] = int(stored.get("last_bad_ts", 0) or 0)
            item["_slack_notified_ts"] = int(stored.get("slack_first_notified_ts", 0) or 0)
            recovered_ts, recovery_evidence = ctx.recovery_observation_for_incident(ident, now)
            item["_recovered_ts"] = recovered_ts
            item["_recovery_evidence"] = recovery_evidence
            recovered.append(item)
        messages.append(("recovered", recovered))
        for ident in recovered_ids:
            active_state.pop(ident, None)
        if not current_ids:
            state["last_status_sent_ts"] = 0

    fast_recovery_events = fast_recovery_recovery_events(
        state=state,
        now_ts=now,
        recent_sec=int(cfg.get("fast_recovery_event_recent_sec", 1800) or 1800),
        triggers=list(cfg.get("fast_recovery_event_triggers", [])),
        events_file=ctx.fast_recovery_events_file,
    )
    stream_engine_auto_recovered_events = stream_engine_ffmpeg_auto_recovered_events(
        state=state,
        now_ts=now,
        recent_sec=int(cfg.get("stream_engine_event_recent_sec", 1800) or 1800),
        events_file=ctx.stream_engine_events_file,
    )
    k8s_container_restart_events = k8s_container_restart_auto_recovered_events(
        state=state,
        now_ts=now,
        recent_sec=int(cfg.get("stream_watchdog_event_recent_sec", 1800) or 1800),
        events_file=ctx.stream_watchdog_events_file,
    )
    runtime_lifecycle_restart_events, runtime_marker = runtime_lifecycle_events(
        state=state,
        now_ts=now,
        recent_sec=int(cfg.get("runtime_lifecycle_event_recent_sec", 86400) or 86400),
        runtime_state_base_dir=ctx.runtime_state_base_dir,
    )
    for phase in ("restart_observed", "recovery_unconfirmed"):
        phase_events = [event for event in fast_recovery_events if event.get("_notification_phase") == phase]
        if phase_events:
            messages.append((phase, phase_events))
    fast_recovery_recovered_events = [
        event for event in fast_recovery_events if event.get("_notification_phase") == "auto_recovered"
    ]
    auto_recovered_events = [
        *fast_recovery_recovered_events,
        *stream_engine_auto_recovered_events,
        *k8s_container_restart_events,
        *runtime_lifecycle_restart_events,
    ]
    if auto_recovered_events:
        messages.append(("auto_recovered", auto_recovered_events))

    state["active"] = active_state
    state["updated_ts_utc"] = utc_now_text(now)

    sent, failures, pending = deliver_notify_messages(
        ctx=ctx,
        messages=messages,
        state=state,
        cfg=cfg,
        now_ts=now,
        dry_run=dry_run,
    )

    if fast_recovery_events and not dry_run:
        mark_fast_recovery_recovery_events_notified(state, fast_recovery_events, now_ts=now)
    if auto_recovered_events and not dry_run:
        mark_stream_engine_ffmpeg_auto_recovered_events_notified(
            state,
            stream_engine_auto_recovered_events,
            now_ts=now,
        )
        mark_k8s_container_restart_auto_recovered_events_notified(
            state,
            k8s_container_restart_events,
            now_ts=now,
        )
    if runtime_marker is not None and not dry_run:
        mark_runtime_lifecycle_seen(state, runtime_marker, now_ts=now)

    if not dry_run:
        ctx.save_state(state)
    if not messages and pending <= 0 and sent <= 0 and failures <= 0:
        print("[notify-status] no active incidents; no notification due")
    else:
        print(f"[notify-status] messages={len(messages)} sent={sent} failures={failures} pending={pending}")
    return 0 if failures == 0 else 1
