from __future__ import annotations

import json
from collections.abc import Iterable


FFMPEG_RESTART_ATTEMPT_EVENTS = frozenset({"ffmpeg_restart_scheduled"})


def _event_type(payload: dict) -> str:
    return str(payload.get("event_type") or payload.get("kind") or "").strip()


def _exit_code(payload: dict) -> str:
    for key in ("exit_code", "returncode", "code"):
        if key in payload and payload.get(key) is not None:
            return str(payload.get(key)).strip()
    return ""


def _payload_text(payload: dict) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True).lower()
    except Exception:
        return str(payload).lower()


def classify_ffmpeg_restart_root_cause(payloads: Iterable[dict]) -> str:
    items = list(payloads)
    text = " ".join(_payload_text(item) for item in items)
    exit_codes = {_exit_code(item) for item in items if _exit_code(item)}
    if any(term in text for term in ("low_upload_pressure", "low upload pressure")):
        return "low_upload_pressure_cluster"
    if (
        any(
            term in text
            for term in (
                "rtmps",
                "a.rtmps.youtube.com",
                "ssl",
                "tls",
                "cannot open connection",
                "connection timed out",
                "temporary failure in name resolution",
                "failed to resolve",
                "network is unreachable",
            )
        )
        or bool(exit_codes & {"146", "251"})
    ):
        return "rtmps_tls_connect_cluster"
    if "224" in exit_codes:
        return "rtmp_broken_pipe_self_recovery"
    if exit_codes & {"-9", "255"}:
        return "controlled_or_signal_restart"
    return "ffmpeg_restart_cluster_unknown"


def _count(values: Iterable[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        key = str(value or "unknown")
        out[key] = out.get(key, 0) + 1
    return out


def summarize_ffmpeg_restart_attempts(
    items: Iterable[tuple[int, dict]],
    *,
    episode_gap_sec: int = 60,
    incident_gap_sec: int = 600,
) -> dict:
    attempts = sorted(
        [(int(ts), payload) for ts, payload in items if _event_type(payload) in FFMPEG_RESTART_ATTEMPT_EVENTS],
        key=lambda item: item[0],
    )
    episodes: list[dict] = []
    current: list[tuple[int, dict]] = []

    def flush_episode() -> None:
        if not current:
            return
        start_ts = current[0][0]
        end_ts = current[-1][0]
        payloads = [payload for _ts, payload in current]
        exit_codes = _count(_exit_code(payload) or "unknown" for payload in payloads)
        episodes.append(
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "duration_sec": max(0, end_ts - start_ts),
                "attempt_count": len(current),
                "root_cause": classify_ffmpeg_restart_root_cause(payloads),
                "exit_codes": exit_codes,
            }
        )

    for item in attempts:
        ts, _payload = item
        if current and ts - current[-1][0] > max(1, int(episode_gap_sec)):
            flush_episode()
            current = []
        current.append(item)
    flush_episode()

    clusters: list[dict] = []
    current_cluster: list[dict] = []

    def flush_cluster() -> None:
        if not current_cluster:
            return
        root_causes = _count(str(ep.get("root_cause", "unknown")) for ep in current_cluster)
        root_cause = next(iter(root_causes)) if len(root_causes) == 1 else "mixed_ffmpeg_restart_cluster"
        clusters.append(
            {
                "start_ts": int(current_cluster[0]["start_ts"]),
                "end_ts": int(current_cluster[-1]["end_ts"]),
                "duration_sec": max(0, int(current_cluster[-1]["end_ts"]) - int(current_cluster[0]["start_ts"])),
                "episode_count": len(current_cluster),
                "attempt_count": sum(int(ep.get("attempt_count", 0) or 0) for ep in current_cluster),
                "root_cause": root_cause,
                "episode_root_causes": root_causes,
            }
        )

    for episode in episodes:
        if (
            current_cluster
            and int(episode["start_ts"]) - int(current_cluster[-1]["end_ts"]) > max(1, int(incident_gap_sec))
        ):
            flush_cluster()
            current_cluster = []
        current_cluster.append(episode)
    flush_cluster()

    return {
        "attempt_count": len(attempts),
        "retry_episode_count": len(episodes),
        "incident_cluster_count": len(clusters),
        "episode_root_causes": _count(str(ep.get("root_cause", "unknown")) for ep in episodes),
        "incident_root_causes": _count(str(cluster.get("root_cause", "unknown")) for cluster in clusters),
        "max_episode_duration_sec": max((int(ep.get("duration_sec", 0) or 0) for ep in episodes), default=0),
        "max_attempts_per_episode": max((int(ep.get("attempt_count", 0) or 0) for ep in episodes), default=0),
        "episodes": episodes,
        "incident_clusters": clusters,
        "episode_gap_sec": max(1, int(episode_gap_sec)),
        "incident_gap_sec": max(1, int(incident_gap_sec)),
    }
