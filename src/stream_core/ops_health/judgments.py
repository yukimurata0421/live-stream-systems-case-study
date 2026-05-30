from __future__ import annotations


SSL_TLS_REASON_TERMS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "ssl_tls_handshake_failed",
        (
            "ssl handshake",
            "tls handshake",
            "handshake failed",
            "ssl_connect",
            "tlsv1 alert",
        ),
    ),
    (
        "ssl_tls_certificate_error",
        (
            "certificate verify failed",
            "self signed certificate",
            "unknown ca",
            "certificate has expired",
            "certificate expired",
        ),
    ),
    (
        "ssl_tls_protocol_error",
        (
            "wrong version number",
            "unsupported protocol",
            "protocol version",
            "tlsv1 alert protocol version",
        ),
    ),
    (
        "ssl_tls_transport_error",
        (
            "ssl_error",
            "ssl error",
            "tls error",
            "gnutls",
            "openssl",
            "tls fatal alert",
        ),
    ),
)


def tcp_send_budget_judgment(sample_count: int, over_budget_duration_sec: int) -> tuple[str, str]:
    if sample_count <= 0:
        return "unknown_no_samples", "no ffmpeg tcp send samples in 24h"
    if over_budget_duration_sec >= 3600:
        return "investigate_bandwidth_budget_pressure", "ffmpeg tcp send samples exceeded budget for >=3600s in 24h"
    if over_budget_duration_sec >= 300:
        return "observe_bandwidth_budget_pressure", "ffmpeg tcp send samples exceeded budget for >=300s in 24h"
    return "ok_within_budget", "ffmpeg tcp send samples are within the 24h bandwidth budget"


def flatten_text(value: object) -> str:
    parts: list[str] = []

    def walk(item: object) -> None:
        if item is None:
            return
        if isinstance(item, dict):
            for key, val in item.items():
                parts.append(str(key))
                walk(val)
            return
        if isinstance(item, (list, tuple, set)):
            for val in item:
                walk(val)
            return
        parts.append(str(item))

    walk(value)
    return " ".join(parts)


def ssl_tls_reason(value: object) -> str:
    text = flatten_text(value).lower()
    if not text:
        return ""
    has_ssl_context = any(term in text for term in ("ssl", "tls", "gnutls", "openssl", "certificate"))
    if not has_ssl_context and "handshake failed" not in text:
        return ""
    for reason, terms in SSL_TLS_REASON_TERMS:
        if any(term in text for term in terms):
            return reason
    return "ssl_tls_error"


def rtmps_ssl_tls_judgment(count_1h: int, count_24h: int) -> tuple[str, str]:
    if count_1h >= 1:
        return "investigate_rtmps_ssl_tls_immediate", "RTMPS SSL/TLS event count >=1 in 1h"
    if count_24h >= 2:
        return "investigate_rtmps_ssl_tls_repeated", "RTMPS SSL/TLS event count >=2 in 24h"
    if count_24h == 1:
        return "observe_rtmps_ssl_tls_single", "RTMPS SSL/TLS event count is 1 in 24h"
    return "ok_none", "RTMPS SSL/TLS event count is 0 in 24h"


def parse_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "ok", "active", "live", "good"}:
        return True
    if text in {"0", "false", "no", "inactive", "none", "bad"}:
        return False
    return None


def remote_warning_restart_judgment(count_1h: int, count_24h: int) -> tuple[str, str]:
    if count_1h >= 2:
        return "review_confirm_condition_immediate", "remote_warning restart count >=2 in 1h"
    if count_24h >= 4:
        return "review_confirm_condition", "remote_warning restart count >=4 in 24h"
    if count_24h >= 2:
        return "observe", "remote_warning restart count is 2-3 in 24h"
    return "ok_single_or_none", "remote_warning restart count <=1 in 24h"


def exit_224_judgment(count_1h: int, count_24h: int) -> tuple[str, str]:
    if count_1h >= 2:
        return "investigate_immediate", "ffmpeg exit_224 count >=2 in 1h"
    if count_24h >= 4:
        return "investigate_network_or_ingest", "ffmpeg exit_224 count >=4 in 24h"
    if count_24h >= 2:
        return "observe_rtmp_path", "ffmpeg exit_224 count is 2-3 in 24h"
    return "ok_single_or_none", "ffmpeg exit_224 count <=1 in 24h"


def truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "ok", "active", "live", "good"}


def _public_probe_text(item: dict) -> str:
    fields = (
        "status",
        "health_source",
        "watch_reason",
        "reason",
        "message",
        "public_probe_error",
        "public_probe_reason",
        "yt_dlp_error",
    )
    return " ".join(str(item.get(field, "")) for field in fields if item.get(field) is not None).lower()


def public_probe_degraded_reason(item: dict) -> str:
    text = _public_probe_text(item)
    status = str(item.get("status", "")).strip().lower()
    source = str(item.get("health_source", "")).strip().lower()
    public_source = (
        "public_probe" in source
        or "public probe" in text
        or "public live probe" in text
        or "watch page" in text
        or "yt-dlp" in text
    )
    failure_terms = (
        "failed",
        "fetch failed",
        "http error 429",
        "too many requests",
        "bot confirmation",
        "network is unreachable",
        "timed out",
    )
    failed = any(term in text for term in failure_terms)
    if not public_source or (not failed and status not in {"warn", "unknown"}):
        return ""
    if "http error 429" in text or "too many requests" in text:
        return "public_probe_429"
    if "bot confirmation" in text or "yt-dlp" in text:
        return "public_probe_bot_confirmation"
    if "watch page" in text:
        return "watch_page_fetch_failed"
    return "public_probe_degraded"


def public_probe_authoritative_live_ok(item: dict) -> bool:
    api_live = str(item.get("api_live_state", "")).strip().lower() == "live"
    api_ok = truthy(item.get("api_ok")) or truthy(item.get("availability_ok")) or truthy(item.get("stream_active"))
    oauth_ok = (
        truthy(item.get("oauth_probe_ok"))
        or truthy(item.get("oauth_healthy"))
        or str(item.get("oauth_stream_status", "")).strip().lower() == "active"
    )
    local_ok = truthy(item.get("local_ok")) or truthy(item.get("ingest_connected")) or truthy(item.get("ffmpeg_pid"))
    return oauth_ok and (api_live or api_ok) and local_ok


def public_probe_judgment(count_1h: int, count_24h: int, authoritative_live_count_24h: int) -> tuple[str, str]:
    if count_1h >= 2:
        return "observe_public_probe_noise_clustered", "public probe degraded count >=2 in 1h"
    if count_24h >= 4:
        return "observe_public_probe_noise_frequent", "public probe degraded count >=4 in 24h"
    if count_24h > 0 and count_24h == authoritative_live_count_24h:
        return "observe_public_probe_noise_authoritative_live_ok", "public probe degraded while OAuth/Data API/local ingest were OK"
    if count_24h > 0:
        return "observe_public_probe_degraded", "public probe degraded count is 1-3 in 24h"
    return "ok_none", "public probe degraded count is 0 in 24h"


def fast_mode_judgment(episode_count: int, active_duration_sec: int, estimated_units: int) -> tuple[str, str]:
    if active_duration_sec >= 1800 or estimated_units >= 1080:
        return "investigate_fast_mode_runaway", "fast mode active duration >=1800s or estimated units >=1080 in 24h"
    if episode_count >= 4 or active_duration_sec >= 300 or estimated_units >= 180:
        return "observe_fast_mode_repeated", "fast mode repeated or active >=300s in 24h"
    if episode_count > 0:
        return "ok_short_fast_mode_episode", "fast mode activity is short in 24h"
    return "ok_none", "fast mode activity is 0 in 24h"


def api_report_judgment(open_fresh: bool, closed_fresh: bool, timers_active: bool) -> tuple[str, str]:
    if not timers_active:
        return "api_report_timer_attention", "one or more API cost report timers are inactive or unknown"
    if not open_fresh:
        return "api_open_day_report_stale", "open_day_latest.json is stale or missing"
    if not closed_fresh:
        return "api_closed_day_report_stale", "latest.json is stale or missing"
    return "ok", "API cost report files and timers look fresh"


def encoder_gap_judgment(sample_count: int, duration_sec: int) -> tuple[str, str]:
    if duration_sec >= 600:
        return "investigate_encoder_gap_viewer_state", "encoder gap under enableAutoStop=false lasted >=600s"
    if sample_count >= 2 or duration_sec >= 120:
        return "observe_encoder_gap_viewer_state", "encoder gap under enableAutoStop=false repeated or lasted >=120s"
    if sample_count > 0:
        return "ok_short_encoder_gap", "single short encoder gap sample under enableAutoStop=false"
    return "ok_none", "no encoder gap samples under enableAutoStop=false"


def estimate_fast_mode_units(duration_sec: int, *, interval_sec: int, units_per_probe: int) -> int:
    if duration_sec <= 0:
        return 0
    intervals = max(1, (duration_sec + max(1, interval_sec) - 1) // max(1, interval_sec))
    return intervals * max(0, units_per_probe)


def sample_duration(items: list[tuple[int, bool]], *, now_ts: int, max_step_sec: int = 600) -> tuple[int, int]:
    if not items:
        return 0, 0
    ordered = sorted((int(ts), bool(active)) for ts, active in items)
    active_count = sum(1 for _ts, active in ordered if active)
    duration = 0
    for idx, (ts, active) in enumerate(ordered):
        if not active:
            continue
        next_ts = ordered[idx + 1][0] if idx + 1 < len(ordered) else now_ts
        duration += max(0, min(max_step_sec, next_ts - ts))
    return active_count, duration


def encoder_gap_active(item: dict) -> bool:
    enable_auto_stop = parse_bool(item.get("oauth_enable_auto_stop"))
    if enable_auto_stop is not False:
        return False
    ffmpeg_pid = 0
    try:
        ffmpeg_pid = int(item.get("ffmpeg_pid", 0) or 0)
    except Exception:
        ffmpeg_pid = 0
    encoder_ok = (
        truthy(item.get("stream_active"))
        and truthy(item.get("ingest_connected"))
        and truthy(item.get("local_ok"))
        and ffmpeg_pid > 1
    )
    remote_live = (
        str(item.get("api_live_state", "")).strip().lower() == "live"
        or str(item.get("oauth_life_cycle_status", "")).strip().lower() in {"live", "livestarting", "testing", "teststarting"}
    )
    return (not encoder_ok) and remote_live
