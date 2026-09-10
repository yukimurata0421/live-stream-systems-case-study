# ruff: noqa: F821


def select_restart_reason(
    state: dict[str, Any],
    *,
    url_preservation_mode: bool,
    remote_warning_streak: int,
    remote_warning_confirm: int,
    remote_warning_reason: str,
    network: NetworkObservation,
    net_fail_confirm: int,
    stall_confirm: int,
    low_upload_confirm: int,
    low_upload_max_mbps: float,
    tcp: TcpObservation,
) -> tuple[str, str]:
    import math

    if url_preservation_mode and remote_warning_streak >= remote_warning_confirm:
        return "remote_warning", f"youtube pre-loss warning while broadcast live: {remote_warning_reason}"
    if int(state.get("net_fail_streak", 0)) >= net_fail_confirm:
        return (
            "network_down",
            (
                f"network down: gw_ok={network.gateway_ok} public_ok_count={network.public_ok_count} "
                f"dns_ok={network.dns_ok} tcp_probe_ok={network.tcp_probe_ok}"
            ),
        )
    if int(state.get("stall_streak", 0)) >= stall_confirm:
        return (
            "tcp_stall",
            (f"tcp stall: bytes_delta={tcp.bytes_delta} lastsnd_ms={tcp.lastsnd_ms} notsent={tcp.notsent} unacked={tcp.unacked}"),
        )
    measurement = state.get("ack_delivery_measurement_v1")
    if isinstance(measurement, dict):
        latest = measurement.get("latest")
        statistics = measurement.get("statistics")
        threshold = measurement.get("configured_restart_threshold_mbps")
        latest_ack_mbps = latest.get("ack_mbps") if isinstance(latest, dict) else None
        confirmations = measurement.get("restart_confirmations")
        low_streak = measurement.get("shadow_low_streak")
        threshold_valid = (
            isinstance(threshold, (int, float))
            and not isinstance(threshold, bool)
            and math.isfinite(float(threshold))
            and float(threshold) > 0
        )
        latest_rate_valid = (
            isinstance(latest_ack_mbps, (int, float))
            and not isinstance(latest_ack_mbps, bool)
            and math.isfinite(float(latest_ack_mbps))
            and float(latest_ack_mbps) >= 0
        )
        confirmation_valid = (
            type(confirmations) is int and 1 <= confirmations <= 12 and type(low_streak) is int and low_streak >= confirmations
        )
        measured_restart = (
            measurement.get("schema_version") == "stream_v3.ack_delivery_measurement.v1"
            and measurement.get("status") == "VALID"
            and measurement.get("measurement_enabled") is True
            and measurement.get("action_enabled") is True
            and threshold_valid
            and latest_rate_valid
            and float(latest_ack_mbps) < float(threshold)
            and measurement.get("below_configured_threshold") is True
            and confirmation_valid
            and measurement.get("restart_candidate_confirmed") is True
            and isinstance(latest, dict)
            and latest.get("queue_pressure") is True
            and latest.get("pending_effect") is False
            and isinstance(statistics, dict)
            and statistics.get("baseline_ready") is True
            and not network.network_down
            and network.tcp_probe_ok
        )
        if measured_restart:
            return (
                "tcp_stall",
                (
                    "measured ACK delivery rate low: "
                    f"rolling_mbps={latest_ack_mbps} "
                    f"threshold_mbps={threshold} "
                    f"confirmations={low_streak} "
                    f"notsent={tcp.notsent} unacked={tcp.unacked} lastsnd_ms={tcp.lastsnd_ms}"
                ),
            )
    # Low upload pressure and ACK-rate shadow candidates remain observable, but
    # neither can own a physical effect without an explicit measured threshold,
    # a complete baseline, and the separate action-enable gate above.
    del low_upload_confirm, low_upload_max_mbps
    return "", ""


def youtube_hint(payload: Any) -> dict[str, Any]:
    empty = {
        "api_live_state": "",
        "oauth_life_cycle_status": "",
        "oauth_stream_status": "",
        "oauth_stream_health_status": "",
        "remote_source": "",
        "remote_status": "",
    }
    if not isinstance(payload, dict):
        return {
            **empty,
            "source_observed_at": "",
            "status_age_sec": None,
            "status_max_age_sec": None,
            "fresh": False,
            "stale_values_suppressed": False,
        }
    source_observed_at = str(payload.get("ts_utc") or "")
    age = payload.get("_controller_status_age_sec")
    max_age = payload.get("_controller_status_max_age_sec")
    fresh = payload.get("_controller_status_fresh") is True
    values = {
        "api_live_state": payload.get("api_live_state", ""),
        "oauth_life_cycle_status": payload.get("oauth_life_cycle_status", ""),
        "oauth_stream_status": payload.get("oauth_stream_status", ""),
        "oauth_stream_health_status": payload.get("oauth_stream_health_status", ""),
        "remote_source": payload.get("remote_source", ""),
        "remote_status": payload.get("remote_status", ""),
    }
    return {
        **(values if fresh else empty),
        "source_observed_at": source_observed_at,
        "controller_observed_at": str(payload.get("_controller_status_observed_at") or ""),
        "status_age_sec": age if isinstance(age, int) else None,
        "status_max_age_sec": max_age if isinstance(max_age, int) else None,
        "fresh": fresh,
        "stale_values_suppressed": not fresh and any(bool(value) for value in values.values()),
    }
