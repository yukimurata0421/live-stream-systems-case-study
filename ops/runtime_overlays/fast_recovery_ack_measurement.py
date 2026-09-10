# ruff: noqa: F821


def maybe_append_tcp_send_sample(
    state: dict[str, Any],
    *,
    now_ts: int,
    ffmpeg_pid: int,
    bytes_sent: int,
    metrics: dict[str, int | str],
) -> None:
    """Measure ACK delivery without changing the v20 recovery decision path."""

    observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
    observed_target = observation.get("target_identity") if isinstance(observation, dict) else None
    try:
        observed_ffmpeg_pid = int(observed_target.get("ffmpeg_pid", 0) or 0) if isinstance(observed_target, dict) else 0
    except (TypeError, ValueError):
        observed_ffmpeg_pid = 0
    source_matches_running_child = not (
        ffmpeg_pid <= 1
        or not metrics
        or not isinstance(observed_target, dict)
        or observed_ffmpeg_pid != ffmpeg_pid
        or not str(observed_target.get("ffmpeg_generation") or "")
    )
    if not source_matches_running_child:
        state["successor_ack_samples"] = []
    else:
        generation = str(observed_target["ffmpeg_generation"])
        acked = int(metrics.get("bytes_acked", 0) or 0)
        raw_ack = state.get("successor_ack_samples", [])
        ack_samples = [dict(item) for item in raw_ack if isinstance(item, dict)] if isinstance(raw_ack, list) else []
        ack_samples = [
            item
            for item in ack_samples
            if int(item.get("ffmpeg_pid", 0) or 0) == ffmpeg_pid and str(item.get("ffmpeg_generation") or "") == generation
        ]
        observed_at = datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        if not ack_samples or str(ack_samples[-1].get("observed_at") or "") != observed_at:
            ack_samples.append(
                {
                    "observed_at": observed_at,
                    "ffmpeg_pid": ffmpeg_pid,
                    "ffmpeg_generation": generation,
                    "bytes_acked": acked,
                }
            )
        state["successor_ack_samples"] = ack_samples[-4:]

    threshold_raw = env("FR_ACK_RATE_RESTART_THRESHOLD_MBPS")
    try:
        restart_threshold_mbps = float(threshold_raw) if threshold_raw else None
    except ValueError:
        restart_threshold_mbps = float("nan")
    try:
        from fast_recovery_controller import ack_delivery

        ack_report = ack_delivery.observe_ack_delivery(
            state,
            now_ts=now_ts,
            runtime_observation=observation if source_matches_running_child else {},
            stream_profile={
                "video_bitrate": env("VIDEO_BITRATE", "3400k"),
                "video_maxrate": env("VIDEO_MAXRATE", "3400k"),
                "video_bufsize": env("VIDEO_BUFSIZE", "6800k"),
                "audio_bitrate": env("AUDIO_BITRATE", "192k"),
            },
            measurement_enabled=bool_env("FR_ACK_RATE_MEASUREMENT_ENABLED", True),
            action_enabled=bool_env("FR_ACK_RATE_ACTION_ENABLED", False),
            restart_threshold_mbps=restart_threshold_mbps,
            pending_effect=pending_recovery_count(state) > 0,
            window_sec=max(10, int_env("FR_ACK_RATE_WINDOW_SEC", 60)),
            history_sec=max(60, int_env("FR_ACK_RATE_HISTORY_SEC", 86400)),
            history_sample_sec=max(10, int_env("FR_ACK_RATE_HISTORY_SAMPLE_SEC", 60)),
            minimum_healthy_samples=max(1, int_env("FR_ACK_RATE_MIN_HEALTHY_SAMPLES", 720)),
            restart_confirmations=max(1, int_env("FR_ACK_RATE_RESTART_CONFIRMATIONS", 2)),
            source_max_age_sec=max(1, int_env("FR_ACK_RATE_SOURCE_MAX_AGE_SEC", 30)),
            max_window_slack_sec=max(1, int_env("FR_ACK_RATE_MAX_WINDOW_SLACK_SEC", 30)),
            queue_notsent_threshold=LOW_UPLOAD_PRESSURE_NOTSENT_BYTES,
            queue_unacked_threshold=LOW_UPLOAD_PRESSURE_UNACKED,
            queue_lastsnd_threshold_ms=LOW_UPLOAD_PRESSURE_LASTSND_MS,
        )
    except Exception as exc:  # measurement failure must not take down the existing recovery loop
        state["ack_delivery_measurement_v1"] = {
            "schema_version": "stream_v3.ack_delivery_measurement.v1",
            "status": "UNKNOWN",
            "reason_code": f"ACK_RATE_MEASUREMENT_EXCEPTION:{type(exc).__name__}",
            "restart_candidate_confirmed": False,
        }
    else:
        if ack_report.get("history_sample_emitted") is True:
            append_event(
                "tcp_ack_delivery_measurement",
                "source-timed rolling ACK delivery measurement",
                {"ack_delivery": ack_report, "physical_effect_count": 0},
            )

    if not source_matches_running_child:
        return

    if TCP_SEND_SAMPLE_LOG_SEC <= 0:
        return
    last_pid = int(state.get("last_tcp_send_sample_pid", 0) or 0)
    last_ts = int(state.get("last_tcp_send_sample_ts", 0) or 0)
    last_bytes_sent = int(state.get("last_tcp_send_sample_bytes_sent", 0) or 0)
    if last_pid != ffmpeg_pid or last_ts <= 0 or last_bytes_sent <= 0 or bytes_sent < last_bytes_sent:
        state["last_tcp_send_sample_ts"] = now_ts
        state["last_tcp_send_sample_pid"] = ffmpeg_pid
        state["last_tcp_send_sample_bytes_sent"] = bytes_sent
        return
    elapsed_sec = max(0, now_ts - last_ts)
    if elapsed_sec < TCP_SEND_SAMPLE_LOG_SEC:
        return
    bytes_delta = max(0, bytes_sent - last_bytes_sent)
    mbps = round((bytes_delta * 8) / (elapsed_sec * 1_000_000), 3) if elapsed_sec > 0 else 0.0
    append_event(
        "tcp_send_sample",
        "ffmpeg tcp send sample",
        {
            "ffmpeg_pid": ffmpeg_pid,
            "ffmpeg_generation": generation,
            "sample_interval_sec": elapsed_sec,
            "bytes_sent_delta": bytes_delta,
            "bytes_sent": bytes_sent,
            "mbps": mbps,
            "bytes_acked": acked,
            "send_q": int(metrics.get("send_q", 0) or 0),
            "notsent": int(metrics.get("notsent", 0) or 0),
            "unacked": int(metrics.get("unacked", 0) or 0),
            "lastsnd_ms": int(metrics.get("lastsnd_ms", 0) or 0),
            "rto_ms": int(metrics.get("rto_ms", 0) or 0),
            "conn": str(metrics.get("conn", "") or ""),
        },
    )
    state["last_tcp_send_sample_ts"] = now_ts
    state["last_tcp_send_sample_pid"] = ffmpeg_pid
    state["last_tcp_send_sample_bytes_sent"] = bytes_sent
