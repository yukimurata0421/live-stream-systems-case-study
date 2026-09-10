# ruff: noqa: F821


def append_recovery_dispatch_result(
    *,
    action: dict[str, Any],
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
    ok: bool,
    detail: str,
    automatic_retry: bool | None = None,
) -> None:
    scope = str(action.get("recovery_scope") or "runtime")
    if not ok and automatic_retry is False:
        kind = "recovery_outcome_unknown"
    elif not ok:
        kind = "recovery_action_failed"
    elif scope == "ffmpeg_child":
        kind = "recovery_signal_sent"
    else:
        kind = "recovery_action_dispatched"
    process_still_running: bool | None = None
    if ok and scope == "ffmpeg_child" and "owns final child cleanup" in detail:
        process_still_running = True
    append_event(
        kind,
        reason,
        {
            **action,
            "trigger": reason_kind,
            "ffmpeg_pid": ffmpeg_pid,
            "dispatch_ok": ok,
            "automatic_retry": automatic_retry,
            "process_still_running_after_dispatch": process_still_running,
            "detail": detail,
        },
    )


def remember_pending_recovery(
    state: dict[str, Any],
    *,
    action: dict[str, Any],
    now_ts: int,
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
) -> None:
    """Persist both the controller correlation and executor-owned request ID."""

    raw = state.get("pending_recovery_actions", [])
    pending = [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    item = {
        **action,
        "requested_at_ts": now_ts,
        "trigger": reason_kind,
        "reason": reason,
        "requested_ffmpeg_pid": ffmpeg_pid,
    }
    correlation_id = str(action.get("correlation_id") or action.get("recovery_action_id") or "")
    if correlation_id:
        item["correlation_id"] = correlation_id
    if EFFECT_EXECUTOR_SOCKET and correlation_id and not item.get("owner_request_id"):
        try:
            status = effect_contract.effect_request_status_by_correlation(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                correlation_id=correlation_id,
            )
        except BaseException as exc:  # noqa: BLE001 - an oracle gap must retain the pending action
            item["executor_correlation_status_error"] = type(exc).__name__
            item["executor_request_state"] = "UNKNOWN"
            item["executor_scope_state"] = "UNKNOWN"
        else:
            item["executor_correlation_reason"] = str(status.get("reason") or "UNKNOWN")
            item["executor_correlation_matching_count"] = int(status.get("matching_count", 0) or 0)
            if status.get("ok") is True and int(status.get("matching_count", 0) or 0) == 1:
                item["owner_request_id"] = str(status.get("request_id") or "")
                item["effect_scope_id"] = str(status.get("effect_scope_id") or "")
                item["executor_request_state"] = str(status.get("state") or "UNKNOWN")
                item["executor_scope_state"] = str(status.get("effect_scope_state") or "UNKNOWN")
                item["executor_result"] = status.get("result") if isinstance(status.get("result"), dict) else {}
                item["physical_attempt_count"] = int(status.get("physical_attempt_count", 0) or 0)
                item["awaiting_transport_verification"] = (
                    str(status.get("effect_scope_state") or "") == "EFFECT_OBSERVED_AWAITING_VERIFICATION"
                )
            else:
                item["executor_request_state"] = "UNKNOWN"
                item["executor_scope_state"] = "UNKNOWN"
    pending.append(item)
    state["pending_recovery_actions"] = pending[-16:]


def sync_pending_recovery_from_executor(state: dict[str, Any], scopes: list[dict[str, Any]]) -> None:
    """Retain durable physical results until successor transport is verified."""

    raw = state.get("pending_recovery_actions", [])
    local: dict[str, dict[str, Any]] = {}
    provisional: list[dict[str, Any]] = []
    source_items = [dict(item) for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []

    for item in source_items:
        action_id = str(item.get("recovery_action_id") or "")
        correlation_id = str(item.get("correlation_id") or "")
        owner = str(item.get("owner_request_id") or "")
        if not correlation_id and action_id.startswith("fra-"):
            correlation_id = action_id
        if not owner and action_id and not action_id.startswith("fra-"):
            owner = action_id
        if not owner and correlation_id:
            try:
                correlation_status = effect_contract.effect_request_status_by_correlation(
                    socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                    correlation_id=correlation_id,
                )
            except BaseException as exc:  # noqa: BLE001 - retain provisional state on an oracle gap
                item["executor_correlation_status_error"] = type(exc).__name__
                item["executor_request_state"] = "UNKNOWN"
                item["executor_scope_state"] = "UNKNOWN"
            else:
                item["executor_correlation_reason"] = str(correlation_status.get("reason") or "UNKNOWN")
                item["executor_correlation_matching_count"] = int(correlation_status.get("matching_count", 0) or 0)
                if correlation_status.get("ok") is True and int(correlation_status.get("matching_count", 0) or 0) == 1:
                    owner = str(correlation_status.get("request_id") or "")
                    item["effect_scope_id"] = str(correlation_status.get("effect_scope_id") or "")
                    item["executor_request_state"] = str(correlation_status.get("state") or "UNKNOWN")
                    item["executor_scope_state"] = str(correlation_status.get("effect_scope_state") or "UNKNOWN")
                    result = correlation_status.get("result")
                    item["executor_result"] = result if isinstance(result, dict) else {}
                    item["physical_attempt_count"] = int(correlation_status.get("physical_attempt_count", 0) or 0)
                else:
                    item["executor_request_state"] = "UNKNOWN"
                    item["executor_scope_state"] = "UNKNOWN"
        if correlation_id:
            item["correlation_id"] = correlation_id
        if not owner:
            provisional.append(item)
            continue
        item["owner_request_id"] = owner
        existing = local.get(owner)
        if existing is None:
            local[owner] = item
            continue
        existing_is_controller_action = str(existing.get("recovery_action_id") or "").startswith("fra-")
        item_is_controller_action = action_id.startswith("fra-")
        primary, secondary = (item, existing) if item_is_controller_action and not existing_is_controller_action else (existing, item)
        merged = {**secondary, **primary, "owner_request_id": owner}
        if correlation_id or existing.get("correlation_id"):
            merged["correlation_id"] = str(correlation_id or existing.get("correlation_id") or "")
        local[owner] = merged

    pending: list[dict[str, Any]] = []
    for scope in scopes:
        owner = str(scope.get("owner_request_id") or "")
        if not owner:
            continue
        identity = scope.get("identity") if isinstance(scope.get("identity"), dict) else {}
        existing = local.pop(owner, {})
        item = {
            **existing,
            "recovery_action_id": str(existing.get("recovery_action_id") or owner),
            "owner_request_id": owner,
            "owner_request_digest": str(scope.get("owner_request_digest") or ""),
            "effect_scope_id": str(scope.get("effect_scope_id") or ""),
            "executor_scope_state": str(scope.get("state") or "UNKNOWN"),
            "requested_ffmpeg_pid": int(identity.get("ffmpeg_pid", 0) or 0),
            "requested_ffmpeg_generation": str(identity.get("ffmpeg_generation") or ""),
            "executor_identity": identity,
            "awaiting_transport_verification": str(scope.get("state") or "") == "EFFECT_OBSERVED_AWAITING_VERIFICATION",
        }
        try:
            status = effect_contract.effect_request_status(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                request_id=owner,
            )
        except BaseException as exc:  # noqa: BLE001 - retain pending state on an oracle gap
            item["executor_status_error"] = type(exc).__name__
        else:
            result = status.get("result") if isinstance(status.get("result"), dict) else {}
            item["executor_request_state"] = str(status.get("state") or "UNKNOWN")
            item["executor_scope_state"] = str(status.get("effect_scope_state") or item["executor_scope_state"])
            item["executor_result"] = result
            item["physical_attempt_count"] = int(status.get("physical_attempt_count", 0) or 0)
            observed_running = result.get("process_still_running_after_dispatch")
            item["process_still_running_after_dispatch"] = (
                observed_running if isinstance(observed_running, bool) else (False if result.get("exit_observed") is True else None)
            )
        pending.append(item)

    reported_raw = state.get("reported_executor_terminal_request_ids", [])
    reported = {str(item) for item in reported_raw} if isinstance(reported_raw, list) else set()
    for owner, existing in local.items():
        try:
            status = effect_contract.effect_request_status(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                request_id=owner,
            )
        except BaseException as exc:  # noqa: BLE001 - status uncertainty must not erase a pending action
            pending.append({**existing, "executor_status_error": type(exc).__name__})
            continue
        request_state = str(status.get("state") or "UNKNOWN")
        scope_state = str(status.get("effect_scope_state") or "UNKNOWN")
        result = status.get("result") if isinstance(status.get("result"), dict) else {}
        if scope_state in {
            "RECONCILED_EFFECT_OBSERVED",
            "RELEASED_NO_EFFECT",
            "RETIRED_TARGET_OUTCOME_UNKNOWN",
        }:
            continue
        if request_state == "EFFECT_FAILED":
            if scope_state != "EFFECT_FAILED":
                pending.append(
                    {
                        **existing,
                        "executor_request_state": request_state,
                        "executor_scope_state": scope_state,
                        "executor_result": result,
                        "physical_attempt_count": int(status.get("physical_attempt_count", 0) or 0),
                        "executor_status_inconsistent": True,
                    }
                )
                state["last_reason"] = f"executor request/scope state mismatch: request={request_state} scope={scope_state}"
                continue
            if owner not in reported:
                append_event(
                    "recovery_action_failed",
                    str(result.get("reason") or "executor effect failed"),
                    {
                        "recovery_action_id": owner,
                        "effect_scope_id": status.get("effect_scope_id"),
                        "executor_request_state": request_state,
                        "executor_scope_state": scope_state,
                        "executor_result": result,
                        "physical_attempt_count": int(status.get("physical_attempt_count", 0) or 0),
                        "automatic_retry": False,
                    },
                )
                reported.add(owner)
            state["last_reason"] = f"executor effect failed: {result.get('reason', 'UNKNOWN')}"
            continue
        observed_running = result.get("process_still_running_after_dispatch")
        pending.append(
            {
                **existing,
                "executor_request_state": request_state,
                "executor_scope_state": scope_state,
                "executor_result": result,
                "physical_attempt_count": int(status.get("physical_attempt_count", 0) or 0),
                "awaiting_transport_verification": request_state == "EFFECT_OBSERVED",
                "process_still_running_after_dispatch": (
                    observed_running if isinstance(observed_running, bool) else (False if result.get("exit_observed") is True else None)
                ),
            }
        )
    pending.extend(provisional)
    state["reported_executor_terminal_request_ids"] = sorted(reported)[-64:]
    state["pending_recovery_actions"] = pending[-16:]


def maybe_record_recovery_completed(
    state: dict[str, Any],
    *,
    now_ts: int,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    transport_snapshot: dict[str, Any],
    youtube_hint: dict[str, Any],
) -> None:
    raw = state.get("pending_recovery_actions", [])
    pending = [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    if not pending or ffmpeg_pid <= 1:
        return
    observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
    observed_target = observation.get("target_identity") if isinstance(observation, dict) else None
    metrics = transport_snapshot.get("metrics") if isinstance(transport_snapshot.get("metrics"), dict) else {}
    network = transport_snapshot.get("network") if isinstance(transport_snapshot.get("network"), dict) else {}
    ack_raw = state.get("successor_ack_samples", [])
    ack_samples = [dict(item) for item in ack_raw if isinstance(item, dict)] if isinstance(ack_raw, list) else []
    if (
        not isinstance(observed_target, dict)
        or int(observed_target.get("ffmpeg_pid", 0) or 0) != ffmpeg_pid
        or not str(observed_target.get("ffmpeg_generation") or "")
        or int(metrics.get("bytes_sent", 0) or 0) <= 0
        or bool(network.get("network_down"))
        or not bool(network.get("tcp_probe_ok"))
        or len(ack_samples) < 3
    ):
        return
    generation = str(observed_target["ffmpeg_generation"])
    samples = ack_samples[-3:]
    sample_times = [parse_iso_ts(str(item.get("observed_at") or "")) for item in samples]
    acked = [int(item.get("bytes_acked", 0) or 0) for item in samples]
    if (
        any(int(item.get("ffmpeg_pid", 0) or 0) != ffmpeg_pid for item in samples)
        or any(str(item.get("ffmpeg_generation") or "") != generation for item in samples)
        or any(value <= 0 for value in sample_times)
        or any(later <= earlier or later - earlier > 30 for earlier, later in zip(sample_times, sample_times[1:], strict=False))
        or now_ts - sample_times[0] > 30
        or any(later <= earlier for earlier, later in zip(acked, acked[1:], strict=False))
        or int(metrics.get("bytes_acked", 0) or 0) != acked[-1]
    ):
        return
    completed: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for item in pending:
        requested_pid = int(item.get("requested_ffmpeg_pid", 0) or 0)
        requested_generation = str(item.get("requested_ffmpeg_generation") or "")
        if (
            (requested_pid > 1 and requested_pid == ffmpeg_pid)
            or (requested_generation and requested_generation == generation)
            or str(item.get("executor_request_state") or "") not in {"EFFECT_OBSERVED", ""}
        ):
            remaining.append(item)
            continue
        completed.append(item)
    for item in completed:
        same_key_count = sum(1 for other in completed if other.get("idempotency_key") == item.get("idempotency_key"))
        append_event(
            "recovery_completed",
            str(item.get("reason") or "recovery completed"),
            {
                **{
                    key: item.get(key)
                    for key in (
                        "recovery_action_id",
                        "controller_id",
                        "execution_mode",
                        "execute",
                        "idempotency_key",
                        "requested_signal",
                        "recovery_scope",
                        "trigger",
                    )
                },
                "requested_ffmpeg_pid": int(item.get("requested_ffmpeg_pid", 0) or 0),
                "requested_ffmpeg_generation": str(item.get("requested_ffmpeg_generation") or ""),
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_generation": generation,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "recovery_elapsed_sec": max(0, now_ts - int(item.get("requested_at_ts", now_ts) or now_ts)),
                "same_idempotency_action_count": same_key_count,
                "executor_result": item.get("executor_result", {}),
                "transport_snapshot": transport_snapshot,
                "transport_verification": {
                    "oracle": "SUCCESSOR_CONSECUTIVE_ACK_PROGRESS",
                    "ack_observation_count": len(samples),
                    "bytes_acked_start": acked[0],
                    "bytes_acked_end": acked[-1],
                    "bytes_acked_delta": acked[-1] - acked[0],
                    "ack_samples": samples,
                },
                "youtube_hint": youtube_hint,
            },
        )
    state["pending_recovery_actions"] = remaining[-16:]


def maybe_reconcile_delayed_executor_effects(
    state: dict[str, Any],
    *,
    unresolved_scopes: list[dict[str, Any]],
    now_ts: int,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    transport_snapshot: dict[str, Any],
    youtube_hint: dict[str, Any],
) -> bool:
    if not unresolved_scopes:
        before_count = pending_recovery_count(state)
        maybe_record_recovery_completed(
            state,
            now_ts=now_ts,
            ffmpeg_pid=ffmpeg_pid,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            transport_snapshot=transport_snapshot,
            youtube_hint=youtube_hint,
        )
        return pending_recovery_count(state) < before_count
    if ffmpeg_pid <= 1:
        return False
    observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
    observed_target = observation.get("target_identity") if isinstance(observation, dict) else None
    metrics = transport_snapshot.get("metrics") if isinstance(transport_snapshot.get("metrics"), dict) else {}
    network = transport_snapshot.get("network") if isinstance(transport_snapshot.get("network"), dict) else {}
    if (
        not isinstance(observed_target, dict)
        or int(observed_target.get("ffmpeg_pid", 0) or 0) != ffmpeg_pid
        or int(metrics.get("bytes_sent", 0) or 0) <= 0
        or bool(network.get("network_down"))
        or not bool(network.get("tcp_probe_ok"))
    ):
        return False
    generation = str(observed_target.get("ffmpeg_generation") or "")
    ack_raw = state.get("successor_ack_samples", [])
    ack_samples = [dict(item) for item in ack_raw if isinstance(item, dict)] if isinstance(ack_raw, list) else []
    samples = ack_samples[-3:]
    sample_times = [parse_iso_ts(str(item.get("observed_at") or "")) for item in samples]
    acked = [int(item.get("bytes_acked", 0) or 0) for item in samples]
    ack_progress = (
        len(samples) == 3
        and bool(generation)
        and all(int(item.get("ffmpeg_pid", 0) or 0) == ffmpeg_pid for item in samples)
        and all(str(item.get("ffmpeg_generation") or "") == generation for item in samples)
        and all(value > 0 for value in sample_times)
        and all(later > earlier and later - earlier <= 30 for earlier, later in zip(sample_times, sample_times[1:], strict=False))
        and now_ts - sample_times[0] <= 30
        and all(later > earlier for earlier, later in zip(acked, acked[1:], strict=False))
        and int(metrics.get("bytes_acked", 0) or 0) == acked[-1]
    )
    reconciled_any = False
    for scope in unresolved_scopes:
        before = scope.get("identity") if isinstance(scope.get("identity"), dict) else None
        if not isinstance(before, dict):
            continue
        same_runtime = all(
            before.get(name) == observed_target.get(name)
            for name in ("host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id")
        )
        target_retired = all(before.get(name) == observed_target.get(name) for name in ("host_id", "namespace", "container_name")) and any(
            before.get(name) != observed_target.get(name) for name in ("host_boot_id", "pod_uid", "container_id")
        )
        if same_runtime and (
            int(before.get("ffmpeg_pid", 0) or 0) == int(observed_target.get("ffmpeg_pid", 0) or 0)
            or str(before.get("ffmpeg_generation") or "") == generation
        ):
            continue
        if not same_runtime and not target_retired:
            continue
        if same_runtime and not ack_progress:
            state["last_reason"] = "successor exists; consecutive ACK progress not yet proven"
            continue
        operation_result: dict[str, Any] = {}
        try:
            status = effect_contract.effect_request_status(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                request_id=str(scope.get("owner_request_id") or ""),
            )
            operation_result = status.get("result") if isinstance(status.get("result"), dict) else {}
        except BaseException:  # noqa: BLE001 - reconciliation evidence remains sufficient without this annotation
            operation_result = {}
        transport = {
            "bytes_sent": int(metrics.get("bytes_sent", 0) or 0),
            "network_down": bool(network.get("network_down")),
            "tcp_probe_ok": bool(network.get("tcp_probe_ok")),
        }
        if same_runtime:
            transport.update(
                {
                    "ffmpeg_generation": generation,
                    "ack_observation_count": len(samples),
                    "bytes_acked_start": acked[0],
                    "bytes_acked_end": acked[-1],
                    "bytes_acked_delta": acked[-1] - acked[0],
                    "ack_samples": samples,
                }
            )
        try:
            response = effect_contract.reconcile_delayed_effect(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                unresolved_scope=scope,
                runtime_observation=observation,
                transport=transport,
            )
        except BaseException as exc:  # noqa: BLE001 - keep the scope unresolved and suppress action creation
            state["last_reason"] = f"effect reconciliation pending: {type(exc).__name__}"
            continue
        expected_state = "RECONCILED_EFFECT_OBSERVED" if same_runtime else "RETIRED_TARGET_OUTCOME_UNKNOWN"
        if response.get("ok") is not True or response.get("state") != expected_state:
            state["last_reason"] = f"effect reconciliation rejected: {response.get('reason', 'UNKNOWN')}"
            continue
        append_event(
            "recovery_completed",
            (
                "FFmpeg exit and successor ACK progress reconciled from executor ledger"
                if same_runtime
                else "retired exact target closed with physical outcome still unknown"
            ),
            {
                "recovery_action_id": scope.get("owner_request_id"),
                "effect_scope_id": scope.get("effect_scope_id"),
                "reconciliation_id": response.get("reconciliation_id"),
                "reconciliation_evidence_digest": response.get("evidence_digest"),
                "requested_ffmpeg_pid": int(before.get("ffmpeg_pid", 0) or 0),
                "requested_ffmpeg_generation": str(before.get("ffmpeg_generation") or ""),
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_generation": generation,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "recovery_elapsed_sec": max(0, now_ts - parse_iso_ts(str(scope.get("created_at") or ""))),
                "same_idempotency_action_count": 1,
                "executor_result": operation_result,
                "transport_snapshot": transport_snapshot,
                "transport_verification": (
                    {
                        "oracle": "SUCCESSOR_CONSECUTIVE_ACK_PROGRESS",
                        "ack_observation_count": len(samples),
                        "bytes_acked_start": acked[0],
                        "bytes_acked_end": acked[-1],
                        "bytes_acked_delta": acked[-1] - acked[0],
                        "ack_samples": samples,
                    }
                    if same_runtime
                    else {"oracle": "TARGET_RETIRED", "ack_observation_count": 0}
                ),
                "youtube_hint": youtube_hint,
                "append_only_reconciliation": True,
                "automatic_retry_count": 0,
                "target_retired": target_retired,
                "physical_effect_outcome": "OBSERVED" if same_runtime else "UNKNOWN",
            },
        )
        reconciled_any = True
    return reconciled_any


def maybe_append_tcp_send_sample(
    state: dict[str, Any],
    *,
    now_ts: int,
    ffmpeg_pid: int,
    bytes_sent: int,
    metrics: dict[str, int | str],
) -> None:
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
            # A source sample for another child must invalidate the current
            # decision cycle rather than leave a prior confirmed report live.
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


def read_youtube_live_warning(now_ts: int, last_restart_ts: int) -> tuple[bool, str, dict[str, Any]]:
    warning, reason, payload = remote_warning_core.read_youtube_live_warning(
        stats_path=YTW_STATS_FILE,
        quota_state_path=QUOTA_STATE_FILE,
        now_ts=now_ts,
        last_restart_ts=last_restart_ts,
        url_preservation_mode=URL_PRESERVATION_MODE,
        status_max_age_sec=YTW_STATUS_MAX_AGE_SEC,
        require_local_ok=REMOTE_WARNING_REQUIRE_LOCAL_OK,
        live_like_lifecycle=LIVE_LIKE_LIFECYCLE,
        parse_iso_ts=parse_iso_ts,
    )
    annotated = dict(payload) if isinstance(payload, dict) else {}
    stats_ts = parse_iso_ts(str(annotated.get("ts_utc") or ""))
    age_sec = now_ts - stats_ts if stats_ts > 0 else None
    annotated["_controller_status_observed_at"] = iso_now()
    annotated["_controller_status_max_age_sec"] = YTW_STATUS_MAX_AGE_SEC
    annotated["_controller_status_age_sec"] = age_sec
    annotated["_controller_status_fresh"] = age_sec is not None and -1 <= age_sec <= YTW_STATUS_MAX_AGE_SEC
    return warning, reason, annotated
