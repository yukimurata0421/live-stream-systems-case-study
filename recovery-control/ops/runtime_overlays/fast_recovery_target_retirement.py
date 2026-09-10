# ruff: noqa: F821


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
    if not unresolved_scopes or ffmpeg_pid <= 1:
        return False
    observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
    observed_target = observation.get("target_identity") if isinstance(observation, dict) else None
    metrics = transport_snapshot.get("metrics") if isinstance(transport_snapshot.get("metrics"), dict) else {}
    network = transport_snapshot.get("network") if isinstance(transport_snapshot.get("network"), dict) else {}
    if (
        not isinstance(observed_target, dict)
        or int(metrics.get("bytes_sent", 0) or 0) <= 0
        or bool(network.get("network_down"))
        or not bool(network.get("tcp_probe_ok"))
    ):
        return False
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
            or str(before.get("ffmpeg_generation") or "") == str(observed_target.get("ffmpeg_generation") or "")
        ):
            continue
        if not same_runtime and not target_retired:
            continue
        try:
            response = effect_contract.reconcile_delayed_effect(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                unresolved_scope=scope,
                runtime_observation=observation,
                transport={
                    "bytes_sent": int(metrics.get("bytes_sent", 0) or 0),
                    "network_down": bool(network.get("network_down")),
                    "tcp_probe_ok": bool(network.get("tcp_probe_ok")),
                },
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
                "delayed FFmpeg exit reconciled from executor ledger"
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
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "recovery_elapsed_sec": max(0, now_ts - parse_iso_ts(str(scope.get("created_at") or ""))),
                "same_idempotency_action_count": 1,
                "transport_snapshot": transport_snapshot,
                "youtube_hint": youtube_hint,
                "append_only_reconciliation": True,
                "automatic_retry_count": 0,
                "target_retired": target_retired,
                "physical_effect_outcome": "OBSERVED" if same_runtime else "UNKNOWN",
            },
        )
        reconciled_any = True
    return reconciled_any
