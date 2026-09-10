# ruff: noqa: F821


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
