from __future__ import annotations

from pathlib import Path

try:
    from stream_core.common.json_io import read_json_file
    from stream_core.common.timeutil import parse_utc_ts
except ModuleNotFoundError:
    from common.json_io import read_json_file
    from common.timeutil import parse_utc_ts


PLANNED_ROLLOUT_ID_ANNOTATION = "stream-v3.yukimurata.dev/planned-rollout-id"
PLANNED_ROLLOUT_AT_ANNOTATION = "stream-v3.yukimurata.dev/planned-rollout-at"
PLANNED_ROLLOUT_EXPIRES_ANNOTATION = "stream-v3.yukimurata.dev/planned-rollout-expires-at"
PLANNED_ROLLOUT_REASON_ANNOTATION = "stream-v3.yukimurata.dev/planned-rollout-reason"


def planned_rollout_context(
    state_base_dir: Path,
    *,
    observed_ts: int,
    require_pod_start_match: bool = True,
) -> dict:
    snapshot = read_json_file(state_base_dir / "watchdog" / "k8s_container_restart_counts.json")
    pods = snapshot.get("pods") if isinstance(snapshot.get("pods"), list) else []
    candidates: list[tuple[int, dict]] = []
    for pod in pods:
        if not isinstance(pod, dict) or str(pod.get("phase") or "") != "Running":
            continue
        started_ts = parse_utc_ts(str(pod.get("started_at_utc") or pod.get("created_at_utc") or ""))
        candidates.append((started_ts, pod))
    if not candidates:
        return {}
    started_ts, pod = max(candidates, key=lambda item: item[0])
    annotations = (
        pod.get("planned_rollout_annotations")
        if isinstance(pod.get("planned_rollout_annotations"), dict)
        else {}
    )
    rollout_id = str(annotations.get(PLANNED_ROLLOUT_ID_ANNOTATION) or "").strip()
    planned_at = parse_utc_ts(str(annotations.get(PLANNED_ROLLOUT_AT_ANNOTATION) or ""))
    expires_at = parse_utc_ts(str(annotations.get(PLANNED_ROLLOUT_EXPIRES_ANNOTATION) or ""))
    if not rollout_id or planned_at <= 0 or expires_at <= planned_at:
        return {}
    # A bounded annotation applies only near the declared rollout. A later Pod
    # recreation from the same template must not inherit stale planned intent.
    if observed_ts < planned_at - 60 or observed_ts > expires_at:
        return {}
    if require_pod_start_match and started_ts > 0 and abs(started_ts - observed_ts) > 120:
        return {}
    images = pod.get("container_images") if isinstance(pod.get("container_images"), dict) else {}
    return {
        "rollout_id": rollout_id,
        "planned_at_ts": planned_at,
        "expires_at_ts": expires_at,
        "reason": str(annotations.get(PLANNED_ROLLOUT_REASON_ANNOTATION) or "")[:120],
        "pod": str(pod.get("name") or ""),
        "pod_uid": str(pod.get("uid") or ""),
        "stream_engine_image": str(images.get("stream-engine") or ""),
    }
