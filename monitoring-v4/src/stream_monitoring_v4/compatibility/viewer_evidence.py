"""Interpret viewer producer facts without equating capture with visual health."""


def probe_state(payload):
    if "probe_ok" in payload:
        return payload["probe_ok"] if type(payload["probe_ok"]) is bool else None
    if (payload.get("schema") not in {"stream_v3.viewer_synthetic.v1", "stream_v3.viewer_synthetic.v2"}
            or payload.get("status") not in {"healthy", "degraded", "failed"}):
        return None
    frame, failures = payload.get("frame_ok"), payload.get("consecutive_probe_failures")
    if type(frame) is not bool or type(failures) is not int or failures < 0:
        return None
    return frame and failures == 0


def visual_state(payload):
    if probe_state(payload) is not True or payload.get("frame_ok") is not True:
        return None
    flags = [payload.get("black_detected"), payload.get("freeze_detected")]
    failures = payload.get("consecutive_visual_failures")
    if any(flag is True for flag in flags) or type(failures) is int and failures > 0:
        return False
    if all(flag is False for flag in flags) and type(failures) is int and failures == 0:
        return True
    return None
