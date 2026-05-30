from __future__ import annotations


def choose_staged_audio_recovery(
    *,
    stage: int,
    dj_service: str,
    stream_service: str,
    reason_prefix: str,
) -> list[tuple[str, str, str]]:
    if stage <= 1:
        return [(dj_service, "dj", f"{reason_prefix} [stage1 dj-only]")]
    if stage == 2:
        return [(stream_service, "stream", f"{reason_prefix} [stage2 stream-only]")]
    return [
        (dj_service, "dj", f"{reason_prefix} [stage3]"),
        (stream_service, "stream", f"{reason_prefix} [stage3]"),
    ]
