from __future__ import annotations


def configured_video_id_from(*, video_id: str, live_url: str) -> str:
    if video_id:
        return video_id.strip()
    if not live_url:
        return ""
    marker = "watch?v="
    idx = live_url.find(marker)
    if idx < 0:
        return ""
    return live_url[idx + len(marker) :].split("&", 1)[0].strip()
