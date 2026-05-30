from __future__ import annotations


def mask_rtmp_url(rtmp_url: str) -> str:
    return f"{rtmp_url.rsplit('/', 1)[0]}/***" if "/" in rtmp_url else "***"


def resolve_rtmp_url(rtmp_url: str, stream_key: str) -> str:
    if not stream_key:
        return rtmp_url

    base = rtmp_url.strip()
    key = stream_key.strip()
    if not base or "YOUR_STREAM_KEY" in base or "<" in base:
        base = "rtmps://a.rtmps.youtube.com:443/live2"

    if base.endswith(f"/{key}"):
        return base
    if "YOUR_STREAM_KEY" in base:
        return base.replace("YOUR_STREAM_KEY", key)
    return f"{base.rstrip('/')}/{key}"


def validate_rtmp_url(rtmp_url: str, stream_key: str) -> None:
    bad = {"YOUR_REAL_STREAM_KEY", "YOUR_STREAM_KEY", ""}
    if stream_key in bad or "<" in rtmp_url or "YOUR_STREAM_KEY" in rtmp_url:
        raise RuntimeError("RTMP_URL has placeholder key.")
    allowed_prefixes = (
        "rtmp://a.rtmp.youtube.com/live2/",
        "rtmps://a.rtmps.youtube.com/live2/",
        "rtmps://a.rtmps.youtube.com:443/live2/",
    )
    if not rtmp_url.startswith(allowed_prefixes):
        raise RuntimeError("RTMP_URL format is invalid for YouTube Live.")
