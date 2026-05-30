from __future__ import annotations

import json
import urllib.parse
import urllib.request


def fetch(url: str, timeout_sec: int | None = None, *, default_timeout_sec: int) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome Safari",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    timeout = default_timeout_sec if timeout_sec is None else max(1, int(timeout_sec))
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_oauth_json(
    url: str,
    headers: dict[str, str] | None = None,
    timeout_sec: int | None = None,
    *,
    default_timeout_sec: int,
) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "stream-youtube-watchdog/1.0",
            "Accept": "application/json",
            **(headers or {}),
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec or default_timeout_sec) as r:
        raw = r.read().decode("utf-8", errors="ignore")
    return json.loads(raw)


def post_form_json(
    url: str,
    form: dict[str, str],
    timeout_sec: int | None = None,
    *,
    default_timeout_sec: int,
) -> dict:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "User-Agent": "stream-youtube-watchdog/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec or default_timeout_sec) as r:
        raw = r.read().decode("utf-8", errors="ignore")
    return json.loads(raw)
