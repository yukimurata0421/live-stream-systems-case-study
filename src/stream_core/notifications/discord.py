from __future__ import annotations

import json
import urllib.request


def send_discord_webhook(webhook_url: str, content: str, *, username: str = "ADS-B Stream Watchdog", timeout: float = 10.0) -> tuple[bool, str]:
    payload = json.dumps({"content": content, "username": username}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "adsb-streamnew-notifier/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status = getattr(response, "status", response.getcode())
            body = response.read(256).decode("utf-8", errors="replace")
    except Exception as exc:
        return False, str(exc)
    if 200 <= int(status) < 300:
        return True, f"http_{status}"
    return False, f"http_{status}:{body}"

