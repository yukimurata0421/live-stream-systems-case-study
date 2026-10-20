#!/usr/bin/env python3
"""
Monitor whether amazon_mail_notifier.service is running.

- Run systemctl is-active amazon_mail_notifier.service
- Send a Discord alert when the service is not active
"""

import os
import subprocess
from datetime import datetime, timezone

import requests

SERVICE_NAME = "amazon_mail_notifier.service"
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def is_service_active(name: str) -> bool:
    """Check service state with systemctl is-active"""
    result = subprocess.run(
        ["systemctl", "is-active", name],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "active"


def send_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not configured.")

    payload = {"content": message}
    resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code >= 300:
        raise RuntimeError(f"Discord Webhook error: {resp.status_code} {resp.text}")


def main():
    now = datetime.now(timezone.utc).isoformat()

    if is_service_active(SERVICE_NAME):
        print(f"[{now}] OK: {SERVICE_NAME} is active.")
        return

    msg = (
        f"🚨 The Amazon email notification service may have stopped.\n"
        f"Service: {SERVICE_NAME}\n"
        f"Time: {now}\n"
        "systemctl status amazon_mail_notifier.service should be checked."
    )
    print(msg)
    send_discord(msg)


if __name__ == "__main__":
    main()
