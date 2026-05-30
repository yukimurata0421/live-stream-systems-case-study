#!/usr/bin/env python3
"""
amazon_mail_notifier.service が本当に動いているかチェックする監視スクリプト。

- systemctl is-active amazon_mail_notifier.service を実行
- active でなければ Discord にアラート通知
"""

import os
import subprocess
from datetime import datetime, timezone

import requests

SERVICE_NAME = "amazon_mail_notifier.service"
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def is_service_active(name: str) -> bool:
    """systemctl is-active でサービス状態を確認"""
    result = subprocess.run(
        ["systemctl", "is-active", name],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "active"


def send_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL が設定されていません。")

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
        f"🚨 Amazon メール通知サービスが停止している可能性があります。\n"
        f"サービス名: {SERVICE_NAME}\n"
        f"時刻: {now}\n"
        "systemctl status amazon_mail_notifier.service を確認してください。"
    )
    print(msg)
    send_discord(msg)


if __name__ == "__main__":
    main()
