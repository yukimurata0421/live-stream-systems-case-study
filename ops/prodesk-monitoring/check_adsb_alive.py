#!/usr/bin/env python3
"""
ADS-B 受信サービスの監視スクリプト。

- 監視対象 service が active かどうかチェック
- active でなければ Discord にアラート
- 同一アラートの連投をクールダウンで抑制
"""

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

SERVICE_NAME = os.environ.get("SERVICE_NAME", "airspy_adsb.service")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
ALERT_STATE_FILE = Path(
    os.environ.get("ALERT_STATE_FILE", "/tmp/check_adsb_alive_state.json").strip()
)
ALERT_COOLDOWN_SEC = int(os.environ.get("ALERT_COOLDOWN_SEC", "1800"))


def is_service_active(name: str) -> bool:
    result = subprocess.run(
        ["systemctl", "is-active", name],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "active"


def _read_state() -> dict:
    try:
        if ALERT_STATE_FILE.exists():
            return json.loads(ALERT_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _write_state(state: dict) -> None:
    try:
        ALERT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        ALERT_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"state write failed: {exc}")


def _should_send_alert(result_key: str, now_ts: int) -> bool:
    state = _read_state()
    last_key = str(state.get("last_alert_key", ""))
    last_ts = int(state.get("last_alert_ts", 0) or 0)
    if result_key != last_key:
        return True
    return (now_ts - last_ts) >= max(0, ALERT_COOLDOWN_SEC)


def _mark_alert_sent(result_key: str, now_ts: int) -> None:
    _write_state({"last_alert_key": result_key, "last_alert_ts": now_ts})


def _mark_recovered(now_ts: int) -> None:
    _write_state({"last_alert_key": "", "last_alert_ts": now_ts})


def send_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL が設定されていません。")

    payload = {"content": message}
    resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code >= 300:
        raise RuntimeError(f"Discord Webhook error: {resp.status_code} {resp.text}")


def main():
    now = datetime.now(timezone.utc).isoformat()
    now_ts = int(time.time())

    if is_service_active(SERVICE_NAME):
        print(f"[{now}] OK: {SERVICE_NAME} is active.")
        _mark_recovered(now_ts)
        return

    msg = (
        "🚨 ADS-B 受信システムが停止している可能性があります。\n"
        f"サービス名: {SERVICE_NAME}\n"
        f"時刻: {now}\n"
        f"systemctl status {SERVICE_NAME} を確認してください。"
    )
    print(msg)

    result_key = f"{SERVICE_NAME}:inactive"
    if _should_send_alert(result_key, now_ts):
        send_discord(msg)
        _mark_alert_sent(result_key, now_ts)
    else:
        print("Duplicate alert suppressed by cooldown.")


if __name__ == "__main__":
    main()
