from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class OAuthStatusContext:
    base_dir: Path
    state_base_dir: Path
    youtube_monitor_env_file: Path
    youtube_watchdog_stats_file: Path
    read_env_file: Callable[[Path], dict[str, str]]
    read_json_file: Callable[[Path], dict]
    parse_bool: Callable[[object], bool | None]
    utc_now_text: Callable[[], str]
    authorization_url: Callable[[dict[str, str]], str]
    build_status_payload: Callable[..., dict]
    attach_live_probe: Callable[..., None]


def load_youtube_oauth_config(ctx: OAuthStatusContext) -> dict[str, str]:
    cfg = ctx.read_env_file(Path("/etc/default/adsb-streamnew"))
    cfg.update(ctx.read_env_file(ctx.youtube_monitor_env_file))
    cfg.update({key: value for key, value in os.environ.items() if key.startswith("YTW_") or key.startswith("STREAM_RUNTIME_")})
    return cfg


def oauth_status_payload(ctx: OAuthStatusContext, *, now_ts: int | None = None, live_probe: bool = False) -> dict:
    now = int(time.time() if now_ts is None else now_ts)
    cfg = load_youtube_oauth_config(ctx)
    token_state_file = Path(
        cfg.get("YTW_OAUTH_TOKEN_STATE_FILE", str(ctx.state_base_dir / "youtube_oauth_token_state.json")).strip()
        or str(ctx.state_base_dir / "youtube_oauth_token_state.json")
    )
    token_state = ctx.read_json_file(token_state_file)
    stats = ctx.read_json_file(ctx.youtube_watchdog_stats_file)
    payload = ctx.build_status_payload(
        cfg=cfg,
        token_state=token_state,
        stats=stats,
        now_ts=now,
        token_state_file=token_state_file,
        watchdog_stats_file=ctx.youtube_watchdog_stats_file,
        state_base_dir=ctx.state_base_dir,
        parse_bool=ctx.parse_bool,
        utc_now_text=ctx.utc_now_text,
    )
    if live_probe:
        ctx.attach_live_probe(payload, cfg=cfg, base_dir=ctx.base_dir)
    return payload


def oauth_status(ctx: OAuthStatusContext, *, json_output: bool = False, live_probe: bool = False) -> int:
    payload = oauth_status_payload(ctx, live_probe=live_probe)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        stats = payload["stats"]
        print(
            "[oauth-status] "
            f"judgment={payload['judgment']} enabled={payload['enabled']} "
            f"configured={payload['configured']} shadow_mode={payload['shadow_mode']} "
            f"stats_oauth_probe_ok={stats.get('oauth_probe_ok')} api_ok={stats.get('api_ok')}"
        )
        print(f"[oauth-status] reason={stats.get('oauth_reason', '')}")
        if payload.get("access_token_expires_in_sec") is not None:
            print(
                "[oauth-status] "
                f"access_token_cached={payload['access_token_cached']} "
                f"expires_in_sec={payload['access_token_expires_in_sec']}"
            )
        if payload.get("authorization_url"):
            print(f"[oauth-status] authorization_url={payload['authorization_url']}")
        for action in payload.get("actions", []):
            print(f"[oauth-status] action={action}")
        if payload.get("live_probe") is not None:
            print(f"[oauth-status] live_probe={json.dumps(payload['live_probe'], ensure_ascii=False, sort_keys=True)}")
    return 0 if payload["judgment"] == "oauth_control_plane_ok" else 1
