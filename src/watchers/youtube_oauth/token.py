from __future__ import annotations

import urllib.error
from collections.abc import Callable

from .config import OAuthConfig


LoadState = Callable[[], dict]
SaveState = Callable[[dict], object]
UtcNow = Callable[[], str]


def oauth_is_configured(config: OAuthConfig) -> bool:
    return config.is_configured()


def get_oauth_access_token(
    *,
    config: OAuthConfig,
    now_ts: int,
    load_state: LoadState,
    save_state: SaveState,
    post_form_json: Callable[..., dict],
    utc_now: UtcNow,
) -> tuple[str, int, str]:
    if not config.enabled:
        return "", 0, "oauth disabled"
    if not config.is_configured():
        return "", 0, "oauth not configured"

    state = load_state()
    access_token = str(state.get("access_token", "")).strip()
    expires_at = int(state.get("expires_at", 0))
    if access_token and expires_at - now_ts >= config.min_token_ttl_sec:
        return access_token, expires_at, "oauth access token cached"

    try:
        payload = post_form_json(config.token_url, config.refresh_form(), timeout_sec=config.timeout_sec)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        msg = f"oauth token refresh http {exc.code}: {body[:240]}"
        if "invalid_grant" in body:
            msg += " (refresh token invalid/expired or app test-user restriction)"
        return "", 0, msg
    except Exception as exc:
        return "", 0, f"oauth token refresh failed: {exc}"

    token = str(payload.get("access_token", "")).strip()
    expires_in = int(payload.get("expires_in", 0) or 0)
    if not token or expires_in <= 0:
        return "", 0, "oauth token refresh returned no access token"

    new_state = {
        "access_token": token,
        "expires_at": now_ts + expires_in,
        "token_type": str(payload.get("token_type", "Bearer")),
        "scope": str(payload.get("scope", "")),
        "updated_at_utc": utc_now(),
    }
    save_state(new_state)
    return token, int(new_state["expires_at"]), "oauth token refreshed"
