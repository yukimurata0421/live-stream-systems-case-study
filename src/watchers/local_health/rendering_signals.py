from __future__ import annotations

import urllib.parse


def parse_url_port(url: str, default: int) -> int:
    try:
        return urllib.parse.urlparse(url).port or default
    except ValueError:
        return default
