from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any


_ID_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,47}:[0-9a-f]{24,64}$")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def stable_id(namespace: str, *parts: Any, length: int = 32) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_.-]{1,47}", namespace):
        raise ValueError(f"invalid ID namespace: {namespace!r}")
    if length < 24 or length > 64:
        raise ValueError("stable ID digest length must be between 24 and 64")
    digest = hashlib.sha256(canonical_json(parts).encode("utf-8")).hexdigest()[:length]
    return f"{namespace}:{digest}"


def payload_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def validate_id(value: str, *, field: str) -> None:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise ValueError(f"{field} is not a valid stable ID: {value!r}")


def json_copy(value: Mapping[str, Any] | Sequence[Any]) -> Any:
    """Return a JSON-only defensive copy and reject non-finite/non-JSON values."""

    return json.loads(canonical_json(value))
