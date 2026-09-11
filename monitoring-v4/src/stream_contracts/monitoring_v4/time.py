from __future__ import annotations

from datetime import datetime, timezone


class ContractTimeError(ValueError):
    """Raised when a contract timestamp is missing or not an absolute UTC time."""


def parse_utc(value: str, *, field: str = "timestamp") -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ContractTimeError(f"{field} is required")
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError as exc:
        raise ContractTimeError(f"{field} must be RFC3339: {raw!r}") from exc
    if parsed.tzinfo is None:
        raise ContractTimeError(f"{field} must include a timezone")
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ContractTimeError(f"{field} must be UTC")
    return parsed.astimezone(timezone.utc)


def utc_text(value: datetime | int | float) -> str:
    if isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(value, timezone.utc)
    elif isinstance(value, datetime):
        if value.tzinfo is None:
            raise ContractTimeError("datetime must include a timezone")
        parsed = value.astimezone(timezone.utc)
    else:
        raise ContractTimeError(f"unsupported timestamp type: {type(value).__name__}")
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def unix_ts(value: str) -> int:
    return int(parse_utc(value).timestamp())


def require_not_before(later: str, earlier: str, *, later_field: str, earlier_field: str) -> None:
    if parse_utc(later, field=later_field) < parse_utc(earlier, field=earlier_field):
        raise ContractTimeError(f"{later_field} must not precede {earlier_field}")
