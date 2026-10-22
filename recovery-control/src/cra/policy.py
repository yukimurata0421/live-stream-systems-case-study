"""Recovery policy: validation, signature check and adoption rule (I8)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from cra import signing

SCHEMA = "cra.policy.v1"
MODES = ("off", "dry_run", "enforce")

# (name, minimum, maximum) for every integer field.
_INT_FIELDS = (
    ("version", 0, 2**31),
    ("stall_confirm_samples", 2, 30),
    ("stall_lastsnd_ms", 1000, 600_000),
    ("stall_notsent_bytes", 1, 1 << 30),
    ("stall_unacked", 1, 1 << 20),
    ("min_ffmpeg_uptime_sec", 0, 3600),
    ("cooldown_sec", 10, 86_400),
    ("max_per_hour", 0, 60),
    ("max_per_day", 0, 500),
    ("verify_window_sec", 10, 600),
)


class PolicyError(ValueError):
    pass


@dataclass(frozen=True)
class Policy:
    version: int = 0
    mode: str = "dry_run"
    stall_confirm_samples: int = 5
    stall_lastsnd_ms: int = 4000
    stall_notsent_bytes: int = 8192
    stall_unacked: int = 64
    min_ffmpeg_uptime_sec: int = 20
    cooldown_sec: int = 60
    max_per_hour: int = 4
    max_per_day: int = 12
    verify_window_sec: int = 60

    def to_dict(self) -> dict[str, Any]:
        return {"schema": SCHEMA, **asdict(self)}


# Used until a signed policy is adopted. Never acts on its own.
BUILTIN = Policy()


def parse(value: Any) -> Policy:
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise PolicyError("POLICY_SCHEMA_INVALID")
    known = {"schema", "mode"} | {name for name, _, _ in _INT_FIELDS}
    unknown = set(value) - known
    if unknown:
        raise PolicyError(f"POLICY_UNKNOWN_FIELDS:{','.join(sorted(unknown))}")
    if value.get("mode") not in MODES:
        raise PolicyError("POLICY_MODE_INVALID")
    fields: dict[str, Any] = {"mode": value["mode"]}
    for name, minimum, maximum in _INT_FIELDS:
        item = value.get(name)
        if type(item) is not int or not minimum <= item <= maximum:
            raise PolicyError(f"POLICY_FIELD_INVALID:{name}")
        fields[name] = item
    if fields["max_per_day"] < fields["max_per_hour"]:
        raise PolicyError("POLICY_DAILY_BELOW_HOURLY")
    return Policy(**fields)


def sign_envelope(key: Any, policy: Policy) -> dict[str, Any]:
    body = policy.to_dict()
    return {"policy": body, "signature": signing.sign(key, body)}


def open_envelope(envelope: Any, public_key: str) -> Policy:
    if not isinstance(envelope, dict) or not isinstance(envelope.get("signature"), str):
        raise PolicyError("POLICY_ENVELOPE_INVALID")
    body = envelope.get("policy")
    if not signing.verify(public_key, body, envelope["signature"]):
        raise PolicyError("POLICY_SIGNATURE_INVALID")
    return parse(body)


def adoption_error(current: Policy | None, candidate: Policy) -> str | None:
    """None when the candidate may replace the current policy."""
    if current is None:
        return None
    if candidate.version < current.version:
        return "POLICY_VERSION_REGRESSION"
    if candidate.version == current.version:
        return None if candidate == current else "POLICY_VERSION_REUSED"
    return None
