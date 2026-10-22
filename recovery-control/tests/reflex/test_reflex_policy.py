from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from cra import policy as policy_mod
from cra import signing
from cra.policy import Policy, PolicyError


def test_builtin_never_enforces() -> None:
    assert policy_mod.BUILTIN.mode == "dry_run"


def test_roundtrip(tmp_path: Path) -> None:
    public = signing.generate_key(tmp_path / "k")
    policy = Policy(version=3, mode="enforce")
    envelope = policy_mod.sign_envelope(signing.load_private_key(tmp_path / "k"), policy)
    assert policy_mod.open_envelope(envelope, public) == policy


def test_wrong_key_rejected(tmp_path: Path) -> None:
    signing.generate_key(tmp_path / "a")
    other = signing.generate_key(tmp_path / "b")
    envelope = policy_mod.sign_envelope(signing.load_private_key(tmp_path / "a"), Policy(version=1))
    with pytest.raises(PolicyError, match="SIGNATURE"):
        policy_mod.open_envelope(envelope, other)


@pytest.mark.parametrize(
    "change",
    [
        {"mode": "aggressive"},
        {"cooldown_sec": 1},
        {"max_per_hour": True},
        {"stall_confirm_samples": 1},
        {"max_per_day": 1, "max_per_hour": 2},
        {"extra": 1},
        {"schema": "other"},
    ],
)
def test_invalid_fields(change: dict) -> None:
    value = {**Policy(version=1).to_dict(), **change}
    with pytest.raises(PolicyError):
        policy_mod.parse(value)


def test_adoption_rules() -> None:
    v2 = Policy(version=2)
    assert policy_mod.adoption_error(None, v2) is None
    assert policy_mod.adoption_error(v2, Policy(version=3)) is None
    assert policy_mod.adoption_error(v2, v2) is None
    assert policy_mod.adoption_error(v2, Policy(version=1)) == "POLICY_VERSION_REGRESSION"
    assert policy_mod.adoption_error(v2, dataclasses.replace(v2, mode="enforce")) == "POLICY_VERSION_REUSED"


def test_private_key_file_is_owner_only(tmp_path: Path) -> None:
    signing.generate_key(tmp_path / "k")
    assert (tmp_path / "k").stat().st_mode & 0o777 == 0o600
