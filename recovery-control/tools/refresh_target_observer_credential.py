#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]


def token_metadata(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("TOKEN_NOT_JWT")
    payload = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
    value = json.loads(base64.urlsafe_b64decode(payload))
    if not isinstance(value, dict):
        raise ValueError("TOKEN_PAYLOAD_INVALID")
    return {
        "subject": str(value.get("sub") or ""),
        "issued_at": int(value.get("iat") or 0),
        "expires_at": int(value.get("exp") or 0),
        "audience": value.get("aud") or [],
    }


def create_token(*, kubectl: str, namespace: str, service_account: str, duration: str) -> str:
    completed = subprocess.run(
        [
            kubectl,
            "-n",
            namespace,
            "create",
            "token",
            service_account,
            f"--duration={duration}",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    token = completed.stdout.strip()
    if not token:
        raise RuntimeError("TOKEN_RESPONSE_EMPTY")
    return token


def replace_token(path: Path, token: str) -> None:
    stat = path.stat()
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    users = config.get("users") if isinstance(config, dict) else None
    if not isinstance(users, list) or len(users) != 1:
        raise ValueError("KUBECONFIG_USER_CARDINALITY")
    user = users[0].get("user") if isinstance(users[0], dict) else None
    if not isinstance(user, dict) or "token" not in user:
        raise ValueError("KUBECONFIG_TOKEN_FIELD_MISSING")
    user["token"] = token
    data = yaml.safe_dump(config, sort_keys=False).encode()
    descriptor, raw_temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw_temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, stat.st_mode & 0o777)
        os.chown(temporary, stat.st_uid, stat.st_gid)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def existing_token(path: Path) -> str:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    users = config.get("users") if isinstance(config, dict) else None
    if not isinstance(users, list) or len(users) != 1:
        raise ValueError("KUBECONFIG_USER_CARDINALITY")
    user = users[0].get("user") if isinstance(users[0], dict) else None
    if not isinstance(user, dict):
        raise ValueError("KUBECONFIG_USER_INVALID")
    token = str(user.get("token") or "")
    if not token:
        raise ValueError("KUBECONFIG_TOKEN_FIELD_MISSING")
    return token


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--namespace", default="stream-v3")
    parser.add_argument("--service-account", default="cra-phase4-target-observer")
    parser.add_argument("--duration", default="24h")
    parser.add_argument("--min-remaining-seconds", type=int, default=43200)
    args = parser.parse_args()
    expected_subject = f"system:serviceaccount:{args.namespace}:{args.service_account}"
    current_metadata = token_metadata(existing_token(args.kubeconfig))
    remaining = current_metadata["expires_at"] - int(time.time())
    if current_metadata["subject"] == expected_subject and remaining > max(0, args.min_remaining_seconds):
        print(
            json.dumps(
                {
                    "credential": "CURRENT_CREDENTIAL_RETAINED",
                    "subject": current_metadata["subject"],
                    "issued_at": current_metadata["issued_at"],
                    "expires_at": current_metadata["expires_at"],
                    "remaining_seconds": remaining,
                    "rbac_changed": False,
                    "secret_value_recorded": False,
                },
                sort_keys=True,
            )
        )
        return 0
    token = create_token(
        kubectl=args.kubectl,
        namespace=args.namespace,
        service_account=args.service_account,
        duration=args.duration,
    )
    metadata = token_metadata(token)
    if metadata["subject"] != expected_subject:
        raise RuntimeError("TOKEN_SUBJECT_MISMATCH")
    if metadata["expires_at"] <= metadata["issued_at"]:
        raise RuntimeError("TOKEN_EXPIRY_INVALID")
    replace_token(args.kubeconfig, token)
    stat = args.kubeconfig.stat()
    print(
        json.dumps(
            {
                "credential": "ROTATED_WITHOUT_VALUE_DISCLOSURE",
                "subject": metadata["subject"],
                "issued_at": metadata["issued_at"],
                "expires_at": metadata["expires_at"],
                "audience": metadata["audience"],
                "mode": oct(stat.st_mode & 0o777),
                "owner_uid": stat.st_uid,
                "owner_gid": stat.st_gid,
                "rbac_changed": False,
                "secret_value_recorded": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
