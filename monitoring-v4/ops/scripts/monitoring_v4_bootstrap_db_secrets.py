#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import secrets
import subprocess
from typing import Any


NAMESPACE = "stream-monitoring-v4"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Create missing Monitoring v4 database Secrets without printing values")
    result.add_argument("--kubectl", default="/usr/local/bin/k3s")
    return result


def _encoded(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _existing(kubectl: str, name: str) -> dict[str, Any] | None:
    result = subprocess.run(
        [kubectl, "kubectl", "-n", NAMESPACE, "get", "secret", name, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).lower()
        if "notfound" in detail or "not found" in detail:
            return None
        raise RuntimeError(f"failed to read existing Secret: {name}")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"existing Secret is not valid JSON: {name}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"existing Secret is not an object: {name}")
    metadata = payload.get("metadata")
    data = payload.get("data")
    if (
        payload.get("kind") != "Secret"
        or not isinstance(metadata, dict)
        or metadata.get("name") != name
        or metadata.get("namespace") != NAMESPACE
        or not isinstance(metadata.get("resourceVersion"), str)
        or not metadata["resourceVersion"]
        or not isinstance(data, dict)
        or not all(isinstance(key, str) and isinstance(value, str) for key, value in data.items())
    ):
        raise RuntimeError(f"existing Secret has an invalid structure: {name}")
    return payload


def _replace(kubectl: str, payload: dict[str, Any]) -> bool:
    result = subprocess.run(
        [kubectl, "kubectl", "replace", "-f", "-"],
        input=json.dumps(payload),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )
    if result.returncode == 0:
        return True
    if "conflict" in result.stderr.lower() or "object has been modified" in result.stderr.lower():
        return False
    raise RuntimeError("failed to extend existing database Secret")


def _create(kubectl: str, payload: dict[str, Any]) -> bool:
    result = subprocess.run(
        [kubectl, "kubectl", "create", "-f", "-"],
        input=json.dumps(payload),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )
    if result.returncode == 0:
        return True
    if "alreadyexists" in result.stderr.lower() or "already exists" in result.stderr.lower():
        return False
    raise RuntimeError("failed to create database Secret")


def _secret(name: str, data: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "type": "Opaque",
        "data": {key: _encoded(value) for key, value in data.items()},
    }


def _extend_existing(
    kubectl: str,
    name: str,
    current: dict[str, Any],
    values: dict[str, str],
) -> list[str]:
    for _attempt in range(4):
        data = dict(current["data"])
        missing = sorted(key for key in values if key not in data)
        if not missing:
            return []
        for key in missing:
            data[key] = _encoded(values[key])
        resource_version = current["metadata"]["resourceVersion"]
        if _replace(
            kubectl,
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": name,
                    "namespace": NAMESPACE,
                    "resourceVersion": resource_version,
                },
                "type": current.get("type", "Opaque"),
                "data": data,
            },
        ):
            return missing
        refreshed = _existing(kubectl, name)
        if refreshed is None:
            raise RuntimeError(f"Secret disappeared during extension: {name}")
        current = refreshed
    raise RuntimeError(f"Secret extension kept conflicting: {name}")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    resources = {
        "postgres-admin": {
            "database": "stream_monitoring_v4",
            "username": "postgres",
            "password": secrets.token_urlsafe(64),
        },
        "database-roles": {
            "migrator-password": secrets.token_urlsafe(64),
            "core-password": secrets.token_urlsafe(64),
            "exporter-password": secrets.token_urlsafe(64),
            "reporter-password": secrets.token_urlsafe(64),
            "backup-password": secrets.token_urlsafe(64),
            "notifier-password": secrets.token_urlsafe(64),
            "maintenance-password": secrets.token_urlsafe(64),
        },
    }
    created: list[str] = []
    existing: list[str] = []
    extended: dict[str, list[str]] = {}
    for name, values in resources.items():
        current = _existing(args.kubectl, name)
        if current is not None:
            missing = _extend_existing(args.kubectl, name, current, values)
            if missing:
                extended[name] = missing
            else:
                existing.append(name)
            continue
        if _create(args.kubectl, _secret(name, values)):
            created.append(name)
            continue
        # Another bootstrap won the create race. Preserve its values and only
        # add keys absent from that now-authoritative Secret.
        raced = _existing(args.kubectl, name)
        if raced is None:
            raise RuntimeError(f"Secret disappeared after create race: {name}")
        missing = _extend_existing(args.kubectl, name, raced, values)
        if missing:
            extended[name] = missing
        else:
            existing.append(name)
    print(
        json.dumps(
            {
                "created": created,
                "existing": existing,
                "extended_keys": extended,
                "values_printed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
