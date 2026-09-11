from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from stream_monitoring_v4.adapters.json_file import strict_json_loads


REQUIRED_COMPONENT_COUNTS = {
    "database": 1,
    "input-projector": 1,
    "youtube-api-collector": 1,
    "core": 1,
    "reporter": 1,
    "exporter": 2,
}
APP_COMPONENTS = frozenset(
    {"input-projector", "youtube-api-collector", "core", "reporter", "exporter"}
)
ALLOWED_AUXILIARY_COMPONENTS = frozenset(
    {"backup", "backup-retention", "migration", "maintenance", "verification"}
)
MUTATING_AUXILIARY_COMPONENTS = frozenset({"migration"})
REPORT_SCHEMA = "monitoring_v4.shadow_evidence_report.v4"
SENTINEL_SCHEMA = "monitoring_v4.k3s_host_sentinel.v6"
POSTGRES_IMAGE = "postgres@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")


def json_object(text: str) -> Mapping[str, Any]:
    payload = strict_json_loads(text)
    if not isinstance(payload, Mapping):
        raise ValueError("JSON root must be an object")
    return payload


def mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("value must be an object")
    return value


def mapping_list(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError("value must be an array of objects")
    return list(value)


def text(value: Mapping[str, Any], field: str) -> str:
    item = value.get(field)
    if not isinstance(item, str):
        raise ValueError(f"{field} must be a string")
    return item


def integer(value: Mapping[str, Any], field: str) -> int:
    item = value.get(field)
    if type(item) is not int:
        raise ValueError(f"{field} must be an integer")
    return item
