from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def contract_object(
    value: object,
    *,
    schema: str,
    fields: frozenset[str],
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{schema} contract must be an object")
    if value.get("schema") != schema:
        raise ValueError(f"unsupported contract schema: {value.get('schema')!r}")
    expected = fields | {"schema"}
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise ValueError(f"{schema} fields invalid: missing={missing!r} extra={extra!r}")
    return value


def text(value: Mapping[str, Any], field: str) -> str:
    item = value[field]
    if not isinstance(item, str):
        raise ValueError(f"{field} must be a string")
    return item


def boolean(value: Mapping[str, Any], field: str) -> bool:
    item = value[field]
    if not isinstance(item, bool):
        raise ValueError(f"{field} must be a boolean")
    return item


def integer(value: Mapping[str, Any], field: str) -> int:
    item = value[field]
    if type(item) is not int:
        raise ValueError(f"{field} must be an integer")
    return item


def optional_number(value: Mapping[str, Any], field: str) -> int | float | None:
    item = value[field]
    if item is None:
        return None
    if isinstance(item, bool) or not isinstance(item, (int, float)):
        raise ValueError(f"{field} must be a number or null")
    return item


def string_array(value: Mapping[str, Any], field: str) -> tuple[str, ...]:
    item = value[field]
    if not isinstance(item, list) or not all(isinstance(entry, str) for entry in item):
        raise ValueError(f"{field} must be an array of strings")
    return tuple(item)


def object_value(value: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    item = value[field]
    if not isinstance(item, Mapping):
        raise ValueError(f"{field} must be an object")
    return item


def object_array(value: Mapping[str, Any], field: str) -> tuple[Mapping[str, Any], ...]:
    item = value[field]
    if not isinstance(item, list) or not all(isinstance(entry, Mapping) for entry in item):
        raise ValueError(f"{field} must be an array of objects")
    return tuple(item)
