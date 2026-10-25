"""Bounded JSON decoding shared by authority and facts-only observation paths."""

from __future__ import annotations

import json
import math
from typing import Any, NoReturn

MAXIMUM_DEPTH = 32
MAXIMUM_NODES = 32768


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            # Do not echo untrusted names/values into status or logs.
            raise ValueError("JSON_INPUT_DUPLICATE_KEY")
        result[key] = value
    return result


def _constant(_value: str) -> NoReturn:
    raise ValueError("JSON_INPUT_NONFINITE_NUMBER")


def _float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("JSON_INPUT_NONFINITE_NUMBER")
    return result


def load_object(raw: bytes, *, maximum_bytes: int, maximum_nodes: int = MAXIMUM_NODES) -> dict[str, Any]:
    if type(maximum_nodes) is not int or maximum_nodes < 1:
        raise ValueError("JSON_INPUT_NODE_LIMIT_INVALID")
    if len(raw) > maximum_bytes:
        raise ValueError("JSON_INPUT_TOO_LARGE")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("JSON_INPUT_INVALID_UTF8") from error
    # Bound recursive parser work before parsing; ignore braces inside strings.
    depth = 0
    quoted = escaped = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            if depth > MAXIMUM_DEPTH:
                raise ValueError("JSON_INPUT_TOO_DEEP")
        elif char in "]}":
            depth -= 1
    value = json.loads(text, object_pairs_hook=_object, parse_constant=_constant, parse_float=_float)
    if not isinstance(value, dict):
        raise ValueError("JSON_INPUT_NOT_OBJECT")
    stack: list[Any] = [value]
    nodes = 0
    while stack:
        item = stack.pop()
        nodes += 1
        if nodes > maximum_nodes:
            raise ValueError("JSON_INPUT_TOO_MANY_NODES")
        if isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, str):
            try:
                item.encode("utf-8")
            except UnicodeEncodeError as error:
                raise ValueError("JSON_INPUT_INVALID_UNICODE") from error
    return value
