"""Finite manual route grants; do not replace CRA policy or its producer."""
from __future__ import annotations

import math
import time
from collections.abc import Mapping

from .model import EffectRequest, SWITCH_STREAM_ROUTE


class OperatorRouteGrant:
    def __init__(self, values: Mapping, *, executor_instance: str):
        self.values = dict(values)
        self.executor_instance = executor_instance
        required = {"grant_id", "producer_id", "producer_generation", "peer_uid", "expires_at",
                    "host_id", "namespace", "pod_uid", "container_name", "container_id"}
        if set(self.values) != required or type(self.values["peer_uid"]) is not int or self.values["peer_uid"] != 0:
            raise ValueError("OPERATOR_ROUTE_GRANT_INVALID")
        if (type(self.values["producer_generation"]) is not int or self.values["producer_generation"] <= 0
            or type(self.values["expires_at"]) not in (int, float)
            or not math.isfinite(self.values["expires_at"])):
            raise ValueError("OPERATOR_ROUTE_GRANT_INVALID")
        for name in required - {"producer_generation", "peer_uid", "expires_at"}:
            if not isinstance(self.values[name], str) or not 1 <= len(self.values[name]) <= 256:
                raise ValueError("OPERATOR_ROUTE_GRANT_INVALID")
        self.wall = time.time()
        self.elapsed = time.clock_gettime(time.CLOCK_BOOTTIME)
        remaining = self.values["expires_at"] - self.wall
        if not 0 < remaining <= 86400:
            raise ValueError("OPERATOR_ROUTE_GRANT_LIFETIME_INVALID")
        self.deadline = self.elapsed + remaining

    def validate(self, request: EffectRequest, uid: int) -> str:
        wall, elapsed = time.time(), time.clock_gettime(time.CLOCK_BOOTTIME)
        if (wall < self.wall or abs((wall-self.wall)-(elapsed-self.elapsed)) > 2
            or wall >= self.values["expires_at"] or elapsed >= self.deadline):
            return "OPERATOR_ROUTE_GRANT_EXPIRED_OR_CLOCK_CHANGED"
        self.wall, self.elapsed = wall, elapsed
        if uid != self.values["peer_uid"]:
            return "OPERATOR_ROUTE_PEER_UNAUTHORIZED"
        if request.intent_type != SWITCH_STREAM_ROUTE:
            return "OPERATOR_ROUTE_INTENT_INVALID"
        if ((request.route_transition or {}).get("operator_grant_id") != self.values["grant_id"]
            or request.producer_id != self.values["producer_id"]
            or request.producer_generation != self.values["producer_generation"]
            or request.expected_executor_instance_id != self.executor_instance):
            return "OPERATOR_ROUTE_GRANT_BINDING_MISMATCH"
        target = request.ffmpeg_target_identity or {}
        if any(target.get(name) != self.values[name] for name in
               ("host_id", "namespace", "pod_uid", "container_name", "container_id")):
            return "OPERATOR_ROUTE_TARGET_MISMATCH"
        return ""
