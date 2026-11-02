"""Project the shadow recording rules, without reclassifying delivery health.

This reader runs beside Prometheus on arena. Query timestamps belong to that
host; source age has already been established by the host-local adapter.
"""
from __future__ import annotations

from . import observation_time
from .observation_contract import Observation, Validity, finite
from .upload_observation import SOURCE, SCOPE
from .upload_samples import FRESH_SECONDS

JOB = "stream_v3_observation_shadow"
FRESH = "stream_v3:observation_fresh"
AGE = "stream_v3:observation_age_upper_seconds"
RATE = "stream_v3:upload_observed_mbps"
MAX_QUERY_AGE = 30.


def still_valid(payload, *, now=None, exchange=None):
    """A saved projection retains its original receipt; rereads never renew it.

    The caller supplies an authenticated exchange when crossing a host boundary.
    This does not classify the service or update the saved projection.
    """
    try:
        remaining = payload["valid_for_seconds"]
        if (payload["schema"] != "stream.observation_projection.v1"
                or payload["observation"]["validity"] != "VALID"
                or not finite(remaining) or not 0 < remaining <= MAX_QUERY_AGE):
            return False
        return observation_time.qualify(payload["observation_time"], now=now,
            source="observation_projection", max_age=remaining, exchange=exchange).valid
    except (TypeError, KeyError):
        return False


def project(payload, *, now_wall, elapsed_seconds, instance, host, job=JOB):
    def unknown(reason):
        return {"schema": "stream.observation_projection.v1",
                "observation": Observation(SOURCE, SCOPE, Validity.UNAVAILABLE, reason).payload(),
                "valid_for_seconds": 0., "upload_mbps": None}

    try:
        if (not finite(now_wall) or not finite(elapsed_seconds)
                or not 0 <= elapsed_seconds <= 3):
            return unknown("query_time_invalid")
        if payload.get("status") != "success" or payload["data"]["resultType"] != "vector":
            return unknown("query_unavailable")
        values, stamps = {}, set()
        for row in payload["data"]["result"]:
            metric = row["metric"]
            if any(metric.get(key) != value for key, value in
                   {"job": job, "instance": instance, "host": host, "source": SOURCE, "scope": SCOPE}.items()):
                return unknown("query_identity_mismatch")
            name = metric["__name__"]
            if name not in {FRESH, AGE, RATE} or name in values:
                return unknown("query_ambiguous")
            stamp, value = row["value"]
            stamp, value = float(stamp), float(value)
            if not finite(stamp) or not finite(value) or not 0 <= now_wall - stamp <= MAX_QUERY_AGE:
                return unknown("query_expired")
            values[name] = value
            stamps.add(stamp)
        if len(stamps) != 1 or set(values) != {FRESH, AGE, RATE} or values[FRESH] != 1:
            return unknown("observation_unconfirmed")
        elapsed = now_wall - stamps.pop() + elapsed_seconds
        age = values[AGE] + elapsed
        if values[AGE] < 0 or values[RATE] < 0 or age > FRESH_SECONDS or elapsed >= MAX_QUERY_AGE:
            return unknown("observation_expired")
        return {"schema": "stream.observation_projection.v1",
                "observation": Observation.measured(source=SOURCE, scope=SCOPE,
                    age_upper_seconds=age, max_age_seconds=FRESH_SECONDS).payload(),
                "valid_for_seconds": min(MAX_QUERY_AGE - elapsed, FRESH_SECONDS - age),
                "upload_mbps": values[RATE]}
    except (TypeError, KeyError, ValueError, OverflowError, AttributeError):
        return unknown("query_invalid")
