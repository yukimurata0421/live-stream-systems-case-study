"""Explicit direct-scrape selection for upload recovery; no health policy."""
from __future__ import annotations

import json
from pathlib import Path
import re

from .upload_samples import read_json

SCHEMA = "stream.upload_prometheus_source.v1"
JOB = "stream_v3_observation"
LABELS = {"job": JOB, "source": "upload", "scope": "encoder_tcp_send"}


def selected(state_root: Path) -> dict | None:
    """Legacy requires an explicit selection; loss of configuration is unknown."""
    value = read_json(state_root / "upload_measurement/prometheus-source.json", 2048)
    if value == {"schema": SCHEMA, "mode": "legacy"}:
        return None
    if value.get("mode", "direct") != "direct":
        raise ValueError("upload_prometheus_mode_invalid")
    if value.get("schema") != SCHEMA or any(value.get(k) != v for k, v in LABELS.items()):
        raise ValueError("upload_prometheus_source_invalid")
    if not isinstance(value.get("host"), str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", value["host"]):
        raise ValueError("upload_prometheus_host_invalid")
    if not isinstance(value.get("instance"), str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}:[0-9]{1,5}", value["instance"]):
        raise ValueError("upload_prometheus_instance_invalid")
    for key in ("source_hash", "config_hash"):
        if not isinstance(value.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", value[key]):
            raise ValueError("upload_prometheus_build_invalid")
    return value


def expected_labels(source: dict) -> dict:
    return {**LABELS, "host": source["host"], "instance": source["instance"]}


def query(source: dict) -> str:
    """Keep original age, reject expired rule/up samples and bind the release."""
    labels = expected_labels(source)
    selector = lambda extra: "{" + ",".join(k + "=" + json.dumps(v) for k, v in sorted(extra.items())) + "}"
    selected_labels = selector(labels)
    age = "stream_v3:observation_age_upper_seconds" + selected_labels
    fresh = "stream_v3:observation_fresh" + selected_labels
    up = "up" + selector({k: labels[k] for k in ("job", "instance")})
    build = "stream_v3_observer_build_info" + selector({**labels, **{k: source[k] for k in ("source_hash", "config_hash")}})
    return (
        f"({age} + time() - timestamp({age}))"
        f" and ({fresh} == 1)"
        f" and (timestamp({age}) == timestamp({fresh}))"
        f" and (time() - timestamp({age}) >= 0)"
        f" and (time() - timestamp({age}) <= 30)"
        f" and on(job,instance) ({up} == 1)"
        f" and on(job,instance) (time() - timestamp({up}) >= 0)"
        f" and on(job,instance) (time() - timestamp({up}) <= 45)"
        f" and on(job,instance,host,source,scope) ({build} == 1)"
    )
