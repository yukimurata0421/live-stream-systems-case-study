#!/usr/bin/env python3
"""Render an observation-only rule from the pinned producer inventory."""
import argparse
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from stream_v3_release_audit import AUDIT_MAX_AGE_SECONDS, load

SCRAPE_MAX_AGE_SECONDS = 45


def render(registry, source_hash, config_hash, instance):
    components = registry["components"]
    if not 0 < len(components) <= 32 or len({x["name"] for x in components}) != len(components):
        raise ValueError("invalid producer inventory")
    if not all(re.fullmatch("[0-9a-f]{64}", x) for x in [source_hash, config_hash]):
        raise ValueError("invalid expected digest")
    if not re.fullmatch(r"[a-zA-Z0-9_.:-]{1,200}", instance):
        raise ValueError("invalid instance")
    bindings = sum(bool(x.get("unit") and x.get("entrypoint")) for x in components)
    selector = '{job="stream_v3_monitor_observation",instance="' + instance + '"}'
    confirmation = (f"(count(stream_v3_observed_release_configured_entrypoint_match{selector}) == {bindings}) "
                    f"and (min(stream_v3_observed_release_configured_entrypoint_match{selector}) == 1)") if bindings else "vector(1)"
    template = Path(__file__).resolve().parents[2] / "ops/monitoring/observation/provenance-alerts.yml.in"
    text = template.read_text()
    values = {"MONITOR_INSTANCE": instance, "PRODUCER_COUNT": str(len(components)),
              "ENTRYPOINT_CONFIRMATION": confirmation, "AUDIT_MAX_AGE": str(AUDIT_MAX_AGE_SECONDS),
              "SCRAPE_MAX_AGE": str(SCRAPE_MAX_AGE_SECONDS), "SOURCE_HASH": source_hash, "CONFIG_HASH": config_hash}
    for key, value in values.items():
        text = text.replace("@" + key + "@", value)
    if "@" in text:
        raise ValueError("unresolved rule template")
    return text


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--source-hash", required=True)
    p.add_argument("--config-hash", required=True)
    p.add_argument("--instance", required=True)
    a = p.parse_args()
    print(render(load(a.registry), a.source_hash, a.config_hash, a.instance), end="")
