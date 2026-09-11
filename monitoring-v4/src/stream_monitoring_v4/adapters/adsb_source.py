from __future__ import annotations

from typing import Any, Mapping

from ._operational_base import OperationalSnapshotAdapter
from .snapshot_support import primitive_fields, status_word


def adsb_status(payload: Mapping[str, Any]) -> str:
    return status_word(payload.get("status"))


class AdsbSourceAdapter(OperationalSnapshotAdapter):
    domain = "adsb_source"
    source = "adsb_freshness"
    timestamp_keys = ("ts_utc",)
    freshness_limit_sec = 180

    def status(self, payload: Mapping[str, Any]) -> str:
        return adsb_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return primitive_fields(
            payload,
            (
                "status",
                "aircraft_count",
                "source_messages",
                "sample_ts",
                "source_now",
                "reason_present",
            ),
        )
