from __future__ import annotations

from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import (
    ObservationEnvelope,
    ObservationRejection,
)

from ._operational_base import (
    PRODUCER_REVISION,
    OperationalSnapshotAdapter,
    mapping,
    nonnegative_int,
)
from .base import AdapterBatch
from .snapshot_support import primitive_fields, read_source, source_timestamp


def quota_status(payload: Mapping[str, Any]) -> str:
    raw_status = str(payload.get("status", "")).strip().lower()
    ingest = mapping(payload.get("ingest"))
    totals = mapping(payload.get("totals"))
    exceeded = nonnegative_int(totals.get("quota_exceeded_events"))
    coverage = ingest.get("coverage_ok")
    if not raw_status or exceeded is None or not isinstance(coverage, bool):
        return "unknown"
    if raw_status == "ok" and coverage is True and exceeded == 0:
        return "good"
    return "bad"


class YouTubeApiQuotaAdapter(OperationalSnapshotAdapter):
    domain = "api_quota"
    source = "youtube_api_quota"
    timestamp_keys = ("effective_end_utc",)
    freshness_limit_sec = 900

    @staticmethod
    def _window(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return mapping(payload.get("window"))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        # The safe projection intentionally retains the source time inside
        # ``window``. Build an in-memory view; do not rewrite the input file.
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source=self.source,
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        projected = dict(snapshot.payload)
        projected["effective_end_utc"] = self._window(snapshot.payload).get(
            "effective_end_utc"
        )
        synthetic = type(snapshot)(
            payload=projected,
            sha256=snapshot.sha256,
            size=snapshot.size,
            mtime_ns=snapshot.mtime_ns,
            ctime_ns=snapshot.ctime_ns,
            device=snapshot.device,
            inode=snapshot.inode,
            mode=snapshot.mode,
            uid=snapshot.uid,
            gid=snapshot.gid,
            received_at=snapshot.received_at,
        )
        observed_at = source_timestamp(
            synthetic,
            self.timestamp_keys,
            source=self.source,
            received_at=snapshot.received_at,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = self.status(snapshot.payload)
        observation = ObservationEnvelope.create(
            domain=self.domain,
            source=self.source,
            source_event_id=snapshot.sha256,
            source_generation=snapshot.sha256,
            evidence_role="current_authoritative",
            status=status,
            reason_code=f"{self.source}_{status}",
            observed_at=observed_at,
            received_at=snapshot.received_at,
            freshness_limit_sec=self.freshness_limit_sec,
            producer_revision=PRODUCER_REVISION,
            payload=self.safe_payload(snapshot.payload),
        )
        return AdapterBatch(
            self.source,
            (observation,),
            tuple(rejections),
            read_succeeded=True,
        )

    def status(self, payload: Mapping[str, Any]) -> str:
        return quota_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "status": str(payload.get("status", ""))[:32],
            "target_day": str(payload.get("target_day", ""))[:32],
            "window": primitive_fields(
                self._window(payload),
                ("open_day", "start_utc", "end_utc", "effective_end_utc", "lag_sec"),
            ),
            "totals": primitive_fields(
                mapping(payload.get("totals")),
                ("calls", "units", "quota_exceeded_events"),
            ),
            "ingest": primitive_fields(
                mapping(payload.get("ingest")),
                (
                    "log_exists",
                    "coverage_ok",
                    "parse_errors",
                    "missing_ts",
                    "coverage_observed_ratio",
                    "coverage_gap_start_sec",
                    "coverage_gap_end_sec",
                ),
            ),
        }
