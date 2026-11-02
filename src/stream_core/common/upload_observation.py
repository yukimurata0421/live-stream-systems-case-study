"""Host-local upload adapter for direct scrapes; no remote reads or effects."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from . import upload_samples as samples
from .observation_contract import Observation, Validity

SOURCE = "upload"
SCOPE = "encoder_tcp_send"


@dataclass(frozen=True)
class UploadObservation:
    observation: Observation
    collector_available: bool
    mbps: float | None = None
    interval_seconds: float | None = None


def unavailable(reason, *, collector_available=False):
    return UploadObservation(Observation(SOURCE, SCOPE, Validity.UNAVAILABLE, reason,
        max_age_seconds=samples.FRESH_SECONDS), collector_available)


def collect(directory: Path, *, host: str, identity: dict, capture=None) -> UploadObservation:
    """Use the original local boottime; reading a file never renews its age.

    A stopped sampler and a recent measurement are separate facts. Its last
    measurement may remain valid until the original 180-second expiry.
    """
    capture = samples.clock.capture if capture is None else capture
    try:
        status = samples.read_json(directory / "status.json")
        rows = samples.journal_rows(directory)
        now = capture()  # After I/O so a concurrently written sample is not future.
        if (status.get("schema") != "stream.upload_sampler_status.v1"
                or status.get("source_host") != host
                or status.get("machine_id_sha256") != identity["machine_id_sha256"]
                or status.get("boot_id") != identity["boot_id"]):
            return unavailable("sampler_identity_mismatch")
        if not samples.finite(status.get("source_mono")) or status["source_mono"] < 0:
            return unavailable("sampler_time_invalid")
        status_age = now["mono"] - status["source_mono"]
        if not samples.finite(status_age) or status_age < -samples.clock.EPSILON:
            return unavailable("sampler_time_invalid")
        collector_available = (status_age <= 20 and status.get("status") in {"READY", "WAITING"})
        candidates = []
        for row in rows:
            if (row.get("source_host") != host
                    or row.get("measurement_epoch") != status.get("measurement_epoch")):
                continue
            try:
                age = samples.age_upper(row, now=now, local_identity=identity,
                                        expected_machine=identity["machine_id_sha256"])
            except (ValueError, TypeError, KeyError, OverflowError):
                continue
            candidates.append((age, row))
        if not candidates:
            reason = "baseline_pending" if collector_available else "measurement_unavailable"
            return unavailable(reason, collector_available=collector_available)
        age, row = min(candidates, key=lambda item: item[0])
        observation = Observation.measured(source=SOURCE, scope=SCOPE,
            age_upper_seconds=age, max_age_seconds=samples.FRESH_SECONDS)
        # A valid zero rate is a measurement. Neither zero nor positive bytes
        # proves end-to-end delivery, so target_state stays UNKNOWN.
        return UploadObservation(observation, collector_available,
            row["mbps"] if observation.validity == Validity.VALID else None,
            row["sample_interval_sec"] if observation.validity == Validity.VALID else None)
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        return unavailable("measurement_unavailable")
