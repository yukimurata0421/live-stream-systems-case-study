from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import psycopg
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from jsonschema import Draft202012Validator, FormatChecker
from psycopg.rows import dict_row

from cra_authority.monitoring_evidence import reject_control_semantics
from cra_dell_recovery.canonical import KeyRing, canonical_json
from cra_dell_recovery.observation import (
    DellObservationBundle,
    DellObservationContract,
    SignedDellObservationFileSource,
)
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now

CONFIG_SCHEMA = "monitoring_v4.cra_live_adapter_config.v1"
FACT_SCHEMA = "monitoring_v4.cra_fact_bundle.v1"
SAFE_ERROR = re.compile(r"^[A-Z][A-Z0-9_:.-]{0,199}$")
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,239}$")
DATABASE_SCHEMA = "public"
READ_TABLES = (
    "shadow_cycles",
    "domain_current",
    "incident_episodes",
    "incident_candidates",
    "observations",
    "component_health",
)
CONSISTENCY_RETRY_DELAYS_SECONDS = (0.25, 0.5)
VALIDITY_RETRY_DELAYS_SECONDS = (0.25, 0.5, 1.0)
TEMPORAL_CONSISTENCY_ERRORS = frozenset(
    {
        "LIVE_ADAPTER_DELIVERY_CURRENT_NEWER_THAN_CYCLE",
        "LIVE_ADAPTER_DELIVERY_CURRENT_REDUCED_AFTER_CYCLE",
    }
)


def _secure_read(path: Path, *, maximum_bytes: int, secret: bool = False) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("LIVE_ADAPTER_INPUT_NOT_REGULAR_FILE")
        mode = stat.S_IMODE(metadata.st_mode)
        if mode & (0o077 if secret else 0o022):
            raise ValueError("LIVE_ADAPTER_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("LIVE_ADAPTER_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("LIVE_ADAPTER_INPUT_TOO_LARGE")
    return raw


def _read_json(path: Path, *, maximum_bytes: int = 64 * 1024) -> dict[str, Any]:
    value = json.loads(_secure_read(path, maximum_bytes=maximum_bytes))
    if not isinstance(value, dict):
        raise ValueError("LIVE_ADAPTER_INPUT_NOT_OBJECT")
    return dict(value)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _public_key(path: Path) -> Ed25519PublicKey:
    value = serialization.load_pem_public_key(_secure_read(path, maximum_bytes=64 * 1024))
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("LIVE_ADAPTER_DELL_KEY_NOT_ED25519")
    return value


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_mapping(value: object, code: str) -> dict[str, Any]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError(code) from error
    if not isinstance(decoded, dict):
        raise ValueError(code)
    return dict(decoded)


def _json_strings(value: object, code: str) -> list[str]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError(code) from error
    if not isinstance(decoded, list) or not all(isinstance(item, str) for item in decoded):
        raise ValueError(code)
    return list(decoded)


def _reason_codes(values: Sequence[object]) -> list[str]:
    result: set[str] = set()
    for value in values:
        item = str(value).strip().replace(" ", "_")
        if SAFE_ID.fullmatch(item):
            result.add(item)
        elif item:
            result.add("source_reason_unparseable")
    return sorted(result)


@dataclass(frozen=True)
class MonitoringLiveSnapshot:
    cycle: dict[str, Any]
    current: dict[str, Any]
    active_episode: dict[str, Any] | None
    latest_closed_episode: dict[str, Any] | None
    candidate: dict[str, Any] | None
    observations: tuple[dict[str, Any], ...]
    component_health: tuple[dict[str, Any], ...]
    database_user: str


class MonitoringLiveRepository(Protocol):
    def latest(self) -> MonitoringLiveSnapshot: ...


class PostgresMonitoringRepository:
    """One repeatable-read, read-only view over the Monitoring facts tables."""

    def __init__(
        self,
        conninfo_file: Path,
        *,
        statement_timeout_ms: int,
        expected_database_role: str,
    ) -> None:
        raw = _secure_read(conninfo_file, maximum_bytes=16 * 1024, secret=True)
        self.conninfo = raw.decode("utf-8").strip()
        if not self.conninfo or "\x00" in self.conninfo:
            raise ValueError("LIVE_ADAPTER_POSTGRES_CONNINFO_INVALID")
        self.statement_timeout_ms = statement_timeout_ms
        self.expected_database_role = expected_database_role

    def latest(self) -> MonitoringLiveSnapshot:
        with psycopg.connect(self.conninfo, row_factory=dict_row) as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            connection.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{self.statement_timeout_ms}ms",),
            )
            identity = connection.execute(
                """SELECT current_user AS database_user,
                          current_setting('transaction_read_only') AS transaction_read_only,
                          role.rolsuper AS is_superuser,
                          role.rolcreaterole AS can_create_role,
                          role.rolcreatedb AS can_create_database,
                          role.rolinherit AS inherits_privileges,
                          role.rolreplication AS can_replicate,
                          role.rolbypassrls AS can_bypass_rls,
                          role.rolcanlogin AS can_login,
                          EXISTS (
                              SELECT 1 FROM pg_auth_members membership
                              WHERE membership.member = role.oid
                          ) AS has_role_membership,
                          has_database_privilege(current_user, current_database(), 'CREATE') AS can_create_in_database,
                          has_schema_privilege(current_user, 'public', 'CREATE') AS can_create_in_schema
                   FROM pg_roles role WHERE role.rolname = current_user"""
            ).fetchone()
            if identity is None:
                raise ValueError("LIVE_ADAPTER_DATABASE_ROLE_IDENTITY_MISSING")
            if identity["database_user"] != self.expected_database_role:
                raise ValueError("LIVE_ADAPTER_DATABASE_ROLE_MISMATCH")
            if identity["transaction_read_only"] != "on":
                raise ValueError("LIVE_ADAPTER_TRANSACTION_NOT_READ_ONLY")
            if identity["can_login"] is not True or identity["inherits_privileges"] is not False:
                raise ValueError("LIVE_ADAPTER_DATABASE_ROLE_ATTRIBUTE_UNSAFE")
            unsafe_capabilities = (
                "is_superuser",
                "can_create_role",
                "can_create_database",
                "can_replicate",
                "can_bypass_rls",
                "has_role_membership",
                "can_create_in_database",
                "can_create_in_schema",
            )
            if any(identity[name] is not False for name in unsafe_capabilities):
                raise ValueError("LIVE_ADAPTER_DATABASE_ROLE_CAPABILITY_UNSAFE")
            for table in READ_TABLES:
                privilege = connection.execute(
                    """SELECT has_table_privilege(current_user, %(table)s, 'INSERT')
                              OR has_table_privilege(current_user, %(table)s, 'UPDATE')
                              OR has_table_privilege(current_user, %(table)s, 'DELETE')
                              OR has_table_privilege(current_user, %(table)s, 'TRUNCATE') AS can_mutate""",
                    {"table": f"{DATABASE_SCHEMA}.{table}"},
                ).fetchone()
                if privilege is None or privilege["can_mutate"] is not False:
                    raise ValueError(f"LIVE_ADAPTER_DATABASE_ROLE_CAN_MUTATE:{table.upper()}")

            cycle = connection.execute(
                """SELECT cycle_id, started_at, completed_at, completed_ts,
                              build_revision, source_revision, observer_json,
                              current_states_json, parity_json,
                              notification_intent_count, real_delivery_enabled,
                              runtime_mutation_enabled
                       FROM public.shadow_cycles
                       ORDER BY completed_ts DESC, cycle_id DESC LIMIT 1"""
            ).fetchone()
            current = connection.execute(
                """SELECT domain, snapshot_id, state, observed_at, reduced_at,
                              valid_until, policy_revision, reducer_revision,
                              reason_codes_json, source_observation_ids_json,
                              payload_json
                       FROM public.domain_current WHERE domain='delivery'"""
            ).fetchone()
            active_rows = connection.execute(
                """SELECT episode_id, domain, status, severity, opened_at,
                              last_bad_at, last_transition_at, policy_revision,
                              summary, reason_codes_json
                       FROM public.incident_episodes
                       WHERE domain='delivery' AND status='active'
                       ORDER BY last_transition_ts DESC, episode_id DESC LIMIT 2"""
            ).fetchall()
            if len(active_rows) > 1:
                raise ValueError("LIVE_ADAPTER_MULTIPLE_ACTIVE_DELIVERY_EPISODES")
            active = active_rows[0] if active_rows else None
            closed = connection.execute(
                """SELECT episode_id, domain, status, severity, opened_at,
                              last_bad_at, closed_at, last_transition_at,
                              policy_revision, summary, reason_codes_json
                       FROM public.incident_episodes
                       WHERE domain='delivery' AND status='closed'
                       ORDER BY last_transition_ts DESC, episode_id DESC LIMIT 1"""
            ).fetchone()
            candidate = connection.execute(
                """SELECT domain, state, first_seen_at, last_seen_at, samples,
                              snapshot_id, reason_codes_json
                       FROM public.incident_candidates WHERE domain='delivery'"""
            ).fetchone()
            components = connection.execute(
                """SELECT component, status, checked_at, detail
                       FROM public.component_health ORDER BY component"""
            ).fetchall()
            if cycle is None:
                raise ValueError("LIVE_ADAPTER_MONITORING_CYCLE_MISSING")
            if current is None:
                raise ValueError("LIVE_ADAPTER_DELIVERY_CURRENT_MISSING")
            source_ids = _json_strings(
                current["source_observation_ids_json"],
                "LIVE_ADAPTER_SOURCE_OBSERVATION_IDS_INVALID",
            )
            observations: list[dict[str, Any]] = []
            if source_ids:
                observations = list(
                    connection.execute(
                        """SELECT observation_id, domain, source, source_generation,
                                      evidence_role, status, reason_code, observed_at,
                                      producer_revision, payload_sha256
                               FROM public.observations
                               WHERE observation_id = ANY(%s)
                               ORDER BY observed_ts, observation_id""",
                        (source_ids,),
                    ).fetchall()
                )
            if {str(row["observation_id"]) for row in observations} != set(source_ids):
                raise ValueError("LIVE_ADAPTER_SOURCE_OBSERVATION_MISSING")
            return MonitoringLiveSnapshot(
                cycle=dict(cycle),
                current=dict(current),
                active_episode=dict(active) if active is not None else None,
                latest_closed_episode=dict(closed) if closed is not None else None,
                candidate=dict(candidate) if candidate is not None else None,
                observations=tuple(dict(row) for row in observations),
                component_health=tuple(dict(row) for row in components),
                database_user=str(identity["database_user"]),
            )


@dataclass(frozen=True)
class MonitoringLiveAdapterConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> MonitoringLiveAdapterConfig:
        value = _read_json(path)
        required = {
            "schema",
            "adapter_release_id",
            "source_instance_id",
            "source_release_id",
            "expected_monitoring_build_revision",
            "expected_monitoring_source_revision",
            "target_id",
            "expected_database_role",
            "postgres_conninfo_file",
            "dell_observation_file",
            "dell_observation_schema_file",
            "dell_key_id",
            "dell_public_key_file",
            "dell_source_instance_id",
            "dell_source_release_id",
            "expected_target",
            "fact_bundle_schema_file",
            "output_file",
            "target_snapshot_output_file",
            "status_file",
            "required_components",
            "maximum_cycle_age_seconds",
            "maximum_component_age_seconds",
            "fact_ttl_seconds",
            "statement_timeout_ms",
        }
        if set(value) != required:
            raise ValueError("LIVE_ADAPTER_CONFIG_FIELDS_NOT_EXACT")
        if value["schema"] != CONFIG_SCHEMA:
            raise ValueError("LIVE_ADAPTER_CONFIG_SCHEMA_UNSUPPORTED")
        for name in (
            "adapter_release_id",
            "source_instance_id",
            "source_release_id",
            "expected_monitoring_build_revision",
            "expected_monitoring_source_revision",
            "target_id",
            "expected_database_role",
            "dell_key_id",
            "dell_source_instance_id",
            "dell_source_release_id",
        ):
            if not isinstance(value[name], str) or not value[name].strip():
                raise ValueError(f"LIVE_ADAPTER_CONFIG_{name.upper()}_MISSING")
        require_runtime_release(value["adapter_release_id"], "LIVE_ADAPTER_RUNTIME_RELEASE_MISMATCH")
        require_runtime_release(value["source_release_id"], "LIVE_ADAPTER_SOURCE_RELEASE_MISMATCH")
        expected = value["expected_target"]
        if not isinstance(expected, dict) or set(expected) != {"host_id", "namespace", "container_name"}:
            raise ValueError("LIVE_ADAPTER_EXPECTED_TARGET_FIELDS_NOT_EXACT")
        if expected["container_name"] != "stream-engine" or not all(
            isinstance(expected[name], str) and expected[name] for name in expected
        ):
            raise ValueError("LIVE_ADAPTER_EXPECTED_TARGET_INVALID")
        components = value["required_components"]
        if (
            not isinstance(components, list)
            or not components
            or len(components) != len(set(components))
            or not all(isinstance(item, str) and SAFE_ID.fullmatch(item) for item in components)
        ):
            raise ValueError("LIVE_ADAPTER_REQUIRED_COMPONENTS_INVALID")
        for name, lower, upper in (
            ("maximum_cycle_age_seconds", 1.0, 300.0),
            ("maximum_component_age_seconds", 1.0, 300.0),
            ("fact_ttl_seconds", 1.0, 60.0),
        ):
            raw = value[name]
            if (
                isinstance(raw, bool)
                or not isinstance(raw, (int, float))
                or not math.isfinite(float(raw))
                or not lower <= float(raw) <= upper
            ):
                raise ValueError(f"LIVE_ADAPTER_CONFIG_{name.upper()}_OUT_OF_RANGE")
        timeout = value["statement_timeout_ms"]
        if isinstance(timeout, bool) or not isinstance(timeout, int) or not 100 <= timeout <= 30_000:
            raise ValueError("LIVE_ADAPTER_CONFIG_STATEMENT_TIMEOUT_MS_OUT_OF_RANGE")
        return cls(value, path)


class MonitoringLiveAdapter:
    """Converts facts-only Monitoring and Dell snapshots; it has no control API."""

    def __init__(
        self,
        config: MonitoringLiveAdapterConfig,
        repository: MonitoringLiveRepository | None = None,
        *,
        consistency_retry_delays: Sequence[float] = CONSISTENCY_RETRY_DELAYS_SECONDS,
        validity_retry_delays: Sequence[float] = VALIDITY_RETRY_DELAYS_SECONDS,
    ) -> None:
        self.config = config
        value = config.value
        self.repository = repository or PostgresMonitoringRepository(
            Path(value["postgres_conninfo_file"]),
            statement_timeout_ms=int(value["statement_timeout_ms"]),
            expected_database_role=str(value["expected_database_role"]),
        )
        if any(
            isinstance(delay, bool) or not isinstance(delay, (int, float)) or not math.isfinite(float(delay)) or delay < 0
            for delay in consistency_retry_delays
        ):
            raise ValueError("LIVE_ADAPTER_CONSISTENCY_RETRY_DELAY_INVALID")
        self.consistency_retry_delays = tuple(float(delay) for delay in consistency_retry_delays)
        if any(
            isinstance(delay, bool) or not isinstance(delay, (int, float)) or not math.isfinite(float(delay)) or delay < 0
            for delay in validity_retry_delays
        ):
            raise ValueError("LIVE_ADAPTER_VALIDITY_RETRY_DELAY_INVALID")
        self.validity_retry_delays = tuple(float(delay) for delay in validity_retry_delays)
        self._last_validity_retry_count = 0
        expected = dict(value["expected_target"])
        dell_contract = DellObservationContract(
            Path(value["dell_observation_schema_file"]),
            KeyRing({str(value["dell_key_id"]): _public_key(Path(value["dell_public_key_file"]))}),
            allowed_sources={str(value["dell_source_instance_id"]): str(value["dell_source_release_id"])},
            expected_host_id=str(expected["host_id"]),
            expected_namespace=str(expected["namespace"]),
            expected_container_name=str(expected["container_name"]),
        )
        self.dell_source = SignedDellObservationFileSource(
            Path(value["dell_observation_file"]),
            dell_contract,
        )
        self.fact_validator = Draft202012Validator(
            json.loads(Path(value["fact_bundle_schema_file"]).read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )

    def _latest_consistent_snapshot(self) -> MonitoringLiveSnapshot:
        for attempt in range(len(self.consistency_retry_delays) + 1):
            snapshot = self.repository.latest()
            self._validate_snapshot_identity(snapshot)
            try:
                self._validate_snapshot_consistency(snapshot)
            except ValueError as error:
                if str(error) not in TEMPORAL_CONSISTENCY_ERRORS or attempt == len(self.consistency_retry_delays):
                    raise
                time.sleep(self.consistency_retry_delays[attempt])
                continue
            return snapshot
        raise AssertionError("LIVE_ADAPTER_CONSISTENCY_RETRY_UNREACHABLE")

    def _validate_snapshot_identity(self, snapshot: MonitoringLiveSnapshot) -> None:
        value = self.config.value
        cycle = snapshot.cycle
        if str(cycle["build_revision"]) != str(value["expected_monitoring_build_revision"]):
            raise ValueError("LIVE_ADAPTER_MONITORING_BUILD_REVISION_MISMATCH")
        if str(cycle["source_revision"]) != str(value["expected_monitoring_source_revision"]):
            raise ValueError("LIVE_ADAPTER_MONITORING_SOURCE_REVISION_MISMATCH")
        if int(cycle["real_delivery_enabled"]) != 0 or int(cycle["runtime_mutation_enabled"]) != 0:
            raise ValueError("LIVE_ADAPTER_MONITORING_MUTATION_FLAG_UNSAFE")

    @staticmethod
    def _validate_snapshot_consistency(snapshot: MonitoringLiveSnapshot) -> None:
        cycle = snapshot.cycle
        current = snapshot.current
        completed = parse_utc(str(cycle["completed_at"]))
        current_observed = parse_utc(str(current["observed_at"]))
        current_reduced = parse_utc(str(current["reduced_at"]))
        if current_observed > completed:
            raise ValueError("LIVE_ADAPTER_DELIVERY_CURRENT_NEWER_THAN_CYCLE")
        if current_reduced > completed:
            raise ValueError("LIVE_ADAPTER_DELIVERY_CURRENT_REDUCED_AFTER_CYCLE")
        current_states = _json_mapping(cycle["current_states_json"], "LIVE_ADAPTER_CURRENT_STATES_INVALID")
        if current_states.get("delivery") != current["state"]:
            raise ValueError("LIVE_ADAPTER_CYCLE_CURRENT_STATE_MISMATCH")

    def _build_artifacts(
        self,
        *,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        self._last_validity_retry_count = 0
        for attempt in range(len(self.validity_retry_delays) + 1):
            try:
                return self._build_artifacts_once(now=now)
            except ValueError as error:
                if str(error) != "LIVE_ADAPTER_FACT_HAS_NO_VALIDITY_WINDOW" or attempt == len(self.validity_retry_delays):
                    raise
                self._last_validity_retry_count = attempt + 1
                time.sleep(self.validity_retry_delays[attempt])
        raise AssertionError("LIVE_ADAPTER_VALIDITY_RETRY_UNREACHABLE")

    def _build_artifacts_once(
        self,
        *,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        snapshot = self._latest_consistent_snapshot()
        # A bounded consistency retry can cross a wall-clock boundary. Refresh
        # production time after it so a newly completed cycle is not compared
        # against the pre-retry instant. Explicit test/replay time stays fixed.
        current_time = (now or datetime.now(UTC)).astimezone(UTC)
        dell = self.dell_source.latest(now=current_time)
        value = self.config.value
        cycle = snapshot.cycle
        current = snapshot.current
        parity = _json_mapping(cycle["parity_json"], "LIVE_ADAPTER_PARITY_INVALID")
        completed = parse_utc(str(cycle["completed_at"]))
        current_observed = parse_utc(str(current["observed_at"]))
        current_valid = parse_utc(str(current["valid_until"]))
        cycle_deadline = completed + timedelta(seconds=float(value["maximum_cycle_age_seconds"]))

        parity_clean, projection_clean = self._parity(parity, current)
        required = set(str(item) for item in value["required_components"])
        component_by_name = {str(item["component"]): item for item in snapshot.component_health}
        component_times: list[datetime] = []
        source_ready = True
        for name in required:
            item = component_by_name.get(name)
            if item is None:
                source_ready = False
                continue
            checked = parse_utc(str(item["checked_at"]))
            component_times.append(checked)
            if item["status"] != "good" or checked > current_time:
                source_ready = False
            if (current_time - checked).total_seconds() > float(value["maximum_component_age_seconds"]):
                source_ready = False
        if not component_times:
            raise ValueError("LIVE_ADAPTER_REQUIRED_COMPONENT_HEALTH_MISSING")

        input_fresh = (
            completed <= current_time < cycle_deadline
            and current_observed <= current_time < current_valid
            and dell.observed_at <= current_time < dell.valid_until
        )
        incident, incident_refs = self._incident(snapshot, dell)
        checks = {name: dict(item) for name, item in dell.checks.items()}
        delivery_status = "TRUE" if current["state"] == "bad" else "FALSE" if current["state"] == "good" else "UNKNOWN"
        checks["delivery_bad"] = {
            "status": delivery_status,
            "observed_at": isoformat_utc(current_observed),
            "evidence_ref": f"monitoring-v4/domain-current/{current['snapshot_id']}",
        }
        measurements = [dict(item) for item in dell.value["measurements"]]
        if current["state"] in {"bad", "good"}:
            measurements.append(
                {
                    "name": "delivery_bad",
                    "value": 1 if current["state"] == "bad" else 0,
                    "unit": "boolean",
                    "evidence_ref": f"monitoring-v4/domain-current/{current['snapshot_id']}",
                }
            )
        observed = max(completed, current_observed, dell.observed_at, *component_times)
        valid_until = min(
            cycle_deadline,
            current_valid,
            dell.valid_until,
            *(checked + timedelta(seconds=float(value["maximum_component_age_seconds"])) for checked in component_times),
            observed + timedelta(seconds=float(value["fact_ttl_seconds"])),
        )
        if observed > current_time:
            raise ValueError("LIVE_ADAPTER_FACT_OBSERVED_IN_FUTURE")
        if valid_until <= current_time:
            raise ValueError("LIVE_ADAPTER_FACT_HAS_NO_VALIDITY_WINDOW")
        source = {
            "cycle_id": cycle["cycle_id"],
            "cycle_completed_at": cycle["completed_at"],
            "current_snapshot_id": current["snapshot_id"],
            "current_state": current["state"],
            "parity": parity,
            "incident": incident,
            "dell_observation_id": dell.value["observation_id"],
            "dell_observation_revision": dell.value["observation_revision"],
            "components": sorted(required),
        }
        revision = hashlib.sha256(canonical_json(source)).hexdigest()
        references = {
            f"monitoring-v4/cycle/{cycle['cycle_id']}",
            f"monitoring-v4/domain-current/{current['snapshot_id']}",
            *(f"monitoring-v4/observation/{item['observation_id']}" for item in snapshot.observations),
            *incident_refs,
            *(str(item) for item in dell.value["evidence_refs"]),
        }
        facts: dict[str, Any] = {
            "schema": FACT_SCHEMA,
            "target_id": value["target_id"],
            "source_instance_id": value["source_instance_id"],
            "source_release_id": value["source_release_id"],
            "monitoring_cycle_id": cycle["cycle_id"],
            "observation_revision": revision,
            # Evidence timestamps can remain fixed while another source row at
            # that timestamp changes. Sequence transport issuance, not the
            # maximum evidence timestamp, so distinct fact payloads cannot
            # collide in the durable producer inbox.
            "observation_sequence": int(current_time.timestamp() * 1_000_000),
            "observed_at": isoformat_utc(observed),
            "valid_until": isoformat_utc(valid_until),
            "readiness": {
                "input_fresh": input_fresh,
                "parity_clean": parity_clean,
                "projection_clean": projection_clean,
                "source_ready": source_ready,
            },
            "incident": incident,
            "checks": checks,
            "measurements": measurements,
            "evidence_refs": sorted(references),
        }
        reject_control_semantics(facts)
        self.fact_validator.validate(facts)
        return facts, dict(dell.value["target_snapshot"]), parity

    def build(self, *, now: datetime | None = None) -> dict[str, Any]:
        facts, _, _ = self._build_artifacts(now=now)
        return facts

    @staticmethod
    def _parity(parity: dict[str, Any], current: dict[str, Any]) -> tuple[bool, bool]:
        domains = _mapping(parity.get("domains"))
        delivery = _mapping(domains.get("delivery"))
        integrity = parity.get("input_integrity_errors")
        parity_clean = (
            parity.get("schema") == "monitoring_v4.live_parity.v2"
            and parity.get("equivalent") is True
            and parity.get("accepted_difference_count") == 0
            and parity.get("unclassified_contract_difference_count") == 0
            and integrity == []
            and delivery.get("match") is True
            and delivery.get("classification") == "equivalent"
            and delivery.get("actual_state") == current["state"]
            and delivery.get("expected_state") == current["state"]
            and delivery.get("actual_observed_at") == current["observed_at"]
        )
        projection = _mapping(parity.get("projection_integrity"))
        projection_clean = (
            projection.get("complete") is True
            and type(projection.get("expected_count")) is int
            and type(projection.get("projection_count")) is int
            and projection.get("expected_count") == projection.get("projection_count")
            and projection.get("rejection_count") == 0
            and projection.get("missing_keys") == []
            and projection.get("unexpected_keys") == []
        )
        return parity_clean, projection_clean

    @staticmethod
    def _incident(
        snapshot: MonitoringLiveSnapshot,
        dell: DellObservationBundle,
    ) -> tuple[dict[str, Any], set[str]]:
        current = snapshot.current
        references: set[str] = set()
        if snapshot.active_episode is not None:
            episode = snapshot.active_episode
            incident_id = str(episode["episode_id"])
            source_episode_id = incident_id
            state = "CONFIRMED"
            reasons = _json_strings(episode["reason_codes_json"], "LIVE_ADAPTER_EPISODE_REASONS_INVALID")
            references.add(f"monitoring-v4/incident/{incident_id}")
        elif snapshot.candidate is not None:
            candidate = snapshot.candidate
            source_episode_id = f"delivery-candidate-{candidate['snapshot_id']}"
            incident_id = source_episode_id
            state = "OPEN"
            reasons = _json_strings(candidate["reason_codes_json"], "LIVE_ADAPTER_CANDIDATE_REASONS_INVALID")
            references.add(f"monitoring-v4/incident-candidate/{candidate['snapshot_id']}")
        elif current["state"] == "good" and snapshot.latest_closed_episode is not None:
            episode = snapshot.latest_closed_episode
            incident_id = str(episode["episode_id"])
            source_episode_id = incident_id
            state = "CLEAR"
            reasons = _json_strings(episode["reason_codes_json"], "LIVE_ADAPTER_EPISODE_REASONS_INVALID")
            references.add(f"monitoring-v4/incident/{incident_id}")
        elif current["state"] == "good":
            source_episode_id = f"delivery-clear-{current['snapshot_id']}"
            incident_id = source_episode_id
            state = "CLEAR"
            reasons = _json_strings(current["reason_codes_json"], "LIVE_ADAPTER_CURRENT_REASONS_INVALID")
        else:
            source_episode_id = f"delivery-unconfirmed-{current['snapshot_id']}"
            incident_id = source_episode_id
            state = "OPEN"
            reasons = _json_strings(current["reason_codes_json"], "LIVE_ADAPTER_CURRENT_REASONS_INVALID")
            references.add(f"monitoring-v4/unconfirmed-current/{current['snapshot_id']}")
        if state == "CONFIRMED" and current["state"] == "bad" and dell.checks.get("tcp_stall", {}).get("status") == "CONFIRMED":
            reasons.append("confirmed_tcp_stall")
        return (
            {
                "incident_id": incident_id,
                "source_episode_id": source_episode_id,
                "domain": "delivery",
                "state": state,
                "reason_codes": _reason_codes(reasons),
            },
            references,
        )

    def run_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        # Keep an explicit replay time fixed, but let production retries
        # re-sample wall clock inside each attempt. Reusing the pre-sleep time
        # could accept evidence that expired while waiting for the next cycle.
        facts, target_snapshot, parity = self._build_artifacts(now=now)
        current = (now or datetime.now(UTC)).astimezone(UTC)
        value = self.config.value
        _atomic_json(Path(value["target_snapshot_output_file"]), target_snapshot)
        _atomic_json(Path(value["output_file"]), facts)
        status = {
            "schema": "monitoring_v4.cra_live_adapter_status.v2",
            "status": "READY",
            "adapter_release_id": value["adapter_release_id"],
            "monitoring_cycle_id": facts["monitoring_cycle_id"],
            "observation_sequence": facts["observation_sequence"],
            "readiness": facts["readiness"],
            # Preserve the immutable cycle payload so a downstream soak can
            # distinguish a bounded, later-proven source convergence from an
            # unclassified parity violation. A boolean alone destroys that
            # evidence and makes every asynchronous source transition look
            # terminal.
            "parity": parity,
            "validity_retry_count": self._last_validity_retry_count,
            "observed_at": isoformat_utc(current),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        _atomic_json(Path(value["status_file"]), status)
        return status


def _error_code(error: BaseException) -> str:
    detail = str(error)
    return detail if SAFE_ERROR.fullmatch(detail) else "LIVE_ADAPTER_INPUT_INVALID"


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitoring v4 and Dell facts-only CRA live adapter")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = MonitoringLiveAdapterConfig.load(args.config)
    value = config.value
    try:
        adapter = MonitoringLiveAdapter(config)
        if args.check_config:
            print(
                json.dumps(
                    {
                        "config": "VALID",
                        "schema": value["schema"],
                        "control_capability_count": 0,
                        "physical_effect_count": 0,
                    },
                    sort_keys=True,
                )
            )
            return
        print(json.dumps(adapter.run_once(), separators=(",", ":"), sort_keys=True))
    except Exception as error:
        status = {
            "schema": "monitoring_v4.cra_live_adapter_status.v2",
            "status": "SAFE_BLOCKED",
            "adapter_release_id": value["adapter_release_id"],
            "error_class": type(error).__name__,
            "error_code": _error_code(error),
            "observed_at": isoformat_utc(utc_now()),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        _atomic_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
