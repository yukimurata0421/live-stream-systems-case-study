from __future__ import annotations

from typing import Any


SCHEMA_VERSION = 6


DDL_V1 = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    domain TEXT NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    evidence_role TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('good', 'bad', 'unknown', 'not_applicable')),
    reason_code TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    observed_ts BIGINT NOT NULL,
    received_at TEXT NOT NULL,
    received_ts BIGINT NOT NULL,
    freshness_limit_sec INTEGER NOT NULL CHECK(freshness_limit_sec > 0),
    producer_revision TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS observations_domain_time_idx
    ON observations(domain, observed_ts DESC, observation_id);
CREATE INDEX IF NOT EXISTS observations_source_time_idx
    ON observations(source, observed_ts DESC, observation_id);

CREATE TABLE IF NOT EXISTS rejections (
    rejection_id TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    source TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    detail TEXT NOT NULL,
    received_at TEXT NOT NULL,
    received_ts BIGINT NOT NULL,
    payload_sha256 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS domain_current (
    domain TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('good', 'bad', 'unknown')),
    observed_at TEXT NOT NULL,
    observed_ts BIGINT NOT NULL,
    reduced_at TEXT NOT NULL,
    reduced_ts BIGINT NOT NULL,
    valid_until TEXT NOT NULL,
    valid_until_ts BIGINT NOT NULL,
    policy_revision TEXT NOT NULL,
    reducer_revision TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    source_observation_ids_json TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS current_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('good', 'bad', 'unknown')),
    observed_at TEXT NOT NULL,
    observed_ts BIGINT NOT NULL,
    reduced_at TEXT NOT NULL,
    reduced_ts BIGINT NOT NULL,
    valid_until TEXT NOT NULL,
    valid_until_ts BIGINT NOT NULL,
    policy_revision TEXT NOT NULL,
    reducer_revision TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    source_observation_ids_json TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS current_snapshots_domain_time_idx
    ON current_snapshots(domain, reduced_ts DESC, snapshot_id);

CREATE TABLE IF NOT EXISTS incident_candidates (
    domain TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('bad', 'unknown')),
    first_seen_at TEXT NOT NULL,
    first_seen_ts BIGINT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_seen_ts BIGINT NOT NULL,
    samples INTEGER NOT NULL CHECK(samples > 0),
    snapshot_id TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS incident_episodes (
    episode_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'closed')),
    severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
    opened_at TEXT NOT NULL,
    opened_ts BIGINT NOT NULL,
    last_bad_at TEXT NOT NULL,
    last_bad_ts BIGINT NOT NULL,
    closed_at TEXT NOT NULL,
    closed_ts BIGINT NOT NULL,
    bad_samples INTEGER NOT NULL,
    unknown_samples INTEGER NOT NULL,
    last_transition_at TEXT NOT NULL,
    last_transition_ts BIGINT NOT NULL,
    next_notification_at TEXT NOT NULL,
    next_notification_ts BIGINT NOT NULL,
    policy_revision TEXT NOT NULL,
    summary TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS incident_one_active_per_domain_idx
    ON incident_episodes(domain) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS incident_transitions (
    transition_id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES incident_episodes(episode_id),
    domain TEXT NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('detected', 'repeat', 'recovered')),
    severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
    occurred_at TEXT NOT NULL,
    occurred_ts BIGINT NOT NULL,
    current_snapshot_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS incident_transitions_episode_time_idx
    ON incident_transitions(episode_id, occurred_ts, transition_id);

CREATE TABLE IF NOT EXISTS incident_processed_currents (
    snapshot_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    processed_ts BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS notification_intents (
    intent_id TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL REFERENCES incident_transitions(transition_id),
    episode_id TEXT NOT NULL REFERENCES incident_episodes(episode_id),
    route TEXT NOT NULL,
    phase TEXT NOT NULL,
    severity TEXT NOT NULL,
    created_at TEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    not_before TEXT NOT NULL,
    not_before_ts BIGINT NOT NULL,
    subject TEXT NOT NULL,
    content TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    route_policy_revision TEXT NOT NULL,
    template_revision TEXT NOT NULL,
    UNIQUE(transition_id, route),
    UNIQUE(dedupe_key)
);
CREATE INDEX IF NOT EXISTS notification_intents_due_idx
    ON notification_intents(not_before_ts, intent_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES notification_intents(intent_id),
    attempt_no INTEGER NOT NULL CHECK(attempt_no > 0),
    owner TEXT NOT NULL,
    started_at TEXT NOT NULL,
    started_ts BIGINT NOT NULL,
    UNIQUE(intent_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS delivery_results (
    result_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE REFERENCES delivery_attempts(attempt_id),
    intent_id TEXT NOT NULL REFERENCES notification_intents(intent_id),
    success INTEGER NOT NULL CHECK(success IN (0, 1)),
    status_code INTEGER NOT NULL,
    detail TEXT NOT NULL,
    retry_after_sec INTEGER NOT NULL,
    completed_at TEXT NOT NULL,
    completed_ts BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_results_intent_time_idx
    ON delivery_results(intent_id, completed_ts DESC, result_id);

CREATE TABLE IF NOT EXISTS leases (
    lease_name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    acquired_ts BIGINT NOT NULL,
    expires_at TEXT NOT NULL,
    expires_ts BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS component_health (
    component TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('good', 'bad', 'unknown')),
    checked_at TEXT NOT NULL,
    checked_ts BIGINT NOT NULL,
    detail TEXT NOT NULL
);
"""


DDL_V2 = """
CREATE TABLE IF NOT EXISTS sli_projections (
    projection_id TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    objective_id TEXT NOT NULL,
    window_name TEXT NOT NULL,
    assessment_scope TEXT NOT NULL CHECK(assessment_scope IN ('formal', 'fast', 'trend', 'supporting')),
    is_official_window INTEGER NOT NULL CHECK(is_official_window IN (0, 1)),
    observed DOUBLE PRECISION,
    eligible DOUBLE PRECISION,
    bad DOUBLE PRECISION,
    missing DOUBLE PRECISION,
    coverage_pct DOUBLE PRECISION,
    source_freshness_pct DOUBLE PRECISION,
    source_disagreement INTEGER NOT NULL CHECK(source_disagreement IN (0, 1)),
    compliance_status TEXT NOT NULL CHECK(compliance_status IN ('met', 'breached', 'unknown')),
    measurement_unknown_reasons_json TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_start_ts BIGINT NOT NULL,
    window_end TEXT NOT NULL,
    window_end_ts BIGINT NOT NULL,
    evaluated_at TEXT NOT NULL,
    evaluated_ts BIGINT NOT NULL,
    policy_revision TEXT NOT NULL,
    evidence_ids_json TEXT NOT NULL,
    no_automatic_recovery INTEGER NOT NULL CHECK(no_automatic_recovery = 1),
    payload_sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sli_projections_objective_time_idx
    ON sli_projections(objective_id, assessment_scope, window_end_ts DESC, projection_id);

CREATE TABLE IF NOT EXISTS sli_projection_current (
    objective_id TEXT NOT NULL,
    assessment_scope TEXT NOT NULL,
    window_name TEXT NOT NULL,
    projection_id TEXT NOT NULL UNIQUE REFERENCES sli_projections(projection_id),
    PRIMARY KEY(objective_id, assessment_scope, window_name)
);

CREATE TABLE IF NOT EXISTS shadow_cycles (
    cycle_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    started_ts BIGINT NOT NULL,
    completed_at TEXT NOT NULL,
    completed_ts BIGINT NOT NULL,
    build_revision TEXT NOT NULL,
    observer_json TEXT NOT NULL,
    current_states_json TEXT NOT NULL,
    parity_json TEXT NOT NULL,
    notification_intent_count INTEGER NOT NULL CHECK(notification_intent_count >= 0),
    real_delivery_enabled INTEGER NOT NULL CHECK(real_delivery_enabled = 0),
    runtime_mutation_enabled INTEGER NOT NULL CHECK(runtime_mutation_enabled = 0)
);
CREATE INDEX IF NOT EXISTS shadow_cycles_time_idx
    ON shadow_cycles(started_ts, cycle_id);
"""


DDL_V3 = """
ALTER TABLE shadow_cycles
    ADD COLUMN IF NOT EXISTS source_revision TEXT NOT NULL DEFAULT 'unknown-source-revision';
CREATE INDEX IF NOT EXISTS shadow_cycles_revision_time_idx
    ON shadow_cycles(build_revision, source_revision, started_ts, cycle_id);
"""


DDL_V4 = """
CREATE TABLE IF NOT EXISTS notification_delivery_epochs (
    epoch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('planned', 'active', 'closed')),
    cutover_at TEXT NOT NULL,
    cutover_ts BIGINT NOT NULL,
    created_at TEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    activated_at TEXT NOT NULL,
    activated_ts BIGINT NOT NULL,
    closed_at TEXT NOT NULL,
    closed_ts BIGINT NOT NULL,
    policy_revision TEXT NOT NULL,
    writer_identity TEXT NOT NULL,
    note TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS notification_one_active_delivery_epoch_idx
    ON notification_delivery_epochs(status) WHERE status='active';

CREATE TABLE IF NOT EXISTS notification_intent_delivery (
    intent_id TEXT PRIMARY KEY REFERENCES notification_intents(intent_id),
    mode TEXT NOT NULL CHECK(mode IN ('shadow', 'production')),
    epoch_id TEXT REFERENCES notification_delivery_epochs(epoch_id),
    eligible INTEGER NOT NULL CHECK(eligible IN (0, 1)),
    eligibility_reason TEXT NOT NULL,
    assigned_at TEXT NOT NULL,
    assigned_ts BIGINT NOT NULL,
    CHECK(
        (mode='shadow' AND epoch_id IS NULL AND eligible=0)
        OR (mode='production' AND epoch_id IS NOT NULL AND eligible=1)
    )
);
CREATE INDEX IF NOT EXISTS notification_intent_delivery_eligible_idx
    ON notification_intent_delivery(eligible, epoch_id, intent_id);

CREATE TABLE IF NOT EXISTS delivery_attempt_states (
    attempt_id TEXT PRIMARY KEY REFERENCES delivery_attempts(attempt_id),
    state TEXT NOT NULL CHECK(
        state IN ('in_flight', 'succeeded', 'retryable_failed', 'permanent_failed', 'uncertain')
    ),
    lease_name TEXT NOT NULL,
    fence_token BIGINT NOT NULL CHECK(fence_token >= 0),
    next_retry_ts BIGINT NOT NULL,
    completed_ts BIGINT NOT NULL,
    detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_attempt_states_retry_idx
    ON delivery_attempt_states(state, next_retry_ts, attempt_id);

CREATE TABLE IF NOT EXISTS lease_fences (
    lease_name TEXT PRIMARY KEY REFERENCES leases(lease_name) ON DELETE CASCADE,
    fence_token BIGINT NOT NULL CHECK(fence_token > 0)
);

INSERT OR IGNORE INTO notification_intent_delivery(
    intent_id, mode, epoch_id, eligible, eligibility_reason, assigned_at, assigned_ts
)
SELECT intent_id, 'shadow', NULL, 0, 'pre_cutover_quarantine', created_at, created_ts
FROM notification_intents;

INSERT OR IGNORE INTO delivery_attempt_states(
    attempt_id, state, lease_name, fence_token, next_retry_ts, completed_ts, detail
)
SELECT
    attempt.attempt_id,
    CASE
        WHEN EXISTS(
            SELECT 1 FROM delivery_results result
            WHERE result.attempt_id=attempt.attempt_id AND result.success=1
        ) THEN 'succeeded'
        WHEN EXISTS(
            SELECT 1 FROM delivery_results result
            WHERE result.attempt_id=attempt.attempt_id
              AND result.status_code>=400 AND result.status_code<500
              AND result.status_code NOT IN (408, 425, 429)
        ) THEN 'permanent_failed'
        WHEN EXISTS(
            SELECT 1 FROM delivery_results result WHERE result.attempt_id=attempt.attempt_id
        ) THEN 'retryable_failed'
        ELSE 'uncertain'
    END,
    'legacy_notification_dispatcher',
    0,
    0,
    COALESCE((
        SELECT MAX(result.completed_ts) FROM delivery_results result
        WHERE result.attempt_id=attempt.attempt_id
    ), 0),
    'migrated_legacy_attempt'
FROM delivery_attempts AS attempt;
"""


DDL_V5 = """
ALTER TABLE current_snapshots
    ADD CONSTRAINT current_snapshots_identity_unique UNIQUE(snapshot_id, domain);

ALTER TABLE domain_current
    ADD CONSTRAINT domain_current_snapshot_reference
    FOREIGN KEY(snapshot_id, domain)
    REFERENCES current_snapshots(snapshot_id, domain);

ALTER TABLE incident_candidates
    ADD CONSTRAINT incident_candidates_snapshot_reference
    FOREIGN KEY(snapshot_id, domain)
    REFERENCES current_snapshots(snapshot_id, domain);

ALTER TABLE incident_transitions
    ADD CONSTRAINT incident_transitions_snapshot_reference
    FOREIGN KEY(current_snapshot_id, domain)
    REFERENCES current_snapshots(snapshot_id, domain);
"""


DDL_V6 = """
CREATE TABLE IF NOT EXISTS public_artifact_publications (
    publication_id TEXT PRIMARY KEY,
    artifact_key TEXT NOT NULL,
    cycle_id TEXT NOT NULL REFERENCES shadow_cycles(cycle_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK(state IN ('pending', 'published', 'superseded')),
    created_at TEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    attempt_count INTEGER NOT NULL CHECK(attempt_count >= 0),
    last_attempt_at TEXT NOT NULL,
    last_attempt_ts BIGINT NOT NULL,
    published_at TEXT NOT NULL,
    published_ts BIGINT NOT NULL,
    last_error TEXT NOT NULL,
    UNIQUE(artifact_key, cycle_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS public_artifact_one_pending_idx
    ON public_artifact_publications(artifact_key) WHERE state='pending';
CREATE INDEX IF NOT EXISTS public_artifact_publications_cycle_idx
    ON public_artifact_publications(cycle_id, publication_id);
"""


def _statements(script: str) -> list[str]:
    return [statement.strip() for statement in script.split(";") if statement.strip()]


def migrate(connection: Any, *, applied_at: str) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    row = connection.execute(
        "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
    ).fetchone()
    current = int(row["version"])
    if current > SCHEMA_VERSION:
        raise RuntimeError(f"database schema {current} is newer than supported {SCHEMA_VERSION}")
    for version, script in (
        (1, DDL_V1),
        (2, DDL_V2),
        (3, DDL_V3),
        (4, DDL_V4),
        (5, DDL_V5),
        (6, DDL_V6),
    ):
        if current >= version:
            continue
        for statement in _statements(script):
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?) "
            "ON CONFLICT(version) DO NOTHING",
            (version, applied_at),
        )
        current = version
