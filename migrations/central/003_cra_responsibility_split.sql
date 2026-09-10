PRAGMA foreign_keys = ON;

CREATE TABLE monitoring_evidence_projections (
    projection_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    source_instance_id TEXT NOT NULL,
    source_release_id TEXT NOT NULL,
    monitoring_cycle_id TEXT NOT NULL,
    observation_revision TEXT NOT NULL,
    observation_sequence INTEGER NOT NULL CHECK (observation_sequence > 0),
    incident_id TEXT NOT NULL,
    incident_state TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    payload_sha256 TEXT NOT NULL UNIQUE,
    key_id TEXT NOT NULL,
    signature TEXT NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE (source_instance_id, observation_sequence),
    UNIQUE (source_instance_id, monitoring_cycle_id, observation_revision)
) STRICT;

CREATE TABLE cra_policy_decisions (
    decision_id TEXT PRIMARY KEY,
    projection_id TEXT NOT NULL REFERENCES monitoring_evidence_projections(projection_id),
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    policy_revision TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('AUTHORIZED', 'BLOCKED', 'NO_ACTION')),
    action TEXT CHECK (action IS NULL OR action = 'restart_ffmpeg'),
    reason_code TEXT NOT NULL,
    blockers_json TEXT NOT NULL CHECK (json_valid(blockers_json)),
    expected_target_json TEXT NOT NULL CHECK (json_valid(expected_target_json)),
    decided_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    decision_digest TEXT NOT NULL UNIQUE
) STRICT;
CREATE UNIQUE INDEX one_policy_decision_per_projection
    ON cra_policy_decisions(projection_id, policy_revision);

CREATE TABLE dell_journal_records (
    agent_installation_id TEXT NOT NULL,
    journal_sequence INTEGER NOT NULL CHECK (journal_sequence > 0),
    local_action_id TEXT NOT NULL,
    local_session_id TEXT NOT NULL,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    effect_scope_id TEXT NOT NULL,
    record_state TEXT NOT NULL CHECK (record_state IN (
        'EXECUTION_STARTED', 'EFFECT_BOUNDARY_REACHED', 'EFFECT_OBSERVED',
        'EFFECT_FAILED', 'OUTCOME_UNKNOWN'
    )),
    previous_digest TEXT NOT NULL,
    record_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    recorded_at TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY (agent_installation_id, journal_sequence),
    UNIQUE (agent_installation_id, record_digest)
) STRICT;
CREATE INDEX dell_journal_records_action_idx
    ON dell_journal_records(agent_installation_id, local_action_id, journal_sequence);

CREATE TABLE dell_journal_watermarks (
    agent_installation_id TEXT NOT NULL,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    highest_sequence INTEGER NOT NULL CHECK (highest_sequence >= 0),
    highest_record_digest TEXT NOT NULL,
    reconciliation_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (agent_installation_id, target_id)
) STRICT;

CREATE TABLE cra_verifier_decisions (
    verifier_decision_id TEXT PRIMARY KEY,
    command_id TEXT REFERENCES commands(command_id),
    local_action_id TEXT,
    effect_scope_id TEXT NOT NULL,
    pre_projection_id TEXT NOT NULL REFERENCES monitoring_evidence_projections(projection_id),
    post_projection_id TEXT NOT NULL REFERENCES monitoring_evidence_projections(projection_id),
    verdict TEXT NOT NULL CHECK (verdict IN ('RECOVERED', 'FAILED', 'UNKNOWN')),
    reason_codes_json TEXT NOT NULL CHECK (json_valid(reason_codes_json)),
    execution_evidence_json TEXT NOT NULL CHECK (json_valid(execution_evidence_json)),
    decided_at TEXT NOT NULL,
    decision_digest TEXT NOT NULL UNIQUE,
    CHECK ((command_id IS NOT NULL) != (local_action_id IS NOT NULL))
) STRICT;
