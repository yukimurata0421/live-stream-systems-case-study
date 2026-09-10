PRAGMA foreign_keys = ON;

CREATE TABLE control_plane_identity (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    database_lineage_id TEXT NOT NULL,
    database_instance_id TEXT NOT NULL,
    controller_instance_id TEXT NOT NULL,
    restore_state TEXT NOT NULL CHECK (restore_state IN ('CLEAN', 'RESTORED_NEEDS_RECONCILIATION')),
    restored_from_backup_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
) STRICT;

CREATE TABLE targets (
    target_id TEXT PRIMARY KEY,
    host_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    authority_state TEXT NOT NULL CHECK (authority_state IN (
        'AGENT_STARTUP_RECONCILING', 'CENTRAL_ACTIVE', 'CENTRAL_SUSPECT',
        'LOCAL_FALLBACK', 'RECONCILING', 'RESTORED_NEEDS_RECONCILIATION',
        'SAFE_BLOCKED', 'MAINTENANCE')),
    current_authority_epoch INTEGER NOT NULL DEFAULT 0 CHECK (current_authority_epoch >= 0),
    next_command_seq INTEGER NOT NULL DEFAULT 1 CHECK (next_command_seq >= 1),
    last_agent_installation_id TEXT,
    last_dell_high_epoch INTEGER NOT NULL DEFAULT 0 CHECK (last_dell_high_epoch >= 0),
    last_dell_high_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_dell_high_seq >= 0),
    state_reason TEXT NOT NULL,
    last_reconciled_at TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
) STRICT;

CREATE TABLE incidents (
    incident_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    source_episode_id TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('OPEN', 'CONFIRMED', 'AUTHORIZED',
        'RECOVERY_REQUESTED', 'VERIFYING', 'RECOVERED', 'CLOSED', 'ESCALATED')),
    observation_revision TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    confirmed_at TEXT,
    closed_at TEXT,
    last_transition_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
) STRICT;
CREATE UNIQUE INDEX one_open_recovery_incident_per_target ON incidents(target_id)
WHERE state NOT IN ('RECOVERED', 'CLOSED', 'ESCALATED');

CREATE TABLE incident_transitions (
    transition_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    evidence_ref TEXT,
    occurred_at TEXT NOT NULL
) STRICT;

CREATE TABLE recovery_authorizations (
    authorization_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    policy_revision TEXT NOT NULL,
    observation_revision TEXT NOT NULL,
    expected_target_json TEXT NOT NULL CHECK (json_valid(expected_target_json)),
    blockers_json TEXT NOT NULL CHECK (json_valid(blockers_json)),
    authorized_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('AUTHORIZED', 'CONSUMED', 'EXPIRED', 'REVOKED'))
) STRICT;

CREATE TABLE authority_sessions (
    authority_session_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    authority_epoch INTEGER NOT NULL CHECK (authority_epoch > 0),
    controller_instance_id TEXT NOT NULL,
    agent_installation_id TEXT NOT NULL,
    reconciliation_id TEXT NOT NULL UNIQUE,
    challenge_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('PROPOSED', 'ACTIVE', 'EXPIRED', 'REPLACED', 'BLOCKED')),
    started_at TEXT NOT NULL,
    last_heartbeat_sent_at TEXT,
    last_heartbeat_acked_at TEXT,
    ended_at TEXT,
    UNIQUE (target_id, authority_epoch)
) STRICT;

CREATE TABLE commands (
    command_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
    authorization_id TEXT NOT NULL UNIQUE REFERENCES recovery_authorizations(authorization_id),
    authority_session_id TEXT NOT NULL REFERENCES authority_sessions(authority_session_id),
    authority_epoch INTEGER NOT NULL CHECK (authority_epoch > 0),
    command_seq INTEGER NOT NULL CHECK (command_seq > 0),
    idempotency_key TEXT NOT NULL UNIQUE,
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    reason_code TEXT NOT NULL CHECK (reason_code = 'confirmed_tcp_stall'),
    expected_host_boot_id TEXT NOT NULL,
    expected_pod_uid TEXT NOT NULL,
    expected_container_name TEXT NOT NULL CHECK (expected_container_name = 'stream-engine'),
    expected_container_id TEXT NOT NULL,
    expected_ffmpeg_generation TEXT NOT NULL,
    expected_ffmpeg_pid INTEGER NOT NULL CHECK (expected_ffmpeg_pid > 1),
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'COMMITTED', 'OUTBOX_PENDING', 'SENT',
        'ACCEPTED', 'EXECUTION_STARTED', 'EFFECT_OBSERVED', 'EFFECT_FAILED', 'OUTCOME_UNKNOWN',
        'VERIFYING', 'VERIFIED', 'VERIFICATION_FAILED', 'VERIFICATION_UNKNOWN', 'REJECTED',
        'EXPIRED', 'SUPERSEDED')),
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (target_id, authority_epoch, command_seq)
) STRICT;
CREATE UNIQUE INDEX one_unresolved_command_per_target ON commands(target_id)
WHERE status IN ('COMMITTED', 'OUTBOX_PENDING', 'SENT', 'ACCEPTED', 'EXECUTION_STARTED',
    'EFFECT_OBSERVED', 'EFFECT_FAILED', 'OUTCOME_UNKNOWN', 'VERIFYING', 'VERIFICATION_UNKNOWN');

CREATE TABLE command_transitions (
    transition_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(command_id),
    source TEXT NOT NULL CHECK (source IN ('CRA', 'DELL_AGENT', 'MONITORING_V4')),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    source_message_id TEXT,
    occurred_at TEXT NOT NULL
) STRICT;

CREATE TABLE outbox_messages (
    outbox_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE REFERENCES commands(command_id),
    message_type TEXT NOT NULL CHECK (message_type = 'execute_command'),
    canonical_payload TEXT NOT NULL CHECK (json_valid(canonical_payload)),
    payload_sha256 TEXT NOT NULL UNIQUE,
    key_id TEXT NOT NULL,
    signature TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'IN_FLIGHT', 'ACKED', 'RETRY', 'TERMINAL')),
    not_before TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE delivery_attempts (
    attempt_id TEXT PRIMARY KEY,
    outbox_id TEXT NOT NULL REFERENCES outbox_messages(outbox_id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    transport_outcome TEXT NOT NULL CHECK (transport_outcome IN
        ('IN_FLIGHT', 'RESPONSE', 'TIMEOUT', 'TLS_ERROR', 'IO_ERROR')),
    http_status INTEGER,
    receipt_message_id TEXT,
    receipt_digest TEXT,
    detail_code TEXT NOT NULL,
    UNIQUE (outbox_id, attempt_no)
) STRICT;

CREATE TABLE agent_messages (
    message_id TEXT PRIMARY KEY,
    command_id TEXT REFERENCES commands(command_id),
    message_type TEXT NOT NULL CHECK (message_type IN
        ('COMMAND_RECEIPT', 'COMMAND_STATUS', 'AGENT_STATE', 'RECONCILIATION')),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    payload_sha256 TEXT NOT NULL UNIQUE,
    signature_valid INTEGER NOT NULL CHECK (signature_valid IN (0, 1)),
    received_at TEXT NOT NULL
) STRICT;

CREATE TABLE recovery_verifications (
    verification_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE REFERENCES commands(command_id),
    monitoring_cycle_id TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('RECOVERED', 'FAILED', 'UNKNOWN')),
    observed_target_json TEXT NOT NULL CHECK (json_valid(observed_target_json)),
    checks_json TEXT NOT NULL CHECK (json_valid(checks_json)),
    evidence_refs_json TEXT NOT NULL CHECK (json_valid(evidence_refs_json)),
    observed_at TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE recovery_verification_messages (
    verification_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(command_id),
    incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    monitoring_cycle_id TEXT NOT NULL,
    observation_revision TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    verdict TEXT NOT NULL CHECK (verdict IN ('RECOVERED', 'FAILED', 'UNKNOWN')),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    payload_sha256 TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    evidence_fresh_until TEXT NOT NULL,
    received_at TEXT NOT NULL
) STRICT;
