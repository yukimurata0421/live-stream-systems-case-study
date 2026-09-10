PRAGMA foreign_keys = ON;

CREATE TABLE agent_identity (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    agent_id TEXT NOT NULL,
    agent_installation_id TEXT NOT NULL UNIQUE,
    host_id TEXT NOT NULL,
    database_lineage_id TEXT NOT NULL,
    last_host_boot_id TEXT NOT NULL,
    ledger_state TEXT NOT NULL CHECK (ledger_state IN ('HEALTHY', 'DEGRADED', 'LEDGER_LOST', 'SAFE_BLOCKED')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
) STRICT;

CREATE TABLE authority_fences (
    target_id TEXT PRIMARY KEY,
    authority_state TEXT NOT NULL CHECK (authority_state IN ('AGENT_STARTUP_RECONCILING',
        'CENTRAL_ACTIVE', 'CENTRAL_SUSPECT', 'LOCAL_FALLBACK', 'RECONCILING',
        'SAFE_BLOCKED', 'MAINTENANCE')),
    highest_authority_epoch_seen INTEGER NOT NULL DEFAULT 0 CHECK (highest_authority_epoch_seen >= 0),
    active_authority_session_id TEXT,
    active_controller_instance_id TEXT,
    highest_command_seq_consumed INTEGER NOT NULL DEFAULT 0 CHECK (highest_command_seq_consumed >= 0),
    heartbeat_seq INTEGER NOT NULL DEFAULT 0 CHECK (heartbeat_seq >= 0),
    lease_duration_ms INTEGER CHECK (lease_duration_ms > 0),
    last_heartbeat_received_at TEXT,
    state_reason TEXT NOT NULL,
    last_reconciled_at TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
) STRICT;

CREATE TABLE agent_safety_policies (
    policy_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL UNIQUE REFERENCES authority_fences(target_id),
    policy_revision TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    minimum_action_interval_sec INTEGER NOT NULL CHECK (minimum_action_interval_sec > 0),
    action_cost_sec INTEGER NOT NULL CHECK (action_cost_sec > 0),
    hourly_action_cost_limit_sec INTEGER NOT NULL CHECK (hourly_action_cost_limit_sec > 0),
    daily_action_cost_limit_sec INTEGER NOT NULL CHECK (daily_action_cost_limit_sec > 0),
    local_fallback_enabled INTEGER NOT NULL CHECK (local_fallback_enabled IN (0, 1)),
    canonical_payload TEXT NOT NULL CHECK (json_valid(canonical_payload)),
    operator_key_id TEXT NOT NULL,
    operator_signature TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    installed_at TEXT NOT NULL
) STRICT;

CREATE TABLE reconciliation_sessions (
    reconciliation_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    challenge_id TEXT NOT NULL UNIQUE,
    challenge_nonce_hash TEXT NOT NULL UNIQUE,
    proposed_authority_epoch INTEGER CHECK (proposed_authority_epoch > 0),
    proposed_authority_session_id TEXT,
    proposed_controller_instance_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('CHALLENGE_ISSUED', 'COMMITTED', 'EXPIRED', 'REJECTED')),
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    completed_at TEXT
) STRICT;

CREATE TABLE agent_commands (
    command_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    authority_session_id TEXT NOT NULL,
    authority_epoch INTEGER NOT NULL CHECK (authority_epoch > 0),
    command_seq INTEGER NOT NULL CHECK (command_seq > 0),
    incident_id TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    reason_code TEXT NOT NULL CHECK (reason_code = 'confirmed_tcp_stall'),
    canonical_payload TEXT NOT NULL CHECK (json_valid(canonical_payload)),
    payload_sha256 TEXT NOT NULL,
    sender_key_id TEXT NOT NULL,
    signature TEXT NOT NULL,
    expected_target_json TEXT NOT NULL CHECK (json_valid(expected_target_json)),
    state TEXT NOT NULL CHECK (state IN ('RECEIVED', 'ACCEPTED', 'REJECTED',
        'EXECUTION_STARTED', 'EFFECT_OBSERVED', 'EFFECT_FAILED', 'OUTCOME_UNKNOWN',
        'SUPERSEDED_AFTER_AGENT_RESTART')),
    terminal_reason_code TEXT,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (target_id, authority_epoch, command_seq),
    UNIQUE (authorization_id)
) STRICT;
CREATE UNIQUE INDEX one_agent_unresolved_command_per_target ON agent_commands(target_id)
WHERE state IN ('ACCEPTED', 'EXECUTION_STARTED', 'OUTCOME_UNKNOWN');

CREATE TABLE execution_attempts (
    execution_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE REFERENCES agent_commands(command_id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no = 1),
    state TEXT NOT NULL CHECK (state IN ('RESERVED', 'STARTED', 'EFFECT_OBSERVED', 'FAILED', 'OUTCOME_UNKNOWN')),
    before_target_json TEXT NOT NULL CHECK (json_valid(before_target_json)),
    pre_effect_target_json TEXT CHECK (pre_effect_target_json IS NULL OR json_valid(pre_effect_target_json)),
    after_target_json TEXT CHECK (after_target_json IS NULL OR json_valid(after_target_json)),
    signal_name TEXT NOT NULL CHECK (signal_name = 'SIGTERM'),
    signal_return_code INTEGER,
    reserved_at TEXT NOT NULL,
    started_at TEXT,
    effect_observed_at TEXT,
    finished_at TEXT,
    outcome_reason_code TEXT
) STRICT;

CREATE TABLE local_fallback_sessions (
    local_session_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    started_host_boot_id TEXT NOT NULL,
    based_on_last_central_epoch INTEGER NOT NULL CHECK (based_on_last_central_epoch >= 0),
    policy_revision TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'RECONCILING', 'CLOSED', 'SAFE_BLOCKED')),
    started_at TEXT NOT NULL,
    ended_at TEXT
) STRICT;
CREATE UNIQUE INDEX one_active_local_fallback_session ON local_fallback_sessions(target_id)
WHERE state IN ('ACTIVE', 'RECONCILING');

CREATE TABLE local_actions (
    local_action_id TEXT PRIMARY KEY,
    local_session_id TEXT NOT NULL REFERENCES local_fallback_sessions(local_session_id),
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    local_action_seq INTEGER NOT NULL CHECK (local_action_seq > 0),
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    reason_code TEXT NOT NULL CHECK (reason_code = 'confirmed_tcp_stall'),
    target_identity_json TEXT NOT NULL CHECK (json_valid(target_identity_json)),
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    state TEXT NOT NULL CHECK (state IN ('ACCEPTED', 'EXECUTION_STARTED', 'EFFECT_OBSERVED', 'EFFECT_FAILED', 'OUTCOME_UNKNOWN')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (local_session_id, local_action_seq)
) STRICT;

CREATE TABLE agent_transitions (
    transition_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    command_id TEXT REFERENCES agent_commands(command_id),
    local_action_id TEXT REFERENCES local_actions(local_action_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    target_identity_json TEXT CHECK (target_identity_json IS NULL OR json_valid(target_identity_json)),
    occurred_at TEXT NOT NULL,
    CHECK (NOT (command_id IS NOT NULL AND local_action_id IS NOT NULL))
) STRICT;

CREATE TABLE agent_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARN', 'ERROR', 'CRITICAL')),
    reason_code TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    occurred_at TEXT NOT NULL
) STRICT;
