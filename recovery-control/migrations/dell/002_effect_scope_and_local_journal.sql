PRAGMA foreign_keys = ON;

CREATE TABLE effect_scope_fences (
    effect_scope_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    exact_target_json TEXT NOT NULL CHECK (json_valid(exact_target_json)),
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('CENTRAL_COMMAND', 'LOCAL_ACTION')),
    owner_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN (
        'ACCEPTED', 'EXECUTION_STARTED', 'EFFECT_BOUNDARY_REACHED',
        'EFFECT_OBSERVED', 'EFFECT_FAILED', 'OUTCOME_UNKNOWN', 'RELEASED_NO_EFFECT'
    )),
    effect_boundary_reached INTEGER NOT NULL DEFAULT 0 CHECK (effect_boundary_reached IN (0, 1)),
    physical_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (physical_attempt_count BETWEEN 0 AND 1),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;
CREATE INDEX effect_scope_fences_target_state_idx
    ON effect_scope_fences(target_id, state);

CREATE TABLE effect_scope_events (
    event_id TEXT PRIMARY KEY,
    effect_scope_id TEXT NOT NULL REFERENCES effect_scope_fences(effect_scope_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    occurred_at TEXT NOT NULL
) STRICT;
CREATE INDEX effect_scope_events_scope_idx
    ON effect_scope_events(effect_scope_id, occurred_at);

CREATE TABLE local_action_journal (
    journal_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    local_action_id TEXT NOT NULL REFERENCES local_actions(local_action_id),
    local_session_id TEXT NOT NULL REFERENCES local_fallback_sessions(local_session_id),
    target_id TEXT NOT NULL REFERENCES authority_fences(target_id),
    effect_scope_id TEXT NOT NULL REFERENCES effect_scope_fences(effect_scope_id),
    record_state TEXT NOT NULL CHECK (record_state IN (
        'EXECUTION_STARTED', 'EFFECT_BOUNDARY_REACHED', 'EFFECT_OBSERVED',
        'EFFECT_FAILED', 'OUTCOME_UNKNOWN'
    )),
    previous_digest TEXT NOT NULL,
    record_digest TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    recorded_at TEXT NOT NULL,
    UNIQUE (local_action_id, record_state)
) STRICT;

CREATE TABLE local_action_journal_acks (
    target_id TEXT PRIMARY KEY REFERENCES authority_fences(target_id),
    reconciliation_id TEXT NOT NULL,
    ack_sequence INTEGER NOT NULL CHECK (ack_sequence >= 0),
    ack_record_digest TEXT NOT NULL,
    acknowledged_at TEXT NOT NULL
) STRICT;
