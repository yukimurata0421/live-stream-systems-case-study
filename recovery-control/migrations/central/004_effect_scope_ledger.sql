PRAGMA foreign_keys = ON;

CREATE TABLE effect_scope_ledger (
    effect_scope_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    action TEXT NOT NULL CHECK (action = 'restart_ffmpeg'),
    exact_target_json TEXT NOT NULL CHECK (json_valid(exact_target_json)),
    origin_kind TEXT NOT NULL CHECK (origin_kind IN ('CENTRAL_COMMAND', 'IMPORTED_LOCAL_ACTION')),
    origin_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN (
        'RESERVED', 'PRECONDITION_CHECKED', 'PRE_EFFECT_ABORTED',
        'EFFECT_BOUNDARY_REACHED', 'EFFECT_OBSERVED', 'EFFECT_FAILED',
        'OUTCOME_UNKNOWN'
    )),
    effect_boundary_reached_at TEXT,
    physical_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (physical_attempt_count BETWEEN 0 AND 1),
    source_evidence_digest TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((physical_attempt_count = 0 AND effect_boundary_reached_at IS NULL)
        OR (physical_attempt_count = 1 AND effect_boundary_reached_at IS NOT NULL))
) STRICT;
CREATE INDEX effect_scope_ledger_target_boundary_idx
    ON effect_scope_ledger(target_id,effect_boundary_reached_at);

CREATE TABLE effect_reconciliations (
    reconciliation_id TEXT PRIMARY KEY,
    effect_scope_id TEXT NOT NULL REFERENCES effect_scope_ledger(effect_scope_id),
    prior_state TEXT NOT NULL,
    reconciled_state TEXT NOT NULL,
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    evidence_digest TEXT NOT NULL UNIQUE,
    recorded_at TEXT NOT NULL
) STRICT;
CREATE INDEX effect_reconciliations_scope_idx
    ON effect_reconciliations(effect_scope_id,recorded_at);

CREATE UNIQUE INDEX one_cra_final_verdict_per_effect_scope
    ON cra_verifier_decisions(effect_scope_id);
