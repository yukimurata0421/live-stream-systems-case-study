PRAGMA foreign_keys = ON;

CREATE TABLE coordinator_identity (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    database_lineage_id TEXT NOT NULL,
    coordinator_instance_id TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE coordinator_state (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    maintenance_state TEXT NOT NULL CHECK (maintenance_state IN (
        'STARTUP_RECONCILING', 'INACTIVE', 'REQUESTED', 'QUIESCING',
        'ESTABLISHED', 'MUTATING', 'VERIFYING_TARGET', 'RECONCILING',
        'EXIT_PENDING', 'COMPLETED', 'QUIESCE_FAILED', 'ABORTING',
        'ABORTED', 'SAFE_BLOCKED', 'UNKNOWN')),
    maintenance_id TEXT,
    maintenance_generation INTEGER NOT NULL DEFAULT 0 CHECK (maintenance_generation >= 0),
    startup_reconciled INTEGER NOT NULL DEFAULT 0 CHECK (startup_reconciled IN (0, 1)),
    transaction_uncertain INTEGER NOT NULL DEFAULT 0 CHECK (transaction_uncertain IN (0, 1)),
    reconciliation_state TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
) STRICT;

CREATE TABLE maintenance_transactions (
    maintenance_id TEXT PRIMARY KEY,
    maintenance_generation INTEGER NOT NULL UNIQUE CHECK (maintenance_generation > 0),
    state TEXT NOT NULL,
    target_identity_json TEXT CHECK (target_identity_json IS NULL OR json_valid(target_identity_json)),
    resolved INTEGER NOT NULL DEFAULT 0 CHECK (resolved IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE maintenance_fences (
    fence_id TEXT PRIMARY KEY,
    maintenance_id TEXT NOT NULL REFERENCES maintenance_transactions(maintenance_id),
    maintenance_generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PREPARED', 'ACTIVE', 'RELEASE_PREPARED', 'RELEASED', 'ABORTED')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE maintenance_authorizations (
    authorization_id TEXT PRIMARY KEY,
    maintenance_id TEXT NOT NULL REFERENCES maintenance_transactions(maintenance_id),
    maintenance_generation INTEGER NOT NULL,
    projection_json TEXT NOT NULL CHECK (json_valid(projection_json)),
    state TEXT NOT NULL,
    use_count INTEGER NOT NULL DEFAULT 0 CHECK (use_count >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE coordinator_transitions (
    transition_id TEXT PRIMARY KEY,
    maintenance_id TEXT,
    maintenance_generation INTEGER NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    physical_effect_count INTEGER NOT NULL DEFAULT 0 CHECK (physical_effect_count = 0)
) STRICT;

CREATE INDEX maintenance_transactions_unresolved
ON maintenance_transactions(resolved, maintenance_generation);

CREATE INDEX maintenance_fences_active
ON maintenance_fences(state, maintenance_generation);
