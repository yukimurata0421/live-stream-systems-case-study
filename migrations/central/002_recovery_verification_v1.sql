PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS recovery_verification_messages (
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
