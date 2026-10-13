PRAGMA foreign_keys = ON;

-- A target-local revision advances once for every newly committed signed
-- Monitoring projection.  Producer sequence remains source-local; this
-- revision is the Central SQLite commit order across source replacement.
CREATE TABLE monitoring_projection_heads (
    target_id TEXT PRIMARY KEY REFERENCES targets(target_id),
    projection_id TEXT NOT NULL UNIQUE REFERENCES monitoring_evidence_projections(projection_id),
    source_instance_id TEXT NOT NULL,
    observation_revision TEXT NOT NULL,
    observation_sequence INTEGER NOT NULL CHECK (observation_sequence > 0),
    projection_sha256 TEXT NOT NULL CHECK (
        length(projection_sha256) = 64
        AND projection_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    state_revision INTEGER NOT NULL CHECK (state_revision > 0),
    updated_at TEXT NOT NULL
) STRICT;

-- Existing ledgers start from the retained durable projection count.  This is
-- a migration floor, not a claim about rows already compacted before upgrade.
INSERT INTO monitoring_projection_heads(
    target_id,projection_id,source_instance_id,observation_revision,
    observation_sequence,projection_sha256,state_revision,updated_at
)
SELECT p.target_id,p.projection_id,p.source_instance_id,p.observation_revision,
       p.observation_sequence,p.payload_sha256,
       (SELECT count(*) FROM monitoring_evidence_projections AS counted
        WHERE counted.target_id=p.target_id),
       p.received_at
FROM monitoring_evidence_projections AS p
WHERE p.rowid=(
    SELECT max(latest.rowid) FROM monitoring_evidence_projections AS latest
    WHERE latest.target_id=p.target_id
);

-- Bind every executable authorization to the exact signed Monitoring
-- projection and Central target revision used by the decision.  The binding
-- is append-only: a newer projection creates a new decision/authorization
-- instead of rewriting the basis of an existing one.
CREATE TABLE authorization_revision_bindings (
    authorization_id TEXT PRIMARY KEY REFERENCES recovery_authorizations(authorization_id),
    projection_id TEXT NOT NULL REFERENCES monitoring_evidence_projections(projection_id),
    target_id TEXT NOT NULL REFERENCES targets(target_id),
    source_instance_id TEXT NOT NULL,
    observation_revision TEXT NOT NULL,
    observation_sequence INTEGER NOT NULL CHECK (observation_sequence > 0),
    state_revision INTEGER NOT NULL CHECK (state_revision > 0),
    projection_sha256 TEXT NOT NULL CHECK (
        length(projection_sha256) = 64
        AND projection_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    bound_at TEXT NOT NULL
) STRICT;

CREATE INDEX authorization_revision_source_head
    ON authorization_revision_bindings(target_id, state_revision);

CREATE TRIGGER authorization_revision_bindings_no_update
BEFORE UPDATE ON authorization_revision_bindings
BEGIN
    SELECT RAISE(ABORT, 'AUTHORIZATION_REVISION_BINDING_APPEND_ONLY');
END;

CREATE TRIGGER authorization_revision_bindings_no_delete
BEFORE DELETE ON authorization_revision_bindings
BEGIN
    SELECT RAISE(ABORT, 'AUTHORIZATION_REVISION_BINDING_APPEND_ONLY');
END;
