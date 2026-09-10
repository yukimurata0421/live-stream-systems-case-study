PRAGMA foreign_keys = ON;

CREATE TABLE agent_target_binding (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    target_id TEXT NOT NULL UNIQUE,
    bound_at TEXT NOT NULL
) STRICT;

INSERT INTO agent_target_binding(singleton_id,target_id,bound_at)
SELECT 1,target_id,strftime('%Y-%m-%dT%H:%M:%fZ','now')
FROM authority_fences
ORDER BY target_id
LIMIT 1;

CREATE TRIGGER authority_fence_single_target_guard
BEFORE INSERT ON authority_fences
WHEN EXISTS (
    SELECT 1 FROM agent_target_binding AS binding
    WHERE binding.singleton_id=1 AND binding.target_id != NEW.target_id
)
BEGIN
    SELECT RAISE(ABORT, 'DELL_AGENT_SINGLE_TARGET_VIOLATION');
END;

CREATE TRIGGER authority_fence_initial_target_binding
AFTER INSERT ON authority_fences
WHEN NOT EXISTS (SELECT 1 FROM agent_target_binding WHERE singleton_id=1)
BEGIN
    INSERT INTO agent_target_binding(singleton_id,target_id,bound_at)
    VALUES(1,NEW.target_id,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
END;

CREATE TRIGGER agent_target_binding_immutable_update
BEFORE UPDATE OF target_id ON agent_target_binding
BEGIN
    SELECT RAISE(ABORT, 'DELL_AGENT_TARGET_BINDING_IMMUTABLE');
END;

CREATE TRIGGER agent_target_binding_immutable_delete
BEFORE DELETE ON agent_target_binding
BEGIN
    SELECT RAISE(ABORT, 'DELL_AGENT_TARGET_BINDING_IMMUTABLE');
END;
