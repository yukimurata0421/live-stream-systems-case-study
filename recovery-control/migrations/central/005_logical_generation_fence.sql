PRAGMA foreign_keys = ON;

-- The exact effect_scope_id retains PID for evidence compatibility.  Physical
-- admission is fenced by the declared FFmpeg generation without PID.
ALTER TABLE effect_scope_ledger
    ADD COLUMN logical_generation_scope_id TEXT;

CREATE UNIQUE INDEX one_central_effect_per_logical_generation
    ON effect_scope_ledger(logical_generation_scope_id)
    WHERE logical_generation_scope_id IS NOT NULL;
