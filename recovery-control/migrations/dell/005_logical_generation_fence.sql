PRAGMA foreign_keys = ON;

-- Keep the historical exact-snapshot scope id, while making the actual
-- physical admission fence independent of PID for one declared generation.
ALTER TABLE effect_scope_fences
    ADD COLUMN logical_generation_scope_id TEXT;

CREATE UNIQUE INDEX one_effect_fence_per_logical_generation
    ON effect_scope_fences(logical_generation_scope_id)
    WHERE logical_generation_scope_id IS NOT NULL;
