PRAGMA foreign_keys = ON;

-- CRA process/transport liveness renews the authority lease independently of
-- whether current Monitoring facts are sufficient to authorize an action.
ALTER TABLE authority_fences
    ADD COLUMN action_ready INTEGER NOT NULL DEFAULT 0
    CHECK (action_ready IN (0, 1));
