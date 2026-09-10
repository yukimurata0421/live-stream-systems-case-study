PRAGMA foreign_keys = ON;

-- Keep the legacy reason_code column as a compatibility projection of the
-- decision reason.  New rows bind both independent reason dimensions into the
-- decision digest; legacy rows remain explicitly identifiable.
ALTER TABLE cra_policy_decisions
    ADD COLUMN candidate_reason_code TEXT NOT NULL DEFAULT 'LEGACY_UNSEPARATED';

ALTER TABLE cra_policy_decisions
    ADD COLUMN decision_reason_code TEXT NOT NULL DEFAULT 'LEGACY_UNSEPARATED';

UPDATE cra_policy_decisions
SET decision_reason_code = reason_code;
