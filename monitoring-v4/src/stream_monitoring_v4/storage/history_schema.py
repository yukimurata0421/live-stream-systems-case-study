"""Schema v7: small, backend-neutral normal-monitoring read model."""

DDL_V7 = """
ALTER TABLE shadow_cycles ADD COLUMN purpose TEXT NOT NULL DEFAULT 'migration';
ALTER TABLE shadow_cycles ADD COLUMN campaign_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS shadow_cycles_campaign_idx
    ON shadow_cycles(purpose, campaign_id, build_revision, source_revision, started_ts);
CREATE INDEX IF NOT EXISTS observations_received_idx ON observations(received_ts, observation_id);
CREATE INDEX IF NOT EXISTS rejections_received_idx ON rejections(received_ts, rejection_id);
CREATE INDEX IF NOT EXISTS shadow_cycles_completed_idx ON shadow_cycles(completed_ts, cycle_id);
CREATE INDEX IF NOT EXISTS current_snapshots_reduced_idx ON current_snapshots(reduced_ts, snapshot_id);
CREATE INDEX IF NOT EXISTS sli_projections_evaluated_idx ON sli_projections(evaluated_ts, projection_id);
CREATE TABLE IF NOT EXISTS metric_series (
    series_id TEXT PRIMARY KEY,
    metric TEXT NOT NULL,
    target TEXT NOT NULL,
    definition_revision TEXT NOT NULL,
    started_ts BIGINT NOT NULL,
    UNIQUE(metric, target, definition_revision)
);
CREATE TABLE IF NOT EXISTS metric_minutes (
    series_id TEXT NOT NULL REFERENCES metric_series(series_id),
    minute_ts BIGINT NOT NULL,
    sample_ts BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('good', 'bad', 'unknown', 'not_applicable')),
    PRIMARY KEY(series_id, minute_ts)
);
CREATE INDEX IF NOT EXISTS metric_minutes_time_idx ON metric_minutes(minute_ts, series_id);
CREATE TABLE IF NOT EXISTS metric_windows (
    series_id TEXT NOT NULL REFERENCES metric_series(series_id),
    window_sec BIGINT NOT NULL CHECK(window_sec > 0),
    end_ts BIGINT NOT NULL,
    good BIGINT NOT NULL CHECK(good >= 0),
    bad BIGINT NOT NULL CHECK(bad >= 0),
    unknown BIGINT NOT NULL CHECK(unknown >= 0),
    not_applicable BIGINT NOT NULL CHECK(not_applicable >= 0),
    PRIMARY KEY(series_id, window_sec)
);
CREATE TABLE IF NOT EXISTS normal_cycle_current (
    target TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    completed_ts BIGINT NOT NULL,
    payload_json TEXT NOT NULL
);
"""
