# ADS-B Source Evidence Coverage

ADS-B source availability and observation coverage answer different questions.
Availability is the share of classified monitor events whose source is active
or quiet. Coverage is the share of the requested time window for which a
classified monitor event remains current. Neither measures reception of every
individual ADS-B message or proves that the map reached a viewer.

## Why Count Time

A monitor that waits 60 seconds after completing a cycle does not write exactly
one event per minute. For example, a steady 66-second cycle produces fewer
events than a 60-second denominator expects, even without an observation gap.
Counting those events as missing minutes understates evidence coverage.

`rendering_event_measurements` in
[`sli_report.py`](../../src/stream_core/cli_support/sli_report.py) uses this
contract:

- The coverage window is exactly `[start_ts, end_ts)`.
- Each event remains current until the next event of any kind, or 90 seconds
  after its timestamp, whichever comes first. UNKNOWN events end the preceding
  event's interval too.
- `event_coverage_pct` counts time with any current event. `coverage_pct` counts
  only time with a current classified event. A stale source is classified bad
  evidence, so it lowers availability rather than coverage.
- `uncovered_seconds` is time without a current event; `unclassified_seconds`
  is time with a current UNKNOWN event. The two do not overlap.
- An event before the start can cover the opening seconds until it expires or
  a later event replaces it. The loader therefore reads 90 seconds before the
  earliest window, without adding those earlier events to in-window counts.

For a 60-second window, a classified event at second 0 followed by UNKNOWN at
second 30 yields 100% event coverage, 50% classified coverage, and 30
unclassified seconds. A later event cannot fill a gap beyond the preceding
event's 90-second lifetime. The lifetime is a measurement rule, not a promise
that the source was continuously observed between events.

## Source State And Compatibility

The reader also includes the deployed source classifier needed by this
contract. Fresh explicit source heartbeat with no traffic is `quiet`, and
traffic movement is `active`; both are healthy source observations. An
explicit invalid source or failed heartbeat is `stale`. Report failures remain
separate diagnostic event counts. Historical static rows without heartbeat
evidence remain UNKNOWN; the reader does not infer quiet from zero traffic or
manufacture a missing heartbeat field in older producers.

The report provides `adsb_source_events` and retains the equal
`adsb_rendering_events` alias. Its availability ratio and event counts still
use events, including the existing inclusive count boundary at `end_ts`.
`expected_points` and `missing_points` retain their 60-second count basis and
are not the denominator or gap duration for time coverage. An event exactly
at `end_ts` adds no coverage time.

The new fields are `coverage_basis="event_time_span"`, `max_event_span_sec`,
`window_seconds`, `uncovered_seconds`, and `unclassified_seconds`. The public
collector displays the two duration fields in minutes and the lifetime in
seconds, while retaining event units for diagnostic counts. Legacy reports
without the time basis keep their count-based display.

## Public Source Port And Validation

The 2026-10-23 port adapts the reviewed measurement implementation to this
public snapshot. It includes the source-classification prerequisite and
preserves the older run-statistics behavior for other SLI callers. It does
not change other SLI calculations, targets, windows, recovery gates, OCR
thresholds, or the deployment configuration. Local port verification is
recorded in [the publication record](../../publication/adsb-coverage-port-20261023.json).

Run the offline regressions from the repository root:

```bash
python3 -m pytest -q tests/test_adsb_event_coverage.py tests/test_sli_report.py tests/test_public_reliability_collector.py tests/test_sli_pipeline_rotation.py
```

The tests use synthetic events, temporary files and substituted metric queries.
They do not contact a provider or operate a cluster. Private deployment scripts,
host paths, raw logs and runtime state are excluded. This source port performs
no deployment and makes no new long-term availability or reception claim.
