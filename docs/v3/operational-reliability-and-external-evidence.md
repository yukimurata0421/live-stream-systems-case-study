# Operational Reliability And External Evidence

This document defines how durable SLI evidence, independent public checks,
notifications, and recovery authority relate. It describes the implementation
in this repository, not the current health of a live deployment.

## Evidence Classes

| Evidence | Role | Authority limit |
| --- | --- | --- |
| YouTube Data API and OAuth health | Official stream lifecycle, ingest, and input-health evidence | Subject to quota, freshness, and API semantics. |
| Runtime watchdog and TCP evidence | Local delivery process, NVENC, socket, send-progress, and same-URL evidence | A connected socket alone is not proof of advancing delivery. |
| Viewer and map probes | Supporting frame and rendering evidence | Read-only; cannot independently authorize restart or formal SLO burn. |
| Independent public uptime checks | Evidence that public resources are obtainable outside the home network | Supporting evidence; not the formal YouTube live/ingest judgment. |
| Public static snapshot | Reduced presentation of allowlisted evidence | No recovery or SLO authority. |

Formal SLI output requires sufficient coverage and source freshness. Historical
disagreement remains visible but is not automatically promoted to a current
incident.

## Public Video False-Unknown Correction

The original public YouTube check searched the channel `/live` HTML for a live
flag. That HTML varied by checker region, producing a persistent false failure
in one region and intermittent disagreement in another while Data API, OAuth,
ingest, stream health, and viewer-frame evidence remained healthy.

The corrected target checks the current Same URL video's oEmbed JSON instead:

- the target name is `youtube_public_video`, not `youtube_public_live`;
- each checker verifies an HTTP success and the current video identity;
- the importer requires fresh evidence from three checker locations;
- all locations failing is a real external failure signal;
- regional disagreement remains `unknown`, not `failed`; and
- oEmbed proves public retrieval of current video information, not formal live
  or ingest state.

The importer is implemented in
`ops/scripts/stream_v3_external_blackbox_import.py`. Its default project is
empty in the public repository and must be supplied externally.

## Unknown, Failure, And Notification Timing

The notification policy deliberately distinguishes uncertainty from failure:

- a single `unknown` sample does not create an incident;
- `unknown` becomes an incident only after two consecutive five-minute import
  samples;
- an `unknown` reminder is no more frequent than every 15 minutes;
- all required external locations failing can create an incident immediately,
  while the check cadence remains five minutes;
- non-critical sustained incidents are eligible for Slack escalation after 30
  minutes; and
- independent external evidence alone cannot create formal SLO burn or trigger
  a runtime restart.

The monitoring control loop is the single notification-state writer. The
legacy standalone notification timer is installed only as a disabled fallback
reference, preventing two writers from generating duplicate reminders from
different state roots.

YouTube input-quality incident lifecycle is based on the latest usable raw
OAuth sample. Only an explicit current warning, error, or warning/error
configuration issue opens or sustains the warning. `noData`, an unrecognized
health value, or a bad Prometheus projection without a matching raw OAuth fact
remains diagnostic evidence and does not open a current incident. Conversely,
an explicit raw OAuth warning remains actionable while the Prometheus
projection still shows good. A current good sample closes an existing incident
with one recovery notification even while the rolling one-hour fast-feedback
value remains below target.

The raw event and Prometheus input-quality series are two storage and scrape
cadences of the same watchdog producer, not independent measurement sources.
Their rolling and point-in-time disagreement remains in reliability output for
diagnosis and can keep a formal measurement unknown, but the disagreement by
itself does not create a Discord or Slack incident. Coverage and freshness
failures remain independently actionable. Unchanged explicit current warnings
repeat no more frequently than every 10 minutes, and the incident never
authorizes an automatic runtime restart by itself.

## Durable Evidence And Rollups

`src/stream_core/operational_reliability/evidence_store.py` stores bounded,
structured evidence in SQLite. Same-URL records retain hashed identities rather
than plain watch URLs. Numeric log rotations, including compressed rotations,
are read chronologically for backfill and from the tail for latest-event work.

`ops/scripts/stream_v3_operational_reliability_rollup.py` produces:

- hourly durable SLI and Same URL rollups;
- five-minute fast feedback and multi-window burn evaluation;
- explicit coverage, freshness, disagreement, and revision gates; and
- event-based visual supporting evidence without relabeling it as the formal
  sampled-capture visual SLI.

The Prometheus exporter exposes these results while keeping `unknown` distinct
from failure. Operator reports are available through `bin/stream-prod
sli-report`; `bin/stream-short` is a read-only SSH convenience wrapper.

## What This Does Not Claim

- The repository does not prove that a public cloud check is currently
  enabled or healthy.
- oEmbed does not replace Data API, OAuth, watchdog, TCP, or viewer evidence.
- A public-page failure does not prove a delivery interruption.
- Passing public tests does not deploy the release units or change an uptime
  check.

## Review Map

- Importer: `ops/scripts/stream_v3_external_blackbox_import.py`
- Rollup: `ops/scripts/stream_v3_operational_reliability_rollup.py`
- Store: `src/stream_core/operational_reliability/evidence_store.py`
- Incident policy: `src/stream_core/notifications/incidents.py`
- SLI report: `src/stream_core/cli_support/sli_report.py`
- Tests: `tests/test_external_blackbox_evidence.py`,
  `tests/test_operational_reliability.py`, `tests/test_sli_report.py`, and
  `tests/test_stream_v3_prometheus_exporter.py`
