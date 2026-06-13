# Observability Plane Self-Check

The observability plane is allowed to be wrong before the delivery plane is
wrong. This document records how `stream_v3` classifies and hardens that
boundary.

## Incident Class

The failure mode this document covers is:

```text
Prometheus target reachable
exporter /metrics or /healthz slow or unavailable
dashboard panels missing stream_v3_* series
raw delivery evidence still healthy
```

That is an observability-plane incident, not a delivery-plane incident. It can
hide or distort the dashboard, but it does not by itself prove that viewers lost
the stream, that the YouTube watch URL changed, or that the runtime Pod should
be restarted.

The operator response is:

1. verify raw delivery evidence first;
2. classify the dashboard state as current delivery incident, historical event,
   observability noise, or SLO burn;
3. repair the exporter/query/snapshot path before considering delivery-plane
   mutation.

## Why It Matters

A monitoring system can create a convincing false failure:

- a heavy `health-summary` computation can exceed the scrape timeout;
- Prometheus can see the exporter target but receive no useful
  `stream_v3_*` series;
- Grafana can render `No data`, stale red panels, or missing trend cards;
- an operator can misread a monitoring gap as a stream outage.

The safety rule is the same one used for recovery authority: ambiguous
observability evidence must not authorize destructive delivery actions.

## Public Contract

The public implementation keeps this boundary explicit:

| Layer | Responsibility | Failure response |
| --- | --- | --- |
| Delivery runtime | video/audio/browser/FFmpeg/k3s Pod evidence | recover only with fresh delivery evidence |
| Exporter | convert state and summaries into `stream_v3_*` metrics | serve last-good or snapshot evidence when live generation is slow |
| Monitoring watchdog | check exporter, metric contract, snapshots, and optional display dependencies | write a self-check state file and expose its result as metrics |
| Dashboard/public status | reduce evidence for humans | show freshness and avoid becoming recovery authority |

## Implementation

The public repository contains the portable pieces:

- `ops/scripts/stream_v3_health_snapshot.py`
  writes `health_summary_snapshot.json` and `objective_sli_snapshot.json`.
- `ops/scripts/stream_v3_prometheus_exporter.py`
  reads live CLI output first, falls back to snapshots if live collection fails,
  and serves a cached last-good payload if a later refresh fails.
- `ops/scripts/stream_v3_monitoring_watchdog.py`
  checks exporter HTTP reachability, required metric presence, snapshot
  freshness, and optional Prometheus/Grafana health endpoints.
- `ops/systemd/stream-v3-health-snapshot.timer`
  and `ops/systemd/stream-v3-monitoring-watchdog.timer`
  show a path-independent deployment shape.

The examples use `/opt/stream_v3` and
`/var/lib/stream-v3/observability-monitor` as public-safe defaults. Production
paths are expected to be supplied through
`/etc/default/stream-v3-observability-monitor`.

## Metrics To Read

The exporter exposes its own health separately from delivery health:

```text
stream_v3_exporter_up
stream_v3_exporter_snapshot_fallback
stream_v3_exporter_health_summary_snapshot_used
stream_v3_exporter_objective_sli_snapshot_used
stream_v3_exporter_last_good_payload
stream_v3_monitoring_watchdog_ok
stream_v3_monitoring_watchdog_state_age_seconds
stream_v3_monitoring_watchdog_check_ok{check="metrics_contract"}
```

If `stream_v3_exporter_up == 0` while fresh raw delivery evidence remains OK,
the correct classification is a monitoring incident until proven otherwise.

If `stream_v3_exporter_snapshot_fallback == 1`, dashboard data may still be
useful, but it is no longer a fresh live computation. Snapshot age must be read
before using the value for incident judgment.

## Quota-Day Boundary Warmup

YouTube API quota accounting resets on Pacific Time, while the operator may be
reading the dashboard from a different local timezone. Immediately after the
quota-day boundary, an open-day cost report can have too little elapsed time to
form a stable burn-rate window, or it may still be waiting for the first
in-window telemetry record.

That condition is observability warmup, not quota exhaustion and not delivery
failure. The expected classification is:

```text
open-day cost report has no stable window
delivery runtime and public/live evidence remain healthy
expensive or destructive YouTube API operations stay gated
public subsystem health does not burn delivery error budget
```

The important split is between "API evidence is not yet usable for cost
projection" and "the stream is unhealthy." A fail-closed quota guard can still
be useful for blocking risky YouTube mutations, but it should not by itself
promote the monitoring subsystem into a public degraded count when raw delivery
evidence is fresh and healthy.

For public reporting, this case should be summarized as a guardrail warmup or
monitoring-plane correction. Avoid publishing raw notification timelines,
machine-local paths, hostnames, or private runbook commands. The useful review
signal is the boundary: telemetry freshness can restrict control-plane actions
without claiming viewer-facing impact.

## Metric Inventory Cleanup

Dashboard cleanup follows the same rule: a missing state file is not a healthy
sample. The exporter should avoid `missing => 0` for optional ADS-B freshness,
audio route, SLO, cgroup, or stream-watchdog detail files. It either emits a
metric from fresher subsystem evidence or omits the series until evidence
exists.

The public-safe contract is:

```text
ADS-B source health comes from rendering subsystem evidence
audio health comes from music subsystem evidence
runtime memory comes from stream-v3-runtime Pod metrics
monitoring-host memory is diagnostic capacity evidence
open-day API usage is a single PT-day gauge
window labels reflect real aggregation windows only
recovery blocked/executable counts matter only when an action is pending
```

This keeps panels from showing false OK states such as "ADS-B age is zero" or
"audio faults are zero" when the actual issue is absent telemetry.

## Runbook

When public or private dashboards show missing data:

1. Check exporter health:

   ```bash
   curl -fsS http://127.0.0.1:9108/healthz
   curl -fsS http://127.0.0.1:9108/metrics | rg 'stream_v3_exporter_up|stream_v3_monitoring_watchdog'
   ```

2. Check the monitoring watchdog state:

   ```bash
   python3 ops/scripts/stream_v3_monitoring_watchdog.py --json --soft-exit
   ```

3. Refresh snapshots if live summary generation is slow but the state root is
   otherwise healthy:

   ```bash
   python3 ops/scripts/stream_v3_health_snapshot.py --json
   ```

4. Compare dashboard output with raw delivery evidence:

   ```text
   k3s Pod ready
   FFmpeg RTMPS socket connected
   YouTube public/live/ingest healthy
   same watch URL preserved
   capture and audio probes healthy
   recovery action plan not requesting destructive action
   ```

5. Only after raw delivery evidence fails should the incident be promoted from
   observability-plane failure to delivery-plane failure.

## What This Does Not Prove

Snapshot fallback is not a new uptime claim. It preserves operator visibility
when live aggregation is slow, and it makes staleness explicit. It does not prove
every viewer received every frame, and it does not replace long-window SLI
review.

The monitoring watchdog is also not production recovery authority by default.
Public examples keep repair disabled unless the operator explicitly configures a
local repair command and accepts that boundary.
