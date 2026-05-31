# Architecture

`stream_v3` is the current production streaming platform. It is built around
one principle: keep media delivery and operational observation separate, while
also naming the ADS-B source chain as its own evidence boundary.

## System Shape

```text
Airspy USB on HP ProDesk
  -> airspy_adsb
  -> readsb on HP ProDesk
  -> readsb on Dell workstation
  -> Dell modified tar1090 HTTP endpoint
  -> stream_v3 browser rendering and overlay
  -> PulseAudio + AutoDJ
  -> FFmpeg / NVIDIA NVENC
  -> YouTube RTMPS

runtime evidence
  -> HP ProDesk observability / arena services
  -> YouTube Data API / OAuth / public watch-page probes
  -> k3s runtime, state, and log evidence
  -> watchdogs
  -> subsystem classification
  -> SLI summaries
  -> ops/monitoring evidence presentation
  -> staged recovery request
```

## Physical Topology

The running system is intentionally split across two physical hosts with three
logical roles:

- HP ProDesk source role: Airspy USB receiver, `airspy_adsb`, and ProDesk-side
  readsb.
- HP ProDesk observability role: YouTube monitoring, watchdogs, SLI,
  notifications, Prometheus exporter, staged recovery requests, and the
  `ops/monitoring` evidence presentation stack when deployed there.
- Dell workstation delivery role: Dell-side readsb and modified tar1090 map
  endpoint, k3s `stream-v3-runtime`, browser rendering, PulseAudio, AutoDJ,
  FFmpeg, NVENC, and local fast recovery.

This split is part of the architecture, not just a deployment detail. It keeps
the GPU/media delivery host focused on real-time output, keeps long-lived
monitoring state away from the delivery workload, and keeps the Airspy/readsb
source chain distinguishable from delivery failures.

## Plane Split

Delivery-plane components are optimized for keeping video and audio alive.
Observability-plane components are optimized for retaining evidence, explaining
faults, and deciding whether a recovery action is safe.

The HP ProDesk observability plane runs `stream_v3.control_loop --mode monitor`.
That monitor mode runs the YouTube video resolver, YouTube watchdog, stream
watchdog, notification status loop, subsystem status summary, recovery
orchestrator, and shadow SLI tasks. It pulls read-only YouTube Data API, OAuth,
public watch-page, k3s runtime, state-file, and log evidence before recovery is
planned.

The split prevents a monitoring failure from automatically becoming a delivery
failure. It also prevents delivery recovery code from owning dashboard,
long-window SLI state, or YouTube API decision state.

`ops/monitoring/` defines Prometheus, Loki, Grafana, and Alloy as a
host-local evidence and presentation stack. It is not a third delivery plane and
does not own FFmpeg or k3s recovery directly.

## Source Boundary

The Airspy/readsb source path is not managed by the k3s manifests in this public
snapshot. The delivery runtime consumes it through a browser map upstream URL,
which points at the Dell readsb / modified tar1090 endpoint.

`src/stream_core/overlay_server.py` proxies the upstream map and ADS-B JSON for
the stream overlay and report-only checks validate both the overlay path and the
upstream readsb / modified tar1090 path.

## Deployment Model

The k3s manifests are intentionally shadow-first:

- `deploy/k3s/shadow`: validates the workload with local capture and dry-run
  recovery behavior.
- `deploy/k3s/streaming`: enables the `stream_v3` delivery plane for live
  streaming on the Dell workstation.
- `deploy/k3s/v3-observer`: exports v3 runtime state for scraping.
- `deploy/k3s/v3-reports`: scheduled report jobs.
- `deploy/k3s/v2-state-mirror`: optional read-only state mirror for migration.

## Recovery Model

Recovery is staged:

1. collect evidence;
2. classify the subsystem state;
3. build an action plan;
4. block destructive actions when evidence is stale, ambiguous, or in shadow
   mode;
5. request delivery-plane recovery only when the guard allows it.
