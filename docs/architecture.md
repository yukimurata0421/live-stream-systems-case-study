# Architecture

`stream_v3` is a small production-style streaming platform built around one
principle: keep media delivery and operational observation separate.

## System Shape

```text
Raspberry Pi ADS-B edge/source
  -> HP ProDesk observability / arena services
  -> Dell workstation k3s delivery runtime
  -> YouTube RTMPS

readsb / tar1090 / map source
  -> browser rendering
  -> overlay
  -> PulseAudio + AutoDJ
  -> FFmpeg
  -> YouTube RTMPS

runtime evidence
  -> watchdogs
  -> subsystem classification
  -> SLI summaries
  -> Prometheus / Loki / Grafana
  -> staged recovery request
```

## Physical Topology

The running system is intentionally split across three physical tiers:

- Dell workstation: k3s delivery node running `stream-v3-runtime`, browser
  rendering, PulseAudio, AutoDJ, FFmpeg, NVENC, and local fast recovery.
- HP ProDesk: observability / arena node running YouTube monitoring, watchdogs,
  SLI, Prometheus/Loki/Grafana, notifications, and staged recovery requests.
- Raspberry Pi: ADS-B edge/source node that provides the aircraft/map data feed
  consumed by the rendering path.

This physical split is part of the architecture, not just a deployment detail.
It keeps the GPU/media delivery host focused on real-time output, keeps
long-lived monitoring state away from the delivery workload, and keeps the ADS-B
source isolated at the edge.

## Plane Split

Delivery-plane components are optimized for keeping video and audio alive.
Observability-plane components are optimized for retaining evidence, explaining
faults, and deciding whether a recovery action is safe.

The split prevents a monitoring failure from automatically becoming a delivery
failure. It also prevents delivery recovery code from owning dashboard and
long-window SLI state.

## Deployment Model

The k3s manifests are intentionally shadow-first:

- `deploy/k3s/shadow`: validates the workload with local capture and dry-run
  recovery behavior.
- `deploy/k3s/streaming`: enables the delivery plane for live streaming.
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
