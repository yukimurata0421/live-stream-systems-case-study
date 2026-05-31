# Physical Topology

`stream_v3` is the current production shape. The live delivery workload runs on
the Dell workstation, the HP ProDesk is both the ADS-B RF/source host and the
observability host, and the Raspberry Pi is the public status/dashboard gateway.

## Physical Hosts

| Host | Runtime role | Responsibility |
| --- | --- | --- |
| HP ProDesk `192.168.0.60` | ADS-B source and observability | Airspy USB receiver, `airspy_adsb`, ProDesk-side readsb, YouTube resolver/watchdog, stream watchdog, subsystem SLI, notifications, Prometheus exporter on `:9108`, Prometheus `:9090`, Loki `:3100`, Alloy `:12345`, Grafana `:3000`, recovery orchestration, and staged recovery requests |
| Dell workstation `192.168.0.35` | Delivery and local ADS-B mirror | Dell-side readsb and modified tar1090 map endpoint, k3s `stream-v3-runtime`, browser rendering, PulseAudio, AutoDJ, FFmpeg, NVIDIA NVENC, and local fast recovery |
| Raspberry Pi `192.168.0.50` | Public gateway | nginx `:8088` status UI and `/grafana/` proxy to HP ProDesk Grafana. Prometheus and Loki are not hosted here in the current production shape |

## ADS-B Data Flow

The production ADS-B path is:

```text
Airspy USB on HP ProDesk
  -> airspy_adsb
  -> readsb on HP ProDesk
  -> Beast feed to Dell 192.168.0.35:30104
  -> readsb on Dell workstation
  -> Dell modified tar1090 HTTP endpoint
  -> stream_v3 browser rendering and overlay
  -> FFmpeg/NVENC
  -> YouTube Live
```

This repository does not manage the Airspy device or the ProDesk readsb process
directly. In the public code, that source chain appears as the
browser map upstream contract used by the delivery runtime.

## Why It Matters

The physical split makes the delivery/observability split real:

- the Dell workstation spends its resources on local readsb/tar1090 serving,
  browser rendering, audio, GPU encoding, and YouTube ingest;
- the HP ProDesk keeps RF ingestion, YouTube API/public watch evidence,
  monitoring state, dashboards, long-window SLI, and staged recovery logic away
  from the k3s delivery workload;
- the Raspberry Pi gives the public side a small nginx gateway and status UI
  without becoming the Prometheus/Loki backend;
- ADS-B source freshness, map availability, media delivery, and recovery
  decision quality can be classified as separate failure domains.

## Visualization Boundary

`ops/monitoring/docker-compose.yml` defines Prometheus, Loki, Grafana, and Alloy
with host networking and local scrape targets. That stack presents evidence from
the HP ProDesk observability side; it is not part of the k3s delivery workload
and does not directly own FFmpeg recovery.

Raspberry Pi currently serves nginx `:8088` and proxies `/grafana/` to HP
ProDesk Grafana. Prometheus `:9090` and Loki `:3100` are not migrated to
Raspberry Pi; moving them would require exposing or proxying the HP ProDesk
exporter and retargeting Alloy's Loki write path.

## k3s Boundary

k3s is used for the `stream_v3` delivery workload on the Dell workstation. The
observability plane may request staged recovery, but it does not directly own
the FFmpeg process.

## Code Boundary

- `deploy/k3s/base/configmap-shadow.yaml` contains the browser map upstream
  URL defaults consumed by the delivery runtime.
- `deploy/k3s/streaming/patch-configmap-streaming.yaml` points the live
  delivery path at the Dell-side modified tar1090 endpoint.
- `src/stream_core/overlay_server.py` proxies the browser map and ADS-B JSON
  from that upstream endpoint and sanitizes receiver location fields.
- report-only delivery checks validate overlay and upstream readsb / modified
  tar1090 availability.

## Failure-Domain Boundary

The topology separates these failure domains:

- RF/source chain failure: Airspy, `airspy_adsb`, ProDesk readsb, or Dell readsb
  feed freshness;
- map/HTTP source failure: Dell modified tar1090 availability;
- delivery/media runtime failure: browser, overlay, PulseAudio, AutoDJ, FFmpeg,
  NVENC, RTMPS, or upload path;
- observability/classification failure: stale evidence, unsafe action plans, or
  monitoring-state drift.

That separation is what lets the system avoid treating every source or
monitoring warning as a stream restart condition.
