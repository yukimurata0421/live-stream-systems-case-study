# Physical Topology

`stream_v3` is the current production shape. The live delivery workload runs on
the Dell workstation, while the HP ProDesk is both the ADS-B RF/source host and
the observability host.

## Physical Hosts

| Host | Runtime role | Responsibility |
| --- | --- | --- |
| HP ProDesk | ADS-B source and observability | Airspy USB receiver, `airspy_adsb`, ProDesk-side readsb, monitoring, SLI, Prometheus/Loki/Grafana, notifications, recovery orchestration, and staged recovery requests |
| Dell workstation | Delivery and local ADS-B mirror | Dell-side readsb and modified tar1090 map endpoint, k3s `stream-v3-runtime`, browser rendering, PulseAudio, AutoDJ, FFmpeg, NVIDIA NVENC, and local fast recovery |

## ADS-B Data Flow

The production ADS-B path is:

```text
Airspy USB on HP ProDesk
  -> airspy_adsb
  -> readsb on HP ProDesk
  -> readsb on Dell workstation
  -> Dell modified tar1090 HTTP endpoint
  -> stream_v3 browser rendering and overlay
  -> FFmpeg/NVENC
  -> YouTube Live
```

This repository does not manage the Airspy device or the ProDesk readsb process
directly. In the public code, that source chain appears as the
`STREAM1090_URL` / `BROWSER_URL` upstream contract used by the delivery runtime.

## Why It Matters

The physical split makes the delivery/observability split real:

- the Dell workstation spends its resources on local readsb/tar1090 serving,
  browser rendering, audio, GPU encoding, and YouTube ingest;
- the HP ProDesk keeps RF ingestion, monitoring state, dashboards, long-window
  SLI, and staged recovery logic away from the k3s delivery workload;
- ADS-B source freshness, map availability, media delivery, and recovery
  decision quality can be classified as separate failure domains.

## k3s Boundary

k3s is used for the `stream_v3` delivery workload on the Dell workstation. The
observability plane may request staged recovery, but it does not directly own
the FFmpeg process.

## Code Boundary

- `deploy/k3s/base/configmap-shadow.yaml` defines `STREAM1090_URL` and
  `BROWSER_URL`.
- `deploy/k3s/streaming/patch-configmap-streaming.yaml` points the live
  delivery path at the Dell-side modified tar1090 endpoint.
- `src/stream_core/overlay_server.py` proxies `/stream1090/` and ADS-B JSON
  from that upstream endpoint and sanitizes receiver location fields. The
  `/stream1090/` path is an internal legacy route name.
- `src/stream_core/commands/stream1090_report.py` validates overlay and
  upstream readsb / modified tar1090 availability.

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
