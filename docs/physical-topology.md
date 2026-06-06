# Physical Topology

`stream_v3` is the current production shape. The live delivery workload runs on
the Dell workstation, the HP ProDesk is both the ADS-B RF/source host and the
observability host, Raspberry Pi publishes the public-safe status snapshot, and
GCS + Cloudflare form the public static edge.

## Physical Hosts

| Host or edge | Runtime role | Responsibility |
| --- | --- | --- |
| HP ProDesk `192.168.0.60` | ADS-B source and observability | Airspy USB receiver, `airspy_adsb`, ProDesk-side readsb, YouTube resolver/watchdog, stream watchdog, subsystem SLI, notifications, Prometheus exporter on `:9108`, Prometheus `:9090`, Loki `:3100`, Alloy `:12345`, private Grafana `:3000`, recovery orchestration, and staged recovery requests |
| Dell workstation `192.168.0.35` | Delivery and local ADS-B mirror | Dell-side readsb and modified tar1090 map endpoint, k3s `stream-v3-runtime`, browser rendering, PulseAudio, AutoDJ, FFmpeg, NVIDIA NVENC, and local fast recovery |
| Raspberry Pi `192.168.0.50` | Public snapshot publisher and gateway | nginx `:8088` `/grafana/` proxy to HP ProDesk Grafana, public-safe snapshot collector, static site source tree, scheduled GCS push, and existing `adsb-open.addevlab.com` tunnel ingress |
| GCS + Cloudflare | Public static edge | Receives sanitized JSON/static assets by outbound upload and serves <https://yukimurata0421.dev/> without exposing Grafana, Prometheus, Loki, raw logs, credentials, or home-network ingress |

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
- the Raspberry Pi publishes a reduced static snapshot, so external readers can
  inspect freshness and guardrails without reaching the private monitoring
  backend;
- ADS-B source freshness, map availability, media delivery, and recovery
  decision quality can be classified as separate failure domains.

## Visualization Boundary

`ops/monitoring/docker-compose.yml` defines Prometheus, Loki, Grafana, and Alloy
with host networking and local scrape targets. That stack presents evidence from
the HP ProDesk observability side; it is not part of the k3s delivery workload
and does not directly own FFmpeg recovery.

Grafana `:3000`, Prometheus `:9090`, Loki `:3100`, Alloy, and the exporter stay
private on HP ProDesk. Raspberry Pi nginx exposes `/grafana/` as a proxy to HP
ProDesk Grafana; the public snapshot collector uses the Pi-local
`http://127.0.0.1:8088/grafana` path to query public-safe datasource endpoints.
The `yukimurata0421.dev` status path is then one-way: the Pi reduces evidence
to allowlisted static assets, pushes them outbound to GCS, and Cloudflare serves
them. Existing `adsb-open.addevlab.com` Grafana shortcut routes are a separate
Cloudflare Tunnel path that returns to Raspberry Pi nginx and then proxies to
HP ProDesk Grafana. Pi nginx shortcut paths such as `/stream-v3-grafana`
redirect into that `adsb-open` path.

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
