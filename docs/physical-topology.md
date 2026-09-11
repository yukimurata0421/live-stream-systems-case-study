# Physical Topology

This page preserves the three-home-host production topology supported by the
retained v3 evidence: Dell runs delivery, HP ProDesk owns the ADS-B RF/source
and earlier observability/control path, Raspberry Pi publishes the public-safe
snapshot, and GCS + Cloudflare form the public static edge. The newer
Monitoring v4/CRA source contract is listed separately and is not inferred to
be live from this topology record.

## Physical Hosts

| Host or edge | Runtime role | Responsibility |
| --- | --- | --- |
| HP ProDesk `monitoring-host` | ADS-B source and retained k3s observability | Airspy USB receiver, `airspy_adsb`, ProDesk-side readsb, k3s `stream-v3-control`, k3s `stream-v3-observer`, YouTube resolver/watchdog, stream watchdog, subsystem SLI, notifications, private evidence stack, and legacy staged recovery surfaces |
| Dell workstation `delivery-host` | Delivery and local ADS-B mirror | Dell-side readsb and modified tar1090 ADS-B endpoint, k3s `stream-v3-runtime`, custom MapLibre rendering, precipitation fetcher, PulseAudio, AutoDJ, FFmpeg, NVIDIA NVENC, and local fast recovery |
| Raspberry Pi `publisher-host` | Public snapshot publisher and gateway | nginx `:8088` `/grafana/` proxy to HP ProDesk Grafana, public-safe snapshot collector, static site source tree, and scheduled GCS push |
| GCS + Cloudflare | Public static edge | Receives sanitized JSON/static assets by outbound upload and serves <https://yukimurata0421.dev/> without spending home uplink bandwidth on public status reads or exposing Grafana, Prometheus, Loki, raw logs, credentials, or home-network ingress |

## ADS-B Data Flow

The production ADS-B path is:

```text
Airspy USB on HP ProDesk
  -> airspy_adsb
  -> readsb on HP ProDesk
  -> Beast feed to Dell delivery-host:30104
  -> readsb on Dell workstation
  -> Dell modified tar1090 HTTP endpoint
  -> sanitized ADS-B JSON proxy
  -> stream_v3 MapLibre rendering and overlay
  -> FFmpeg/NVENC
  -> YouTube Live
```

This repository does not manage the Airspy device or the ProDesk readsb process
directly. In the public code, that source chain appears as the ADS-B JSON and
range-evidence upstream contract used by the delivery runtime.

## Why It Matters

The physical split makes the delivery/observability split real:

- the Dell workstation spends its resources on local readsb/tar1090 serving,
  browser rendering, audio, GPU encoding, and YouTube ingest;
- the HP ProDesk keeps RF ingestion plus k3s-owned observability/control
  workloads for YouTube API/public watch evidence, monitoring state,
  dashboards, long-window SLI, and staged recovery logic away from the Dell
  k3s delivery workload;
- the Raspberry Pi publishes a reduced static snapshot, so external readers can
  inspect freshness and guardrails without reaching the private monitoring
  backend;
- ADS-B source freshness, map availability, media delivery, and recovery
  decision quality can be classified as separate failure domains.

## Current Authority Overlay

The current public source adds `arena-server` as a facts-only Monitoring owner
and `cra-01` as Central Restart Authority. Dell retains only signed local facts
and one exact fenced FFmpeg-child effect; Raspberry Pi remains publish-only.
This overlay is a source contract, not proof that those hosts or the CRA path
are deployed or production-authorized. See
[`v3/scoped-recovery-authority.md`](v3/scoped-recovery-authority.md).

## Visualization Boundary

`ops/monitoring/docker-compose.yml` defines Prometheus, Loki, Grafana, and Alloy
with host networking and local scrape targets. That stack presents evidence from
the HP ProDesk k3s observability side; it is not part of the Dell k3s delivery
workload and does not directly own FFmpeg recovery.

Grafana `:3000`, Prometheus `:9090`, Loki `:3100`, Alloy, and the exporter stay
private on HP ProDesk. Raspberry Pi nginx exposes `/grafana/` as a proxy to HP
ProDesk Grafana; the public snapshot collector uses the Pi-local
`http://127.0.0.1:8088/grafana` path to query public-safe datasource endpoints.
This is a Pi-initiated pull: Pi nginx forwards collector requests to
`monitoring-host:3000/grafana`, and the Grafana datasource JSON response returns
to the Pi collector before the static snapshot is built.
The `yukimurata0421.dev` status path is then one-way: the Pi reduces evidence
to allowlisted static assets, pushes them outbound to GCS, and Cloudflare serves
them. This keeps public status reads on the static edge instead of on the home
uplink. Non-static operational access is outside the public static status path
and is not named as a public endpoint here.

## k3s Boundary

k3s is used for the retained `stream_v3` delivery workload on Dell and the
earlier observability/control workloads on HP ProDesk. Those legacy request
paths do not supersede the current authority overlay: Monitoring cannot
authorize recovery or directly own the FFmpeg process.

## Code Boundary

- `deploy/k3s/base/configmap-shadow.yaml` contains the ADS-B upstream, custom
  map path, precipitation, and render-warmup defaults consumed by the runtime.
- `deploy/k3s/streaming/patch-configmap-streaming.yaml` points the live
  delivery path at the Dell-side modified tar1090 endpoint.
- `src/stream_core/overlay_server.py` proxies ADS-B JSON from that upstream,
  sanitizes receiver location fields, bounds map/terrain access, serves local
  precipitation generations, and records render readiness.
- report-only delivery checks validate overlay and upstream readsb / modified
  tar1090 availability.

## Failure-Domain Boundary

The topology separates these failure domains:

- RF/source chain failure: Airspy, `airspy_adsb`, ProDesk readsb, or Dell readsb
  feed freshness;
- map/source failure: Dell modified tar1090 ADS-B availability, map/terrain
  delivery, precipitation freshness, or browser render heartbeat;
- delivery/media runtime failure: browser, overlay, PulseAudio, AutoDJ, FFmpeg,
  NVENC, RTMPS, or upload path;
- observability/classification failure: stale evidence, unsafe action plans, or
  monitoring-state drift.

That separation is what lets the system avoid treating every source or
monitoring warning as a stream restart condition.
