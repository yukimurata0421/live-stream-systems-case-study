# Architecture

`stream_v3` is the public source snapshot of the streaming platform. It is built around
one principle: keep media delivery and operational observation separate, while
also naming the ADS-B source chain as its own evidence boundary.

## System Shape

```text
Airspy USB on HP ProDesk
  -> airspy_adsb
  -> readsb on HP ProDesk
  -> readsb on Dell workstation
  -> Dell modified tar1090 HTTP endpoint
  -> sanitized ADS-B JSON proxy
  -> stream_v3 MapLibre rendering and overlay
  -> PulseAudio + AutoDJ
  -> FFmpeg / NVIDIA NVENC
  -> YouTube RTMPS

runtime evidence
  -> HP ProDesk observability services
  -> YouTube Data API / OAuth / public watch-page probes
  -> k3s runtime, state, and log evidence
  -> watchdogs
  -> subsystem classification
  -> SLI summaries
  -> ops/monitoring evidence presentation
  -> staged recovery request

public status publication
  -> Raspberry Pi collector initiates HTTP GET
  -> Pi-local /grafana/ proxy
  -> HP ProDesk Grafana datasource proxy
  -> HP ProDesk Prometheus/Loki
  -> datasource JSON response returns to Raspberry Pi collector
  -> outbound upload to GCS
  -> Cloudflare
  -> yukimurata0421.dev
```

## Physical Topology

The historical public data path is split across three home hosts plus a public
static edge. The current recovery-control contract additionally names
`arena-server` and `cra-01`; this repository does not infer their deployment
state from the older topology record.

- HP ProDesk `monitoring-host` source role: Airspy USB receiver, `airspy_adsb`,
  and ProDesk-side readsb.
- HP ProDesk `monitoring-host` k3s observability role: `stream-v3-control`,
  `stream-v3-observer`, YouTube monitoring, watchdogs, SLI, notifications,
  Prometheus exporter, staged recovery requests, and the
  Prometheus/Loki/Alloy/Grafana evidence stack.
- Dell workstation `delivery-host` local ADS-B mirror role: Dell-side readsb and
  modified tar1090 ADS-B HTTP endpoint.
- Dell workstation `delivery-host` delivery role: k3s `stream-v3-runtime`,
  custom MapLibre rendering, analysis-only precipitation, PulseAudio, AutoDJ,
  FFmpeg, NVENC, and local fast recovery.
- Raspberry Pi `publisher-host` public publisher role: nginx `:8088`
  `/grafana/` proxy to HP ProDesk Grafana, public-safe snapshot collection,
  static site build, and outbound GCS push.
- GCS + Cloudflare public edge role: serve sanitized static status snapshots
  uploaded outbound from Raspberry Pi, offloading public reads away from the
  home uplink without exposing Grafana, Prometheus, Loki, raw logs,
  credentials, or the home network.

This split is part of the architecture, not just a deployment detail. It keeps
the GPU/media delivery host focused on real-time output, keeps long-lived
monitoring state away from the delivery workload, and keeps the Airspy/readsb
source chain distinguishable from delivery failures.

## Plane Split

Delivery-plane components are optimized for keeping video and audio alive.
Observability-plane components are optimized for retaining evidence and
explaining faults. They do not own the current recovery authorization decision.

The retained HP ProDesk observability implementation runs
`stream_v3.control_loop --mode monitor`
as the k3s `stream-v3-control` workload. That monitor mode runs the YouTube
video resolver, YouTube watchdog, stream watchdog, notification status loop,
subsystem status summary, recovery orchestrator, and shadow SLI tasks. It pulls
read-only YouTube Data API, OAuth, public watch-page, k3s runtime, state-file,
and log evidence before recovery is planned.

The current contract routes Monitoring facts through arena-server to cra-01.
CRA owns policy, budget, cooldown, authorization, command lifecycle, and final
verification. Dell can execute only an exact fenced FFmpeg-child command and
must not escalate an absent child to a broader restart. Raspberry Pi remains a
one-way publisher with no control feedback.

`ops/monitoring/` defines Prometheus, Loki, Grafana, and Alloy as a
host-local evidence and presentation stack. It is not a third delivery plane and
does not own FFmpeg or k3s recovery directly. In the current production shape,
that monitoring backend runs on HP ProDesk alongside the ProDesk k3s
observability workloads. Raspberry Pi uses the Pi-local
`/grafana/` proxy to collect allowlisted evidence from the ProDesk Grafana
datasource proxy. The data transfer is pull-based: the Pi collector initiates
HTTP GETs to `127.0.0.1:8088/grafana`, Pi nginx proxies those requests to
`monitoring-host:3000/grafana`, and the datasource JSON response returns to the Pi
collector. The Pi then pushes a reduced static snapshot to GCS for Cloudflare to
serve at `yukimurata0421.dev`. That static edge is used to avoid spending home
uplink bandwidth on public status reads. Non-static operational access is
outside this public static publication path and is not named as a public
endpoint here.

## Source Boundary

The Airspy/readsb source path is not managed by the k3s manifests in this public
snapshot. The delivery runtime consumes `aircraft.json`, receiver metadata,
and range evidence from the Dell readsb / modified tar1090 endpoint through a
sanitizing local proxy. The viewer-facing page is the repository-owned
MapLibre renderer, not the upstream tar1090 page.

The production ADS-B handoff is ProDesk readsb Beast output to Dell
`delivery-host:30104`, where Dell readsb expands it into the local map endpoint
used by the k3s delivery runtime.

`src/stream_core/overlay_server.py` proxies ADS-B JSON, map/terrain tiles,
processed precipitation assets, and render-readiness state. Report-only checks
validate both the rendered overlay and the upstream readsb / modified tar1090
path without giving either probe direct recovery authority.

## Deployment Model

The k3s manifests are intentionally shadow-first:

- `deploy/k3s/shadow`: validates the workload with local capture and dry-run
  recovery behavior.
- `deploy/k3s/streaming`: enables the `stream_v3` delivery plane for live
  streaming on the Dell workstation.
- `deploy/k3s/v3-control`: runs the ProDesk-side observability monitor loop.
- `deploy/k3s/v3-observer`: exports v3 runtime state for scraping from the
  ProDesk-side observability k3s role.
- `deploy/k3s/v3-reports`: scheduled report jobs.
- `deploy/k3s/v2-state-mirror`: optional read-only state mirror for migration.

## Recovery Model

The current model is staged across distinct authorities:

1. collect evidence;
2. classify the subsystem state;
3. publish facts-only Monitoring evidence from arena-server;
4. let cra-01 apply policy, budget, cooldown, authorization, and command
   lifecycle rules;
5. execute at most one exact fenced FFmpeg-child effect on Dell; and
6. return durable result/evidence to CRA for final verification or explicit
   uncertainty.

The older direct arena/k3s scripts remain reviewable migration surfaces, not
the current authority contract. See
[scoped recovery authority](v3/scoped-recovery-authority.md).
