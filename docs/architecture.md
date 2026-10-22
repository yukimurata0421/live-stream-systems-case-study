# Architecture

This page is the single description of the current hosts, data paths, and
ownership boundaries. The [runtime contract](runtime-contract.md) holds the
delivery settings, [evolution](evolution.md) holds the earlier topologies, and
[current status](current-status.md) holds recent changes and unobserved items.

The design keeps media delivery, observation, and public publication on
separate owners, so a warning in one cannot silently become an action in
another.

## Hosts And Roles

| Host or edge | Role | Runs |
| --- | --- | --- |
| HP ProDesk `arena-server` | ADS-B reception and monitoring | Airspy USB receiver, `airspy_adsb`, readsb; the monitor service `stream-v3-arena-monitor` (YouTube resolver and watchdog, stream watchdog, notifications, SLI, exporter); Prometheus, Loki, and Grafana in a local k3s; relay observation over the private tunnel |
| Dell workstation `delivery-host` | Video production and upload | Dell-side readsb and modified tar1090 endpoint; k3s `stream-v3-runtime` (map rendering, precipitation, PulseAudio, AutoDJ, FFmpeg/NVENC, fast recovery); the fallback Deployment and its controller; an independent RAM return-feed observer |
| OCI VM | Media fan-out | MediaMTX relay and the keeper that repairs forwards and renews Twitch |
| Raspberry Pi `publisher-host` | Public status publisher | nginx `/grafana/` proxy to arena-server Grafana, snapshot collectors, static site build, outbound GCS upload |
| GCS + Cloudflare | Public static edge | Serves <https://yukimurata0421.dev/> from uploaded static files |
| `cra-01` | CRA design target | Central Restart Authority source; production authority is disabled, and it takes no part in the media route |

Monitoring and ADS-B reception share the ProDesk; video production has the Dell
to itself. The [hardware selection](hardware-selection.md) explains why.

## Media Path

```text
Airspy USB (arena-server)
  -> airspy_adsb -> readsb (arena-server)
  -> Beast feed to Dell delivery-host:30104
  -> readsb -> modified tar1090 HTTP endpoint (Dell)
  -> sanitized ADS-B JSON proxy
  -> stream_v3 MapLibre rendering and overlay
  -> PulseAudio + AutoDJ
  -> FFmpeg / NVIDIA NVENC
  -> RTMPS over a private tunnel
  -> OCI MediaMTX relay
       -> YouTube primary ingest
       -> Twitch ingest

fallback and recovery confirmation only
  Dell fallback Pod: separate map/audio workload + CPU encoder
  -> direct RTMPS to the same broadcast's YouTube backup ingest
```

The fallback bypasses OCI but shares the Dell host, home power, and home
uplink with the main sender. The [route record](v3/self-healing-oci-fanout.md)
defines when it starts and stops.

The viewer-facing map is the repository-owned MapLibre renderer, not the
upstream tar1090 page. `src/stream_core/overlay_server.py` proxies ADS-B JSON,
map and terrain tiles, precipitation assets, and render readiness, and removes
receiver location fields. The k3s manifests do not manage the Airspy device or
readsb.

## Clock Observation Path

The OCI relay has a third read branch to a separate Dell monitor Deployment.
That branch observes the common relay output, alongside the independent native
YouTube/Twitch forward counters already collected by arena-server. During direct
fallback, a secondary tee/FIFO output of the backup encoder reaches the same
Dell monitor through a bounded UDP receiver. It adds no second encode.

The observer returns signed RAM snapshots, clock facts and heartbeat over the
existing read-only SSH path to ProDesk. ProDesk binds the source and current
runtime, deduplicates cycles, joins existing native relay facts and exports
observation metrics. Neither the image worker nor this importer can restart a
sender, scale a Deployment, switch a route or request a recovery action.
The [clock contract](v3/relay-clock-monitor.md) separates returned video,
per-destination byte progress, observation quality and public reception.

An independent [thumbnail worker](v3/twitch-thumbnail-monitor.md) runs entirely on
arena-server. An adapter reuses the existing Get Streams response and mirrors
its URL into tmpfs; the worker fetches a bounded image once per minute, keeps
ordinary images in RAM and joins paired-clock results with the existing API and
native facts. Its scope is a provider thumbnail, not public-player reception.
It adds no work to the Dell media path or OCI relay and has no recovery authority.

## Public Status Path

```text
Pi collector -- HTTP GET --> Pi-local /grafana/ proxy --> arena-server Grafana
             <-- datasource JSON (Prometheus / Loki) --
Pi collector -- relay and Twitch observations, read-only pull --> arena-server
Pi -> filtered static files -> outbound upload -> GCS -> Cloudflare -> yukimurata0421.dev
```

The Pi initiates every request; arena-server never pushes to it. Public readers
reach only the static edge, so they do not load the home uplink and cannot
reach Grafana, Prometheus, Loki, raw logs, credentials, or the home network.
Non-static operational access is outside this path and is not named here. The
[publisher boundary](v3/public-publisher-boundary.md) lists what is allowed
into the snapshot.

## Who Can Act On What

| Actor | Can change | Cannot change |
| --- | --- | --- |
| Fast recovery inside the Dell Pod | The FFmpeg child of its own Pod | Pods, Deployments, or the relay |
| Kubernetes on Dell | Restarts a failed container or Pod | Anything on OCI |
| Fallback controller on Dell | Scales only `stream-v3-youtube-backup` between 0 and 1 | The main Pod or the relay |
| Keeper on OCI | Re-creates a failed forward; renews the Twitch forward | The YouTube forward during renewal; anything at home |
| Clock observer on Dell / its importer on arena-server | Their bounded RAM snapshots, independent observation state and metrics | Senders, routes, replicas, provider lifecycle or recovery effects |
| Monitor on arena-server | Requests a guarded runtime restart (budget, cooldown, 180-second convergence wait) | The FFmpeg process directly; replacement broadcasts (disabled) |
| Raspberry Pi | Its own static output | Anything else; it receives no control feedback |
| CRA on `cra-01` | Nothing in production | Its policy keeps `production_behavior_change_authorized=false` |

The route's self-healing pieces (keeper and fallback controller) use fixed
thresholds and do not depend on the monitor, so they keep working when
monitoring is down. The guarded restart gates are described in the
[runtime contract](runtime-contract.md#guarded-restart-gates).

### CRA design

CRA is a stricter recovery design kept in the source: monitoring publishes
facts only, `cra-01` owns policy, budget, cooldown, authorization, and command
lifecycle, and Dell executes at most one exact fenced FFmpeg-child effect.
`OUTCOME_UNKNOWN` blocks automatic retry until reconciliation. It is not enabled
in production, and the current route does not use it; the
[decision record](v3/oci-route-simplification-decision.md) explains why. Read
[scoped recovery authority](v3/scoped-recovery-authority.md) and the
[recovery-control README](../recovery-control/README.md) for the design.

## Failure Domains

| Domain | Examples | Usual owner |
| --- | --- | --- |
| RF and source | Airspy, `airspy_adsb`, readsb freshness on either host | arena-server, Dell readsb |
| Map and source data | tar1090 endpoint, tiles, precipitation, browser render heartbeat | Dell runtime |
| Media runtime | Browser, PulseAudio, AutoDJ, FFmpeg, NVENC | Dell runtime |
| Cloud media route | Private tunnel, relay host, a provider forward | Relay, keeper, fallback controller |
| Home network | WAN or carrier interruption | Not recoverable by any host; diagnosed by [route observation](v3/tcp-stall-case-study.md) |
| Observation | Stale evidence, exporter timeouts, monitoring drift | arena-server |
| Publication | Stale snapshot, cache policy | Raspberry Pi |

A failure in one domain does not establish a failure in another. That is why a
source or monitoring warning is not treated as a stream-restart condition.

## Deployment Layout

| Path | Purpose |
| --- | --- |
| `deploy/k3s/streaming` | Live delivery overlay for the Dell runtime |
| `deploy/k3s/shadow` | Shadow validation with local capture and dry-run recovery |
| `deploy/k3s/youtube-backup` | Fallback Deployment, normally zero replicas |
| `deploy/k3s/v3-control`, `deploy/k3s/v3-observer` | k3s packaging of the monitor loop and exporter; production runs the same loop as the arena-server service |
| `deploy/k3s/v3-reports` | Scheduled report jobs |
| `deploy/k3s/v2-state-mirror` | Optional read-only state mirror used during migration |
| `ops/selfheal/` | Keeper, fallback controller, and example units |
| `ops/monitoring/` | Prometheus, Loki, Grafana, and Alloy configuration |
| `ops/public-publisher/` | Pi collectors, site, and uploader |
