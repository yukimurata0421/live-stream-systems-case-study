# Map Rendering And Monitoring Contract

This document connects the viewer-facing ADS-B map to its delivery, weather,
monitoring, and recovery boundaries. It describes code and configuration
contracts in this public snapshot; it is not a current-production health report.

## Rendering Path

The Dell-side modified tar1090 endpoint remains the local ADS-B HTTP source.
The delivery runtime does not display that page directly. The overlay server
proxies its `aircraft.json` and sanitized `receiver.json` data into a dedicated
MapLibre renderer under `ui/overlay/adsb-map/`.

The renderer is intentionally aircraft-first:

- aircraft icons are shown without flight labels or retained tracks;
- range rings and the observed-coverage outline remain visible as reference
  geometry;
- receiver coordinates are removed from the proxied receiver payload;
- map, terrain, airport, and precipitation credits remain visible in the
  video frame; and
- aircraft, precipitation, and UI colors are not changed by the base-map
  time-of-day palette.

The base-map palette is calculated locally from UTC, latitude, and longitude.
It transitions between night, twilight, golden hour, and day without an
external sunrise API. The calculation updates once per minute and the paint
transition takes two minutes, while operational data layers keep fixed colors.

Map assets and third-party data credits are listed in
`ui/overlay/adsb-map/ATTRIBUTION.md`. MapLibre is vendored with its BSD
license. OpenFreeMap vector tiles and Mapterhorn terrain are requested through
the local overlay proxy, so their public endpoints are not embedded as a
browser-side control surface.

## Precipitation Boundary

`src/stream_core/precipitation_fetcher.py` publishes processed JMA
high-resolution precipitation tiles into the runtime state volume. It accepts
only the analysis frame where `basetime == validtime`; forecast frames are not
displayed. Source colors are recolored and assigned intensity-dependent
transparency before the browser reads the local generation.

The fetcher provides two atomic status files:

- `status.json` describes the active analysis generation, observation time,
  bounds, tile template, processing state, and stale threshold;
- `health.json` describes fetch success, retry state, and consecutive failures.

The browser keeps a fresh last-known-good generation during transient fetch
failure and fades the layer out after the configured stale limit. The default
poll interval is 300 seconds, the stale limit is 900 seconds, and retained
generations are bounded. A weather failure degrades weather evidence; by
itself it is not a delivery failure and does not authorize a stream restart.

The JMA website tile route is an operational interface rather than a contracted
API. Its metadata and data roots are configurable, and the attribution file
records the replacement boundary for deployments that require an SLA.

## Render Warmup

The browser reports readiness to the local overlay server only after both of
these conditions are true:

1. required map and terrain sources are loaded; and
2. at least one current ADS-B aircraft payload has been processed.

The overlay server publishes that heartbeat as `/render/status.json` and marks
it stale after 30 seconds. `stream-engine` checks the overlay page, custom map,
upstream modified tar1090 path, and current render heartbeat before starting or
restarting FFmpeg.

The public production examples use a 20-second warmup timeout and
`PRE_FFMPEG_REQUIRE_OVERLAY_READY=0`. That is deliberately fail-open: a timeout
is logged and retained as evidence, but it does not indefinitely block media
delivery. Setting the flag to `1` changes the contract to fail-closed.

## Runtime Readiness And GPU Guards

The streaming overlay of the k3s deployment has four containers:

- `stream-engine`;
- `precipitation-fetcher`;
- `auto-dj`; and
- `fast-recovery-loop`.

`src/stream_core/runtime_readiness.py` does not equate Pod readiness with
streaming readiness. It requires the current host boot ID, a live NVIDIA
driver, an FFmpeg process using `h264_nvenc`, minimum FFmpeg uptime, and an
established RTMP/RTMPS socket. A successful check writes an establishment
marker scoped to the current Pod UID and host boot.

The streaming manifest adds the `stream-v3.io/gpu-ready` scheduling gate. The
host startup helper releases it only after the node, NVIDIA runtime, and device
plugin are ready. Postboot verification checks the current boot annotations and
actual delivery readiness. Remote and staged recovery also perform GPU
preflight checks so a driver or device-plugin outage does not become a rollout
loop.

The host-maintenance helpers are documented in `ops/host-maintenance/README.md`.
They are not installed or started by public CI.

## Read-Only Monitoring Probes

The monitoring plane runs two independent probes. Their JSON state and JSONL
history belong under the local runtime-state root and are excluded from Git.

### Map runtime probe

`ops/scripts/stream_v3_map_runtime_probe.py` is designed for a 60-second
interval. It correlates:

- Deployment generation and ready/available replicas;
- all expected Pod containers, readiness, state, and restart count;
- runtime GPU, NVENC, FFmpeg, and RTMP/RTMPS readiness;
- the Chromium app process, SwiftShader flags, and WebGL fatal diagnostics;
- the map render heartbeat; and
- precipitation status and fetcher health.

Delivery-critical and weather results are separate. Missing containers, stale
rendering, WebGL failure, or failed NVENC/RTMP readiness can fail the delivery
contract. Precipitation failure alone produces a degraded weather result.

### Viewer synthetic probe

`ops/scripts/stream_v3_viewer_synthetic_probe.py` is designed for a 300-second
interval. It reads the selected video ID, uses `yt-dlp` to resolve a low-cost
public viewer URL, and asks FFmpeg for one small frame. It records frame
availability, black-frame detection, a compact perceptual fingerprint,
same-frame freeze evidence, sample age, and consecutive probe and visual
failure counters.

Resolved media URLs can contain signed query parameters. Probe error details
redact URLs, and JSONL history does not retain the resolved viewer URL. Latest
captures are operational artifacts and are excluded from the public snapshot.

The internal Prometheus series exported from these state files is the
monitoring source of truth. A reduced public dashboard can present allowlisted
results, but it is not sufficient by itself to classify an incident or
authorize recovery.

## Alerts, Notifications, And Recovery

`ops/monitoring/prometheus/rules/stream_v3_map.yml` keeps the alert boundary
explicit:

- a missing or stale map sample and a weather outage are warnings;
- delivery-contract or WebGL failure is critical;
- two consecutive viewer probe failures are a warning; and
- two consecutive black or frozen viewer samples are critical.

The viewer critical alert requires correlation with YouTube health, local
NVENC/RTMP evidence, and the map render heartbeat before recovery. The probes
themselves are read-only.

Discord remains the broad operational notification route. Slack receives
critical incidents immediately, delivery/GPU/RTMPS incident families
immediately, and other incidents only after the configurable sustained-active
threshold, which defaults to 1,800 seconds. Recovery events already escalated
to Slack are also resolved there. Failed webhook deliveries use the bounded
outbox rather than discarding the incident.

## Verification Map

The public tests cover:

- required renderer assets, attribution, lack of tracks/labels, solar-layer
  isolation, precipitation ordering, stale behavior, and render heartbeat;
- precipitation metadata selection, recoloring, atomic publication, retries,
  and last-known-good state;
- render warmup fail-open and fail-closed behavior;
- runtime readiness, boot/POD scoping, GPU scheduling and recovery guards;
- map and viewer probe classification and portable default paths;
- exporter metric mapping, Prometheus job labels, alert thresholds, URL
  redaction, notification routing, and outbox replay.

These tests validate deterministic contracts. They do not claim that public
tile services, JMA distribution, YouTube playback, a GPU driver, or a live
cluster is healthy at the time a pull request runs.
