# Current Runtime Contract

## Purpose

This contract separates retained deployment evidence from the current public
source boundary. The retained v3 topology runs delivery on Dell k3s and the
earlier observability/control path on HP ProDesk k3s. The current recovery
contract adds facts-only Monitoring on `arena-server` and central authorization
on `cra-01`; public source integration is not proof of live deployment.

The retained production split is:

- HP ProDesk: Airspy USB, `airspy_adsb`, ProDesk-side readsb, k3s
  `stream-v3-control` / `stream-v3-observer`, and the private observability
  services, including Prometheus, Loki, Alloy, and private Grafana.
- Dell workstation: Dell-side readsb, modified tar1090 ADS-B endpoint, and the
  k3s `stream_v3` custom-map delivery workload.
- Raspberry Pi: nginx `/grafana/` proxy to HP ProDesk Grafana, public-safe
  snapshot collector that pulls datasource JSON via
  `127.0.0.1:8088/grafana`, static site source tree, and scheduled GCS push.
- GCS + Cloudflare: sanitized static status snapshot served at
  <https://yukimurata0421.dev/>. Public readers do not reach Grafana,
  Prometheus, Loki, raw logs, credentials, the home network, or the home uplink
  directly.

The ADS-B source chain is therefore Airspy on HP ProDesk -> `airspy_adsb` ->
ProDesk readsb -> Beast feed to Dell `delivery-host:30104` -> Dell readsb ->
Dell modified tar1090 ADS-B JSON -> sanitized proxy -> `stream_v3` MapLibre
browser rendering.

## Primary Media Route: 2026-10-20

Dell's main sender uploads one RTMPS feed over a private tunnel to MediaMTX on
OCI, which forwards it to YouTube and Twitch. MediaMTX restarts on failure. An
OCI keeper re-creates a forward that has failed for 30 seconds and renews the
Twitch session at 00:00 JST without touching the YouTube forward. A Dell
controller starts a direct upload to the YouTube backup ingest after 30 seconds
of relay failure and stops it after 300 seconds of relay health; that fallback
is the only automatic route action. Moving bytes, provider LIVE state, and
playable media remain separate evidence.

Read the [self-healing fan-out](self-healing-oci-fanout.md) for the drill and
limits, and the [decision record](oci-route-simplification-decision.md) for why
the signed-lease route with its fenced route-transition protocol was retired.
[OCI relay and Twitch](oci-relay-and-twitch.md) and
[Twitch renewal](twitch-session-renewal.md) keep the 2026-10-13 design and
exercise. The CRA policy in this repository still supplies no executable
production authority.

## Delivery Owner

- `stream-v3-runtime` deployment
- `stream-engine` container
- `precipitation-fetcher` container
- `auto-dj` container
- `fast-recovery-loop` container
- custom MapLibre rendering, precipitation, and overlay
- PulseAudio
- FFmpeg encoding producer and one RTMPS remux sender
- NVIDIA NVENC

## Retained Monitoring Owner

- `stream-v3-control` deployment
- `stream-v3-observer` deployment and service
- legacy/reference observability monitor systemd unit
- YouTube resolver and watchdog
- read-only YouTube Data API, OAuth, and public watch-page evidence
- stream watchdog
- k3s runtime, state-file, and log evidence
- notification loop
- subsystem status
- legacy recovery orchestrator migration surface
- shadow SLI
- Prometheus exporter
- 60-second map runtime probe
- 300-second public viewer synthetic probe
- `ops/monitoring` Prometheus, Loki, Grafana, and Alloy evidence presentation

## Current Recovery Authority

- `arena-server`: observations, current state, incidents, parity,
  notifications, and a signed facts-only projection; no command authority.
- `cra-01`: policy, budget, cooldown, authorization, durable command lifecycle,
  reconciliation, and final verification.
- Dell: signed local facts and one exact fenced FFmpeg-child effect; no
  automatic container, Pod, Deployment, or host escalation.
- Raspberry Pi: allowlisted static publication only.

Target identity includes host, boot, namespace, Pod UID, container identity,
FFmpeg generation, and PID. Unknown or drifted identity fails closed, and
`OUTCOME_UNKNOWN` requires reconciliation instead of an automatic retry.

## Encoder Baseline

```text
h264_nvenc
5 fps
3400k CBR video
6800k buffer
192k audio
48 kHz audio sample rate
```

This baseline is v3-specific in ownership and NVENC use, while preserving the
low-bandwidth lineage: first 5fps/3500k/audio192k, then
4fps/3400k/audio192k, then the current 5fps/3400k/audio192k contract after the
2026-05-31 fps tuning check.

## Audio Baseline

PulseAudio runs with shared memory disabled in the container path:

```text
--disable-shm=yes
--enable-memfd=no
```

This avoids container-specific `memblock` assertion failures.
