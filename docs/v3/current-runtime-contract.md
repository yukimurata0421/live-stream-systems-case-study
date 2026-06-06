# Current Runtime Contract

## Purpose

`stream_v3` runs the live streaming delivery path on k3s and keeps monitoring on
the HP ProDesk observability side.

The current production split is:

- HP ProDesk: Airspy USB, `airspy_adsb`, ProDesk-side readsb, and the
  observability services, including Prometheus, Loki, Alloy, and
  private Grafana.
- Dell workstation: Dell-side readsb, modified tar1090 map endpoint, and the
  k3s `stream_v3` delivery workload.
- Raspberry Pi: nginx `/grafana/` proxy to HP ProDesk Grafana, public-safe
  snapshot collector, static site source tree, scheduled GCS push, and existing
  `adsb-open.addevlab.com` tunnel ingress.
- GCS + Cloudflare: sanitized static status snapshot served at
  <https://yukimurata0421.dev/>. Public readers do not reach Grafana,
  Prometheus, Loki, raw logs, credentials, or the home network directly.

The ADS-B source chain is therefore Airspy on HP ProDesk -> `airspy_adsb` ->
ProDesk readsb -> Beast feed to Dell `192.168.0.35:30104` -> Dell readsb ->
Dell modified tar1090 -> `stream_v3` browser rendering.

## Delivery Owner

- `stream-v3-runtime` deployment
- `stream-engine` container
- `auto-dj` container
- `fast-recovery-loop` container
- browser rendering and overlay
- PulseAudio
- FFmpeg RTMPS ingest
- NVIDIA NVENC

## Monitoring Owner

- observability monitor systemd unit
- YouTube resolver and watchdog
- read-only YouTube Data API, OAuth, and public watch-page evidence
- stream watchdog
- k3s runtime, state-file, and log evidence
- notification loop
- subsystem status
- recovery orchestrator
- shadow SLI
- Prometheus exporter
- `ops/monitoring` Prometheus, Loki, Grafana, and Alloy evidence presentation

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
