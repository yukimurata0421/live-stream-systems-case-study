# Current Runtime Contract

## Purpose

`stream_v3` runs the live streaming delivery path on k3s and keeps monitoring on
the arena/prodesk side.

The current production split is:

- HP ProDesk: Airspy USB, `airspy_adsb`, ProDesk-side readsb, and the
  observability / arena services.
- Dell workstation: Dell-side readsb, modified tar1090 map endpoint, and the
  k3s `stream_v3` delivery workload.

The ADS-B source chain is therefore Airspy on HP ProDesk -> `airspy_adsb` ->
ProDesk readsb -> Dell readsb -> Dell modified tar1090 -> `stream_v3`
browser rendering.

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

- arena monitor systemd unit
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
30 fps
3300k CBR video
6600k buffer
192k audio
48 kHz audio sample rate
```

## Audio Baseline

PulseAudio runs with shared memory disabled in the container path:

```text
--disable-shm=yes
--enable-memfd=no
```

This avoids container-specific `memblock` assertion failures.
