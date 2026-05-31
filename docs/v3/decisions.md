# Decisions

## Delivery / Observability Split

Status: accepted

The Dell delivery host owns video, audio, FFmpeg, AutoDJ, k3s runtime, and local
recovery. The HP ProDesk arena/prodesk side owns monitoring, SLI, notification,
and staged recovery requests.

The HP ProDesk also hosts the physical ADS-B RF ingest chain: Airspy USB,
`airspy_adsb`, and ProDesk-side readsb. The Dell workstation receives that feed
into its own readsb and modified tar1090 map endpoint, which is what
`stream_v3` renders and publishes.

Consequence: the monitoring layer needs remote runtime evidence instead of
directly inspecting every in-Pod socket.

## NVENC CBR Baseline

Status: accepted

The runtime uses NVIDIA NVENC with CBR because YouTube health was more stable
than lower-bitrate VBR experiments.

## PulseAudio Shared Memory Policy

Status: accepted

Container PulseAudio uses `--disable-shm=yes` and `--enable-memfd=no` to avoid
container memory transport failures.

## SLO Error Budget Policy

Status: accepted

The primary public outcome is availability and same-URL continuity. Visual
quality and upload efficiency are tuned inside that boundary.

## Host Freeze Recovery

Status: accepted

In-Pod recovery cannot fix a frozen host. Host watchdog configuration is part of
the operational model for single-node deployments.
