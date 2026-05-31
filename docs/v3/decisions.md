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

## Production Authority Transfer

Status: accepted

During migration, a healthy v3 Pod or green shadow acceptance run is not enough
to transfer production authority. Authority moves only through explicit cutover
evidence: runtime owner, state root, CLI/supervisor path, metrics namespace,
alert path, and recovery gates must all point at the intended production owner.

Consequence: v3 could run shadow/read-only checks without bypassing the safety
model that v2 established. After cutover, the same rule explains why current
production ownership is tied to explicit runtime, state, metrics, alert, and
recovery evidence rather than to Pod readiness alone.

## Metrics Namespace And Query Isolation

Status: accepted

v3 evidence uses `stream_v3_*` metrics and the v3 arena monitor job. Dashboard
queries must not aggregate v2 production and v3 shadow-compatible series unless
the source label is explicitly scoped.

Consequence: dashboard red state must be traced to raw series, source labels,
and exporter input before it becomes an incident.

## Exporter Shape Validation

Status: accepted

Exporter mappings are tested against the actual health-summary and runtime-state
JSON shapes. A dashboard panel is not trusted only because the query evaluates;
the path from raw JSON to Prometheus series is part of the contract.

Consequence: stale or mis-mapped fields become observability bugs instead of
delivery incidents.

## NVENC CBR Baseline

Status: accepted

The runtime uses NVIDIA NVENC with CBR because YouTube health was more stable
than lower-bitrate VBR experiments.

This decision is v3-specific. The inherited v2 production lineage was
low-bandwidth: first 5fps/3500k/audio192k, then 4fps/3400k/audio192k. v3 keeps
that cadence in the current env-synced runtime while changing the production
encoder to NVENC CBR.

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
