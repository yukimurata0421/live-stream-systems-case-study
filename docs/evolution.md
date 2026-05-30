# Evolution

stream_v3 is the current production shape of a 24/7 ADS-B YouTube streaming
system. The interesting part is not only the final k3s deployment, but the path
that forced the architecture to grow.

## v1: Single-Machine Prototype

The first `stream` implementation proved that a browser-rendered ADS-B view,
music playback, FFmpeg, and YouTube ingest could run as one host-managed stack.
It was useful, but process ownership and recovery behavior were tightly coupled.

## v2: Refactored Single-Host Runtime

`stream_v2` split the single-host runtime into clearer subsystems: delivery,
rendering, music, YouTube lifecycle, monitoring, watchdogs, and recovery. It
added SLI summaries, runbooks, restart budgets, API cost guards, and contract
tests.

The v2 work made the system operable, but the delivery path and observation path
still competed for the same host resources and process namespace.

## v3: k3s Runtime With Split Planes

`stream_v3` moves the delivery plane into k3s and keeps the observability plane
on the arena/prodesk side.

Delivery plane:

- browser rendering and overlay
- PulseAudio
- AutoDJ
- FFmpeg RTMPS ingest
- NVIDIA NVENC encode
- local fast recovery

Observability plane:

- YouTube resolver and monitor
- stream watchdog
- recovery orchestrator
- notify loop
- SLI summaries
- Prometheus, Loki, and Grafana
- remote recovery requests into the k3s workload

The split reduces recovery blast radius: delivery can focus on producing video
and audio, while the monitoring layer can retain state, classify faults, and
request staged recovery without owning the FFmpeg process directly.
