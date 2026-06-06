# Evolution

stream_v3 is the current production shape of a 24/7 ADS-B YouTube streaming
system. The interesting part is not only the final k3s deployment, but the path
that forced the architecture to grow.

Timeline note: `stream_v2` remained the production authority during migration.
`stream_v3` became the current production shape only after runtime owner, state
root, CLI/supervisor path, metrics namespace, alert path, and recovery gates had
explicit cutover evidence. Mentions of shadow/read-only mode below describe that
migration safety rule, not the current owner.

## v1: Single-Machine Prototype

The first `stream` implementation proved that a browser-rendered ADS-B view,
music playback, FFmpeg, and YouTube ingest could run as one host-managed stack.
It was useful, but process ownership and recovery behavior were tightly coupled.

## v2: Refactored Single-Host Runtime

`stream_v2` split the single-host runtime into clearer subsystems: delivery,
rendering, music, YouTube lifecycle, monitoring, watchdogs, and recovery. It
added SLI summaries, runbooks, restart budgets, API cost guards, and contract
tests.

The important v2 lesson was not only "more monitoring." It was that every
recovery action needed an owner, fresh evidence, a same-URL guard, and a clear
boundary between current incident, historical degradation, and observability
noise. Weak signals such as public probes, map movement checks, TLS
strings, and API quota warnings were kept report-only until their false-positive
rate and viewer impact were understood.

The v2 media profile also changed as upload evidence accumulated. The
low-bandwidth RTMPS profile started as 5fps/3500k/audio192k, then settled into
4fps/3400k/audio192k in the later v2 production routine checks.

The v2 work made the system operable, but the delivery path and observation path
still competed for the same host resources and process namespace.

## v3: k3s Runtime With Split Planes

`stream_v3` moves the delivery plane into k3s and keeps the observability plane
on the HP ProDesk observability side.

The v3 migration is not a rewrite that discards v2 safety. The delivery runtime
moved, but the v2 design constraints remain: preserve the YouTube watch URL,
avoid duplicate publishers, gate destructive YouTube actions, separate current
health from long-window history, and keep shadow/read-only evidence out of
production authority until cutover is explicit.

The encoder target is a deliberate regime change, not a denial of the v2
history: v3 uses NVIDIA NVENC CBR, kept the low-fps 3400k contract from v2,
then adopted the env-synced 5fps/3400k current contract after a 4fps, 5fps, and
10fps tuning check showed 5fps stayed under the upload ceiling with a smaller
per-frame quality trade-off than 10fps.

Delivery plane:

- browser rendering and overlay
- PulseAudio
- AutoDJ
- FFmpeg RTMPS ingest
- NVIDIA NVENC encode
- local fast recovery

Observability plane:

- YouTube resolver and monitor
- read-only YouTube Data API, OAuth, and public watch-page probes
- stream watchdog
- k3s runtime, state-file, and log evidence
- recovery orchestrator
- notify loop
- SLI summaries
- Prometheus exporter and `ops/monitoring` dashboards
- remote recovery requests into the k3s workload
- Raspberry Pi public status UI and `/grafana/` gateway, with Prometheus and
  Loki still hosted on HP ProDesk

The split reduces recovery blast radius: delivery can focus on producing video
and audio, while the monitoring layer can retain state, classify faults, and
request staged recovery without owning the FFmpeg process directly.
