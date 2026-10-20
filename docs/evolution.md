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

`stream_v3` moves delivery into an explicit k3s workload on the Dell
workstation, and the HP ProDesk (`arena-server`) owns observation and control.
The monitor loop has k3s manifests (`stream-v3-control`, `stream-v3-observer`);
in production it runs as the `stream-v3-arena-monitor` service.

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

- monitor loop and exporter on the ProDesk
- YouTube resolver and monitor
- read-only YouTube Data API, OAuth, and public watch-page probes
- stream watchdog
- k3s runtime, state-file, and log evidence
- recovery orchestrator
- notify loop
- SLI summaries
- Prometheus exporter and `ops/monitoring` dashboards
- remote recovery requests into the k3s workload
- Raspberry Pi public snapshot publication to GCS + Cloudflare, with public
  reads offloaded away from the home uplink and Prometheus, Loki, Grafana, and
  raw logs kept private on the ProDesk

The retained split reduced recovery blast radius: delivery could focus on
producing video and audio, while the monitoring layer retained state,
classified faults, and staged requests without owning the FFmpeg process
directly.

## Monitoring v4 And Central Restart Authority

Later operation exposed a harder boundary: independent judgment paths could
form duplicate restart scopes, and an ambiguous response could be interpreted
as permission to try again. Request-level deduplication alone could not prove
that only one physical effect occurred.

The current public source therefore separates the next control path:

- Monitoring v4 on `arena-server` reduces typed observations into current,
  incident, parity, and facts-only evidence;
- CRA on `cra-01` commits policy, budget, cooldown, authorization, command
  lifecycle, reconciliation, and final verification;
- Dell admits one exact target-wide fenced FFmpeg-child effect; and
- `OUTCOME_UNKNOWN` blocks automatic retry until durable reconciliation.

This applies transaction ideas locally to intent, outbox, admission, and effect
fences; it is not distributed ACID. The public implementation and Harness are
reviewable, but production authority and a completed live soak are not claimed.

## October 2026: OCI Fan-Out And A Smaller Recovery Design

Adding Twitch had to preserve the YouTube broadcast without a permanent second
upload on the home uplink. On 2026-10-13 one Dell feed began going to an OCI
relay that forwarded it to both platforms. The route was then wrapped in signed
leases, a central route authority, and native qualification. After 104
qualification runs that design was still incomplete, and on 2026-10-20 it was
replaced by a restart-always relay, a forward keeper, and one fallback
controller. The [decision record](v3/oci-route-simplification-decision.md)
explains the trade-off and the [route record](v3/self-healing-oci-fanout.md)
describes the result.
