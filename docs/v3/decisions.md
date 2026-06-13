# Decisions

## Delivery / Observability Split

Status: accepted

The Dell delivery host owns video, audio, FFmpeg, AutoDJ, k3s runtime, and local
recovery. The HP ProDesk observability side also runs k3s for
`stream-v3-control` and `stream-v3-observer`, and owns monitoring, SLI,
notification, and staged recovery requests.

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

## Shadow Gate Semantics

Status: accepted

`shadow_budget_not_enforced` and `shadow_cooldown_not_enforced` are deliberate
shadow-plan markers, not production policy. Shadow evaluation must stay
non-mutating: it cannot consume restart budget, write cooldown state, or perform
live recovery. Production budget and cooldown enforcement is owned by the
mutating recovery paths documented in `docs/runtime-contract.md`.

Consequence: a green shadow action plan proves evidence classification and
destructive-action blocking, not that production restart budgets were bypassed.

## Current Classifier Replay

Status: accepted

Historical `production_without_shadow` counts must remain visible. When a
classifier is improved, the remediation is shown as `current_classifier_replay`
over retained production events, not by rewriting old orchestrator JSONL.

Consequence: reviewers can distinguish historical gaps from current classifier
coverage. A replay pass proves present classification behavior; it does not
pretend that the executor had already produced those historical intents.

## Compliance And Licensing Boundary

Status: accepted

ADS-B radio publication, receiver privacy, and NCS music attribution are treated
as design constraints. The public record documents the operator's risk posture,
the viewer-facing minimization choices, and the re-review triggers; it does not
try to certify legal compliance or make the stream reusable in every
jurisdiction.

Consequence: receiver coordinates, raw operational state, music files, and
licensing assumptions stay out of the public repository. Description-based NCS
credit remains the canonical music-attribution path, while overlay credit is
only supplemental viewer disclosure.

## Migration Smoke Test

Status: accepted

For v3 changes that affect runtime ownership, encoder behavior, recovery, or
cutover authority, the live smoke-test gate is 24 hours. The basis is that v2
already established the stable long-running behavior model, while v3 still has
to prove that k3s ownership, NVENC, observability wiring, recovery gates, and
same-URL preservation survive one daily cycle.

Consequence: a 24-hour pass is migration confidence, not a long-window SLO
claim. Broader reliability claims still need 14-day or 28-day SLI review.

## Metrics Namespace And Query Isolation

Status: accepted

v3 evidence uses `stream_v3_*` metrics and the v3 observability monitor job. Dashboard
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
the low-bandwidth NVENC CBR shape while letting fps change only through
measurement-backed contract updates.

## Encoder Upload Budget

Status: accepted

The move from v2 `libx264` to v3 `h264_nvenc` CBR increased the measured RTMPS
send envelope at the same nominal 3400k video bitrate. The accepted v3 contract
stays below the 5.0 Mbps warning ceiling in measured windows, but it is closer
to that ceiling than the older v2 CPU-encoded path.

The lower-upload VBR/CQ trial was rejected because YouTube classified the input
as low bitrate / not enough video. Upload efficiency is therefore a guardrail,
not the top-level product outcome.

## Encoder 5fps Current Contract

Status: accepted

On 2026-05-31, v3 compared 4fps, 5fps, and 10fps while holding
`VIDEO_BITRATE=3400k`, `VIDEO_MAXRATE=3400k`, `VIDEO_BUFSIZE=6800k`, and
`AUDIO_BITRATE=192k`. All short trials stayed below the 5.0 Mbps upload ceiling
and YouTube remained healthy in the sampled windows.

The accepted contract is 5fps/3400k/audio192k. It improves cadence over 4fps
with a 20% lower per-frame video budget, while rejecting 10fps as the current
contract because it leaves only 40% of the 4fps per-frame budget and 50% of the
5fps per-frame budget.

## PulseAudio Shared Memory Policy

Status: accepted

Container PulseAudio uses `--disable-shm=yes` and `--enable-memfd=no` to avoid
container memory transport failures.

## SLO Error Budget Policy

Status: accepted

The primary public outcome is availability and same-URL continuity. Visual
quality and upload efficiency are tuned inside that boundary.

## YouTube Lifecycle Mutation Safety

Status: accepted

Broadcast replacement, stream binding, and candidate video promotion require
fresh identity, public/live, API, OAuth, quota, and action-gate evidence.
Delivery recovery can restart local runtime components, but destructive YouTube
lifecycle mutation is intentionally harder because it can break the public
watch URL.

## Visual / Audio / Memory Boundaries

Status: accepted

RTMPS connected is not enough to prove correct output. Visual capture, ADS-B
freshness, now-playing metadata, PulseAudio route, monitor energy, Xvfb shared
memory, and cgroup events remain separate evidence classes.

Consequence: visual, audio, and memory faults can drive scoped subsystem
recovery, but they do not authorize YouTube broadcast replacement by
themselves.

## Host Freeze Recovery

Status: accepted

In-Pod recovery cannot fix a frozen host. Host watchdog configuration is part of
the operational model for single-node deployments.

## Single-Node DR Honesty

Status: accepted

The public DR claim separates measured k3s control-plane recovery from
unmeasured node reboot, disk restore, spare-host rebuild, and viewer-facing
RTMPS reconnect recovery. Single-node k3s is the current deployment shape, not
an HA claim.
