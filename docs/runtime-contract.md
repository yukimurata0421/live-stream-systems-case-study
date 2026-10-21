# Runtime Contract

This page defines what the delivery and monitoring runtimes contain and which
settings they must keep. Hosts and data paths are described once in
[architecture](architecture.md); the media route's health conditions and
thresholds are in the [route record](v3/self-healing-oci-fanout.md).

## Delivery Runtime (Dell)

The k3s Deployment `stream-v3-runtime` contains four containers:

| Container | Responsibility |
| --- | --- |
| `stream-engine` | Xvfb, Chromium, custom MapLibre rendering and overlay capture, PulseAudio sink, FFmpeg NVENC encoding, RTMPS upload to the OCI relay |
| `precipitation-fetcher` | JMA analysis-only precipitation with a bounded last-known-good state |
| `auto-dj` | Playback and now-playing metadata |
| `fast-recovery-loop` | Local observation, connectivity protection, and FFmpeg-child recovery every 10 seconds |

The runtime reads aircraft data from the Dell modified tar1090 endpoint through
a sanitizing proxy. It does not manage the Airspy device.

A separate Deployment, `stream-v3-youtube-backup`, runs only during fallback
and recovery confirmation. It renders its own map and audio and encodes with
`libx264`; it does not reuse the main encoder's output, and GPU sharing is not
implemented.

### Return-Feed Observer

The independent `stream-v3-oci-monitor-probe` Deployment observes the common OCI
relay and, when present, the backup encoder's secondary output. It has one
replica, no GPU or recovery rights, bounded memory-backed images, and 500m CPU /
512 MiB memory limits. Continuous receivers feed ten-second paired JST/UTC OCR.
The [clock contract](v3/relay-clock-monitor.md) defines state, source binding,
missing-data semantics and display-age metrics; the [architecture](architecture.md#clock-observation-path)
defines the paths. Generic manifests require operator configuration and an
immutable image before use.

### Encoder

```text
VIDEO_ENCODER=h264_nvenc
VIDEO_NVENC_RC=cbr
VIDEO_NVENC_PRESET=p4
FRAME_RATE=5
VIDEO_BITRATE=3400k
VIDEO_MAXRATE=3400k
VIDEO_BUFSIZE=6800k
AUDIO_BITRATE=192k
AUDIO_SAMPLE_RATE=48000
```

The profile keeps the low-bandwidth lineage from v2: 5fps/3500k/audio192k,
then 4fps/3400k/audio192k, then 5fps/3400k/audio192k after a 4, 5, and 10 fps
trial showed that 5 fps stayed under the upload ceiling with a smaller quality
trade-off than 10 fps ([tuning record](v3/encoder-fps-tuning-2026-05-31.md)).

### Audio

PulseAudio runs with shared memory disabled in the container to avoid
container-specific `memblock` assertion failures:

```text
--disable-shm=yes
--enable-memfd=no
```

## Monitoring Runtime (arena-server)

The monitor service `stream-v3-arena-monitor` runs
`stream_v3.control_loop --mode monitor` from a fixed release directory. It
contains:

- YouTube video resolver and watchdog, using read-only YouTube Data API,
  OAuth, and public watch-page evidence;
- stream watchdog and subsystem status;
- k3s runtime, state-file, and log evidence collection;
- the notification loop, which is the single periodic notification writer;
- SLI summaries and the Prometheus exporter;
- a 60-second map runtime probe and a 300-second public viewer frame probe.

Prometheus, Loki, Grafana, and Alloy (`ops/monitoring/`) present this evidence.
The `deploy/k3s/v3-control` and `deploy/k3s/v3-observer` manifests package the
same loop for k3s; production runs it as the service above.

The map probe correlates Pod and container state, render heartbeat,
browser/WebGL, weather, GPU, NVENC, and RTMP evidence. The viewer probe captures
a small frame from the public YouTube video and tracks black, freeze, and
consecutive-failure evidence. Probe failures alone do not change the runtime.

Discord receives the broad incident stream. Slack receives critical and
delivery, GPU, and RTMPS incidents immediately, and other incidents after the
configured minimum active period.

## Guarded Restart Gates

The monitor can request a restart of the delivery runtime only through these
gates:

- `src/watchers/youtube_health.py` and `src/watchers/decision/action_gate.py`
  enforce restart budgets, cooldown, budget-release reconfirmation, and
  emergency-override evidence.
- After fast recovery replaces an FFmpeg child, a briefly unavailable
  Deployment is a convergence wait, not a restart trigger. Only 180 seconds of
  continuous unavailability reaches the restart gate.
- GPU preflight, the host boot ID, Pod UID, and current-boot establishment
  marker must agree before a rollout-style restart.
- Replacement-broadcast creation is disabled; a broadcast that really ends is
  recreated by hand.

Inside the Pod, `src/watchers/fast_recovery.py` enforces restart-induced
downtime budgets, block events, and sustained-emergency overrides for the
FFmpeg child.

Mutation requires explicit mode flags. Shadow mode keeps `TEST_MODE=1`,
`STREAM_K8S_DRY_RUN=1`, and `STREAM_V3_CUTOVER_ENABLE=0`; its plans report
`shadow_budget_not_enforced` and `shadow_cooldown_not_enforced`
(`src/stream_v2/recovery_orchestrator/gate.py`) so that a dry run never
consumes budget or extends cooldown.

## CRA Contract (Production-Disabled)

The CRA source defines a narrower recovery path. It is production-disabled and
the current media route does not use it.

| Role | Responsibility in the CRA source | Limit |
| --- | --- | --- |
| `arena-server` Monitoring v4 | Observation, current state, incident and parity evidence, notification intents, signed facts-only projection | No policy, command, final verdict, or process signaling |
| `cra-01` CRA | Policy, budget, cooldown, authorization, command lifecycle, reconciliation, final verification | No raw-source ownership or direct process signaling |
| Dell delivery | Signed local facts and one exact, target-wide fenced FFmpeg-child effect | No action selection or escalation to container, Pod, Deployment, or host restart |
| Raspberry Pi | Allowlisted static publication | No database, incident judgment, credential, or control feedback |

Host, boot, Pod, container, FFmpeg generation, and PID identity must agree
before an effect is admitted. An ambiguous response becomes `OUTCOME_UNKNOWN`
and is reconciled instead of retried under a new request ID. Source and Harness
tests do not establish a live deployment or effect.

## State

Runtime state lives under `.state/` or `/state` and is not part of the public
repository. It includes now-playing metadata, heartbeats, watchdog summaries,
SLI outputs, and recovery action plans. The
[state and evidence contract](v3/runtime-state-and-evidence.md) defines the files.
